/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/shard_table.h"

#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "storage/shmem.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "metadb/foreign_server.h"
#include "utils/error_log.h"
#include "utils/shmem_control.h"
#include "utils/utils.h"

static ShmemControlData *ShardTableShmemControl = NULL;
static FormData_falcon_shard_table *ShardTableShmemCache = NULL;
static int32_t *ShardTableShmemCacheCount = NULL;
static pg_atomic_uint32 *ShardTableShmemCacheInvalid = NULL;

PG_FUNCTION_INFO_V1(falcon_build_shard_table);
PG_FUNCTION_INFO_V1(falcon_update_shard_table);
PG_FUNCTION_INFO_V1(falcon_reload_shard_table_cache);
PG_FUNCTION_INFO_V1(falcon_renew_shard_table);

Datum falcon_build_shard_table(PG_FUNCTION_ARGS)
{
    int32_t shardCount = PG_GETARG_INT32(0);

    if (shardCount <= 0 || shardCount > SHARD_COUNT_MAX)
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "shard count must be in range (%d, %d]", 0, SHARD_COUNT_MAX);

    List *serverIdList = GetAllForeignServerId_Group(false, true);
    if (list_length(serverIdList) == 0)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "no worker.");

    int serverRound = 0;
    Relation rel = table_open(ShardRelationId(), RowExclusiveLock);
    CatalogIndexState indstate = CatalogOpenIndexes(rel);
    TupleDesc tupleDesc = RelationGetDescr(rel);
    for (int i = 0; i < shardCount; ++i) {
        int32_t rangePoint;
        if (i == shardCount - 1)
            rangePoint = SHARD_TABLE_RANGE_MAX;
        else
            rangePoint =
                ((int64_t)SHARD_TABLE_RANGE_MAX - SHARD_TABLE_RANGE_MIN) * (i + 1) / shardCount + SHARD_TABLE_RANGE_MIN;

        Datum datumArray[Natts_falcon_shard_table];
        bool isNullArray[Natts_falcon_shard_table];
        memset(isNullArray, 0, sizeof(isNullArray));
        datumArray[Anum_falcon_shard_table_range_point - 1] = Int32GetDatum(rangePoint);
        // ArrayType for server_ids
        datumArray[Anum_falcon_shard_table_server_ids - 1] = PointerGetDatum(list_nth(serverIdList, serverRound));
        HeapTuple heapTuple = heap_form_tuple(tupleDesc, datumArray, isNullArray);
        CatalogTupleInsertWithInfo(rel, heapTuple, indstate);

        serverRound = (serverRound + 1) % list_length(serverIdList);
    }
    CommandCounterIncrement();
    CatalogCloseIndexes(indstate);
    table_close(rel, RowExclusiveLock);
    InvalidateShardTableShmemCache();

    PG_RETURN_INT16(0);
}

/* alter this function to update one shard at a time */
Datum falcon_update_shard_table(PG_FUNCTION_ARGS)
{
    int32_t rangePoint = PG_GETARG_INT32(0);
    ArrayType *serverIdArrayType = PG_GETARG_ARRAYTYPE_P(1);

    Relation rel = table_open(ShardRelationId(), RowExclusiveLock);
    CatalogIndexState indstate = CatalogOpenIndexes(rel);
    TupleDesc tupleDesc = RelationGetDescr(rel);
    Datum datumArray[Natts_falcon_shard_table];
    bool isNullArray[Natts_falcon_shard_table];
    memset(isNullArray, 0, sizeof(isNullArray));
    bool doUpdateArray[Natts_falcon_shard_table];
    memset(doUpdateArray, 0, sizeof(doUpdateArray));
    doUpdateArray[Anum_falcon_shard_table_server_ids - 1] = true;
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0],
                Anum_falcon_shard_table_range_point,
                BTEqualStrategyNumber,
                F_INT4EQ,
                rangePoint);
    SysScanDesc scanDesc =
        systable_beginscan(rel, ShardRelationIndexId(), true, GetTransactionSnapshot(), 1, scanKey);
    HeapTuple heapTuple = systable_getnext(scanDesc);
    if (!HeapTupleIsValid(heapTuple)) {
        // insert
        datumArray[Anum_falcon_shard_table_range_point - 1] = rangePoint;
        datumArray[Anum_falcon_shard_table_server_ids - 1] = PointerGetDatum(serverIdArrayType);
        CatalogTupleInsertWithInfo(rel, heap_form_tuple(tupleDesc, datumArray, isNullArray), indstate);
    } else {
        // update
        datumArray[Anum_falcon_shard_table_server_ids - 1] = PointerGetDatum(serverIdArrayType);
        HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc, datumArray, isNullArray, doUpdateArray);
        CatalogTupleUpdateWithInfo(rel, &updatedTuple->t_self, updatedTuple, indstate);
    }
    systable_endscan(scanDesc);

    CommandCounterIncrement();

    CatalogCloseIndexes(indstate);
    table_close(rel, RowExclusiveLock);
    InvalidateShardTableShmemCache();
    ReloadShardTableShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_reload_shard_table_cache(PG_FUNCTION_ARGS)
{
    InvalidateShardTableShmemCache();
    ReloadShardTableShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_renew_shard_table(PG_FUNCTION_ARGS)
{
    FuncCallContext *functionContext = NULL;
    List *returnInfoList = NIL;
    uint32_t d_off;
    ShardInfo *shardInfo;
    Datum values[5];
    bool resNulls[5];
    HeapTuple heapTupleRes;
    TupleDesc tupleDescriptor;

    if (SRF_IS_FIRSTCALL()) {
        functionContext = SRF_FIRSTCALL_INIT();

        MemoryContext oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);
        if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE) {
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "return type must be a row type");
        }
        functionContext->tuple_desc = BlessTupleDesc(tupleDescriptor);

        while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
            ReloadShardTableShmemCache();
        }
        LWLockAcquire(&ShardTableShmemControl->lock, AccessShareLock);

        List *shardIdList = NIL;
        int32_t rangeLow = SHARD_TABLE_RANGE_MIN;
        for (int i = 0; i < *ShardTableShmemCacheCount; i++) {
            FormData_falcon_shard_table *shardTableRow = ShardTableShmemCache + i;

            shardInfo = (ShardInfo *)palloc(sizeof(ShardInfo));

            shardInfo->rangeMin = rangeLow;
            shardInfo->rangeMax = shardTableRow->range_point;
            shardInfo->count = shardTableRow->server_ids.count;

            List *serverListPerShard = NIL;
            for (int j = 0; j < shardTableRow->server_ids.count; j++) {
                shardInfo->serverIds[j] = shardTableRow->server_ids.servers[j];
                serverListPerShard = lappend_int(serverListPerShard, shardTableRow->server_ids.servers[j]);
            }
            rangeLow = shardTableRow->range_point + 1;

            returnInfoList = lappend(returnInfoList, shardInfo);
            shardIdList = lappend(shardIdList, serverListPerShard);
        }

        LWLockRelease(&ShardTableShmemControl->lock);

        List *connectionInfoList = GetForeignServerConnectionInfo(shardIdList);
        for (int i = 0; i < list_length(returnInfoList); ++i) {
            ShardInfo *shardInfo = list_nth(returnInfoList, i);
            List *groupConnectionInfo = list_nth(connectionInfoList, i);

            ListCell *cell;
            int j = 0;
            foreach(cell, groupConnectionInfo) {
                ForeignServerConnectionInfo *connectionInfo = lfirst(cell);
                shardInfo->ports[j] = connectionInfo->port;
                strcpy(shardInfo->hosts[j], connectionInfo->host);
                j++;
            }
        }

        functionContext->user_fctx = returnInfoList;
        functionContext->max_calls = list_length(returnInfoList);
        MemoryContextSwitchTo(oldContext);
    }

    functionContext = SRF_PERCALL_SETUP();
    returnInfoList = functionContext->user_fctx;
    d_off = functionContext->call_cntr;

    if (d_off < functionContext->max_calls) {
        shardInfo = (ShardInfo *)list_nth(returnInfoList, d_off);
        memset(resNulls, false, sizeof(resNulls));
        values[0] = Int32GetDatum(shardInfo->rangeMin);
        values[1] = Int32GetDatum(shardInfo->rangeMax);

        // return all servers in a group
        values[2] = PointerGetDatum(build_text_array(shardInfo->hosts, shardInfo->count));
        values[3] = PointerGetDatum(build_int_array(shardInfo->ports, shardInfo->count));
        values[4] = PointerGetDatum(build_int_array(shardInfo->serverIds, shardInfo->count));

        heapTupleRes = heap_form_tuple(functionContext->tuple_desc, values, resNulls);
        SRF_RETURN_NEXT(functionContext, HeapTupleGetDatum(heapTupleRes));
    }

    list_free_deep(returnInfoList);
    functionContext->user_fctx = NULL;
    SRF_RETURN_DONE(functionContext);
}

Oid ShardRelationId(void)
{
    GetRelationOid("falcon_shard_table", &CachedRelationOid[CACHED_RELATION_SHARD_TABLE]);
    return CachedRelationOid[CACHED_RELATION_SHARD_TABLE];
}

Oid ShardRelationIndexId(void)
{
    GetRelationOid("falcon_shard_table_index", &CachedRelationOid[CACHED_RELATION_SHARD_TABLE_INDEX]);
    return CachedRelationOid[CACHED_RELATION_SHARD_TABLE_INDEX];
}

void SearchShardInfoByShardValue(uint64_t shardColValue, int32_t *rangePoint, int32_t *serverId)
{
    // Block shard map read only when transfer operations acquire AccessExclusiveLock
    int32 hashvalue = HashShard(shardColValue);
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, LW_SHARED);
    int l = 0;
    int r = *ShardTableShmemCacheCount;
    while (l < r) {
        int mid = (l + r) / 2;
        if (ShardTableShmemCache[mid].range_point < hashvalue)
            l = mid + 1;
        else
            r = mid;
    }
    if (l == *ShardTableShmemCacheCount) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                   "shard value %d out of range, max support %d",
                                   hashvalue,
                                   ShardTableShmemCache[*ShardTableShmemCacheCount - 1].range_point);
    }
    *rangePoint = ShardTableShmemCache[l].range_point;
    *serverId = ShardTableShmemCache[l].server_ids.servers[0];
    LWLockRelease(&ShardTableShmemControl->lock);
}

void SearchShardInfoByShardValue_Group(uint64_t shardColValue, int32_t *rangePoint, int32_t *serverId, int32_t *count)
{
    // Block shard map read only when transfer operations acquire AccessExclusiveLock
    int32 hashvalue = HashShard(shardColValue);
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, LW_SHARED);
    int l = 0;
    int r = *ShardTableShmemCacheCount;
    while (l < r) {
        int mid = (l + r) / 2;
        if (ShardTableShmemCache[mid].range_point < hashvalue)
            l = mid + 1;
        else
            r = mid;
    }
    if (l == *ShardTableShmemCacheCount) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                   "shard value %d out of range, max support %d",
                                   hashvalue,
                                   ShardTableShmemCache[*ShardTableShmemCacheCount - 1].range_point);
    }
    *rangePoint = ShardTableShmemCache[l].range_point;
    *count = ShardTableShmemCache[l].server_ids.count;
    for (int i = 0; i < *count; i++) {
        serverId[i] = ShardTableShmemCache[l].server_ids.servers[i];
    }
    LWLockRelease(&ShardTableShmemControl->lock);
}

List *GetShardTableData()
{
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, AccessShareLock);

    List *result = NIL;
    for (int i = 0; i < *ShardTableShmemCacheCount; i++) {
        FormData_falcon_shard_table *shardTableRow = ShardTableShmemCache + i;

        FormData_falcon_shard_table *data = (FormData_falcon_shard_table *)palloc(sizeof(FormData_falcon_shard_table));
        data->range_point = shardTableRow->range_point;
        data->server_ids = shardTableRow->server_ids;
        result = lappend(result, data);
    }
    LWLockRelease(&ShardTableShmemControl->lock);
    return result;
}

int32_t GetShardTableSize()
{
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, AccessShareLock);

    int32_t result = *ShardTableShmemCacheCount;

    LWLockRelease(&ShardTableShmemControl->lock);
    return result;
}

void InvalidateShardTableShmemCacheCallback(Datum argument, Oid relationId)
{
    if (relationId == InvalidOid || relationId == ShardRelationId()) {
        InvalidateShardTableShmemCache();
    }
}

size_t ShardTableShmemsize()
{
    return sizeof(ShmemControlData) + sizeof(int32_t) + sizeof(pg_atomic_uint32) +
           sizeof(FormData_falcon_shard_table) * SHARD_COUNT_MAX;
}
void ShardTableShmemInit()
{
    bool initialized;
    ShardTableShmemControl = ShmemInitStruct("Shard Table Control", ShardTableShmemsize(), &initialized);
    ShardTableShmemCacheCount = (int32_t *)(ShardTableShmemControl + 1);
    ShardTableShmemCacheInvalid = (pg_atomic_uint32 *)(ShardTableShmemCacheCount + 1);
    ShardTableShmemCache = (FormData_falcon_shard_table *)(ShardTableShmemCacheInvalid + 1);
    if (!initialized) {
        ShardTableShmemControl->trancheId = LWLockNewTrancheId();
        ShardTableShmemControl->lockTrancheName = "Falcon Shard Table Shmem Control";
        LWLockRegisterTranche(ShardTableShmemControl->trancheId, ShardTableShmemControl->lockTrancheName);
        LWLockInitialize(&ShardTableShmemControl->lock, ShardTableShmemControl->trancheId);

        *ShardTableShmemCacheCount = 0;
        pg_atomic_init_u32(ShardTableShmemCacheInvalid, 1);
    }
}

void InvalidateShardTableShmemCache() { pg_atomic_exchange_u32(ShardTableShmemCacheInvalid, 1); }

ServerList ExtractServersFromArray(Datum serverIdArrayType) {
    ServerList result = {0};

    Datum *serverIdArray;
    ArrayTypeArrayToDatumArrayAndSize(DatumGetArrayTypeP(serverIdArrayType), &serverIdArray, &(result.count));

    for (int i = 0; i < result.count; i++) {
        result.servers[i] = DatumGetInt32(serverIdArray[i]);
    }

    return result;
}

void ReloadShardTableShmemCache()
{
    LWLockAcquire(&ShardTableShmemControl->lock, LW_EXCLUSIVE);
    if (!pg_atomic_exchange_u32(ShardTableShmemCacheInvalid, 0)) {
        LWLockRelease(&ShardTableShmemControl->lock);
        return;
    }

    *ShardTableShmemCacheCount = 0;
    bool exceedMaxNumOfShardTable = false;
    Relation rel = table_open(ShardRelationId(), AccessShareLock);
    Relation relIndex = index_open(ShardRelationIndexId(), AccessShareLock);
    SysScanDesc scanDesc = systable_beginscan_ordered(rel, relIndex, NULL, 0, NULL);
    TupleDesc tupleDesc = RelationGetDescr(rel);

    Datum datumArray[Natts_falcon_shard_table];
    bool isNullArray[Natts_falcon_shard_table];
    HeapTuple heapTuple;
    while (HeapTupleIsValid(heapTuple = systable_getnext(scanDesc))) {
        if (*ShardTableShmemCacheCount >= SHARD_COUNT_MAX) {
            exceedMaxNumOfShardTable = true;
            break;
        }

        heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);

        ShardTableShmemCache[*ShardTableShmemCacheCount].range_point =
            DatumGetInt32(datumArray[Anum_falcon_shard_table_range_point - 1]);
        // first server is the leader
        ShardTableShmemCache[*ShardTableShmemCacheCount].server_ids =
            ExtractServersFromArray(datumArray[Anum_falcon_shard_table_server_ids - 1]);

        ++*ShardTableShmemCacheCount;
    }
    systable_endscan_ordered(scanDesc);
    index_close(relIndex, AccessShareLock);
    table_close(rel, AccessShareLock);

    if (exceedMaxNumOfShardTable) {
        InvalidateShardTableShmemCache();
        FALCON_ELOG_ERROR_EXTENDED(
            PROGRAM_ERROR,
            "shard table exceed max size %d, tables whose range_point are larger than %d are ignored",
            SHARD_COUNT_MAX,
            ShardTableShmemCache[*ShardTableShmemCacheCount - 1].range_point);
    }

    LWLockRelease(&ShardTableShmemControl->lock);
}
