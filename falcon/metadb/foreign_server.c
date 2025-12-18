/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/foreign_server.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "catalog/indexing.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "replication/walreceiver.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"
#include "utils/lsyscache.h"
#include "postgres.h"
#include "miscadmin.h"

#include "control/control_flag.h"
#include "utils/error_log.h"
#include "utils/shmem_control.h"
#include "utils/utils.h"

static ShmemControlData *ForeignServerShmemControl = NULL;
static HTAB *ForeignServerShmemCache = NULL;
static int32_t *ForeignServerShmemCacheVersion = NULL;
static pg_atomic_uint32 *ForeignServerShmemCacheInvalid = NULL;

static MemoryContext ForeignServerContext = NULL;
static HTAB *ForeignServerConnectionManager = NULL;
static int32_t ForeignServerLocalConnectionManagerVersion = 0;
static int32_t LocalServerId = -1;                                //
static char LocalServerName[FOREIGN_SERVER_NAME_MAX_LENGTH] = ""; //

static void RenewForeignServerLocalCache(const bool needLock);
static inline void InsertForeignServerByRel(Relation rel,
                                            const int32_t serverId,
                                            const char *serverName,
                                            const char *host,
                                            const int32_t port,
                                            const bool isLocal,
                                            const char *userName,
                                            const int32_t group_id,
                                            const bool is_leader,
                                            const bool is_valid);
static inline void DeleteForeignServerByRel(Relation rel, const int32_t serverId);
static inline void UpdateForeignServerByRel(Relation rel, const int32_t serverId, const char *host, const int32_t port);
static inline void UpdateForeignServerMultipleByRel(Relation rel, const char **added, int added_count,
                                                    const char **removed, int removed_count, const int32_t group_id);

PG_FUNCTION_INFO_V1(falcon_foreign_server_test);
PG_FUNCTION_INFO_V1(falcon_insert_foreign_server);
PG_FUNCTION_INFO_V1(falcon_delete_foreign_server);
PG_FUNCTION_INFO_V1(falcon_update_foreign_server);
PG_FUNCTION_INFO_V1(falcon_update_foreign_server_multiple);
PG_FUNCTION_INFO_V1(falcon_reload_foreign_server_cache);

Datum falcon_foreign_server_test(PG_FUNCTION_ARGS)
{
    char *mode = PG_GETARG_CSTRING(0);

    if (strcmp(mode, "EXCESS_MAX_FOREIGN_SERVER_NUM") == 0) {
        StringInfo serverName = makeStringInfo();
        for (int i = 0; i < FOREIGN_SERVER_NUM_MAX; ++i) {
            int32_t serverId = i + 12345;
            resetStringInfo(serverName);
            appendStringInfo(serverName, "worker_%d", serverId);

            // fake foreign server
            InsertForeignServer(serverId, serverName->data, "192.168.0.1", serverId, false, "liumingyu", 0, true, true);
        }
        InvalidateForeignServerShmemCache();
        ReloadForeignServerShmemCache();
    } else if (strcmp(mode, "GET_ALL_CONN") == 0) {
        List *workerIdList = GetAllForeignServerId(false, false);
        (void)GetForeignServerConnection(workerIdList);
    } else if (strcmp(mode, "GET_INFO_CONN_AND_CLEANUP") == 0) {
        List *workerIdList = GetAllForeignServerId_Group(false, false);
        List *connInfoList = GetForeignServerConnectionInfo(workerIdList);
        List *foreignServerInfoList = GetForeignServerInfo(workerIdList);
        for (int i = 0; i < list_length(workerIdList); ++i) {
            // int32_t workerId = list_nth_int(workerIdList, i);
            ArrayType *serverId_list = list_nth(workerIdList, i);
            Oid element_type = INT4OID;
            int16 elmlen;
            bool elmbyval;
            char elmalign;
            get_typlenbyvalalign(INT4OID, &elmlen, &elmbyval, &elmalign);
            Datum *elements;
            bool *nulls;
            int nelems;
            deconstruct_array(serverId_list, element_type, elmlen, elmbyval, elmalign, &elements, &nulls, &nelems);
            for (int j = 0; j < nelems; j++) {
                if (!nulls[j]) continue;
                int32_t workerId = DatumGetInt32(elements[j]);

                ForeignServerConnectionInfo *connInfo = list_nth(list_nth(connInfoList, i), j);
                FormData_falcon_foreign_server *foreignServerInfo = list_nth(foreignServerInfoList, i * nelems + j);
                elog(WARNING, "workerId: %d, connInfo: %s, %d", workerId, connInfo->host, connInfo->port);
                elog(WARNING,
                    "workerId: %d, foreignServerInfo: %s, %d, %d, %s, %d, %s",
                    workerId,
                    foreignServerInfo->host,
                    foreignServerInfo->port,
                    foreignServerInfo->server_id,
                    foreignServerInfo->server_name,
                    foreignServerInfo->is_local,
                    foreignServerInfo->user_name);
            }
            pfree(elements);
            pfree(nulls);
        }
        (void)GetForeignServerConnection(list_make1_int(1));
        CleanupForeignServerConnections();
    }
    PG_RETURN_INT16(0);
}

Datum falcon_insert_foreign_server(PG_FUNCTION_ARGS)
{
    int32_t serverId = PG_GETARG_INT32(0);
    char *serverName = PG_GETARG_CSTRING(1);
    char *host = PG_GETARG_CSTRING(2);
    int32_t port = PG_GETARG_INT32(3);
    bool isLocal = PG_GETARG_BOOL(4);
    char *userName = PG_GETARG_CSTRING(5);
    int32_t group_id = PG_GETARG_INT32(6);
    bool is_leader = PG_GETARG_BOOL(7);
    bool is_valid = true;

    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    InsertForeignServerByRel(rel, serverId, serverName, host, port, isLocal, userName, group_id, is_leader, is_valid);

    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_delete_foreign_server(PG_FUNCTION_ARGS)
{
    int32_t serverId = PG_GETARG_INT32(0);

    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    DeleteForeignServerByRel(rel, serverId);
    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_update_foreign_server(PG_FUNCTION_ARGS)
{
    int32_t serverId = PG_GETARG_INT32(0);
    char *host = PG_GETARG_CSTRING(1);
    int32_t port = PG_GETARG_INT32(2);

    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    UpdateForeignServerByRel(rel, serverId, host, port);
    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();
    PG_RETURN_INT16(0);
}

Datum falcon_update_foreign_server_multiple(PG_FUNCTION_ARGS)
{
    ArrayType *array;
    Datum *elements0, *elements1;
    bool *nulls0, *nulls1;
    int nelems;
    array = PG_GETARG_ARRAYTYPE_P(0);
    deconstruct_array(array, TEXTOID, -1, false, TYPALIGN_INT, &elements0, &nulls0, &nelems);
    const char *added[FOREIGN_SERVER_GROUP_NUM_MAX];
    int notNullNum_added = 0;
    for (int i = 0; i < nelems; i++)
    {
        if (!nulls0[i])
        {
            text *txt = DatumGetTextPP(elements0[i]);
            added[notNullNum_added++] = text_to_cstring(txt);
        }
    }
    array = PG_GETARG_ARRAYTYPE_P(1);
    deconstruct_array(array, TEXTOID, -1, false, TYPALIGN_INT, &elements1, &nulls1, &nelems);
    const char *removed[FOREIGN_SERVER_GROUP_NUM_MAX];
    int notNullNum_removed = 0;
    for (int i = 0; i < nelems; i++)
    {
        if (!nulls1[i])
        {
            text *txt = DatumGetTextPP(elements1[i]);
            removed[notNullNum_removed++] = text_to_cstring(txt);
        }
    }

    int32_t group_id = PG_GETARG_INT32(2);
    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    // first remove then add
    UpdateForeignServerMultipleByRel(rel, added, notNullNum_added, removed, notNullNum_removed, group_id);
    pfree(elements0);
    pfree(nulls0);
    pfree(elements1);
    pfree(nulls1);
    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();
    PG_RETURN_INT16(0);
}

Datum falcon_reload_foreign_server_cache(PG_FUNCTION_ARGS)
{
    InvalidateForeignServerShmemCache();
    ReloadForeignServerShmemCache();

    PG_RETURN_INT16(0);
}

Oid ForeignServerRelationId(void)
{
    GetRelationOid("falcon_foreign_server", &CachedRelationOid[CACHED_RELATION_FOREIGN_SERVER]);
    return CachedRelationOid[CACHED_RELATION_FOREIGN_SERVER];
}

Oid ForeignServerRelationIndexId(void)
{
    GetRelationOid("falcon_foreign_server_index", &CachedRelationOid[CACHED_RELATION_FOREIGN_SERVER_INDEX]);
    return CachedRelationOid[CACHED_RELATION_FOREIGN_SERVER_INDEX];
}

Oid ForeignServerRelationGroupIdIndexId(void)
{
    GetRelationOid("falcon_foreign_server_group_id_index", &CachedRelationOid[CACHED_RELATION_FOREIGN_SERVER_GROUP_ID_INDEX]);
    return CachedRelationOid[CACHED_RELATION_FOREIGN_SERVER_GROUP_ID_INDEX];
}

void ForeignServerCacheInit()
{
    ForeignServerContext = AllocSetContextCreateInternal(TopMemoryContext,
                                                         "Falcon Foreign Server Hash Table Context",
                                                         ALLOCSET_DEFAULT_MINSIZE,
                                                         ALLOCSET_DEFAULT_INITSIZE,
                                                         ALLOCSET_DEFAULT_MAXSIZE);
    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ForeignServerConnection);
    info.hcxt = ForeignServerContext;
    int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    ForeignServerConnectionManager =
        hash_create("Foreign Server Connection Hash Table", FOREIGN_SERVER_NUM_EXPECT, &info, hashFlags);
    ForeignServerLocalConnectionManagerVersion = 0;
}

static inline void InsertForeignServerByRel(Relation rel,
                                            const int32_t serverId,
                                            const char *serverName,
                                            const char *host,
                                            const int32_t port,
                                            const bool isLocal,
                                            const char *userName,
                                            const int32_t group_id,
                                            const bool is_leader,
                                            const bool is_valid)
{
    Datum values[Natts_falcon_foreign_server];
    bool isNulls[Natts_falcon_foreign_server];
    HeapTuple heapTuple;
    TupleDesc tupleDescriptor = RelationGetDescr(rel);

    memset(isNulls, false, sizeof(isNulls));
    values[Anum_falcon_foreign_server_server_id - 1] = Int32GetDatum(serverId);
    values[Anum_falcon_foreign_server_server_name - 1] = CStringGetTextDatum(serverName);
    values[Anum_falcon_foreign_server_host - 1] = CStringGetTextDatum(host);
    values[Anum_falcon_foreign_server_port - 1] = Int32GetDatum(port);
    values[Anum_falcon_foreign_server_is_local - 1] = BoolGetDatum(isLocal);
    values[Anum_falcon_foreign_server_user_name - 1] = CStringGetTextDatum(userName);
    values[Anum_falcon_foreign_server_group_id - 1] = Int32GetDatum(group_id);
    values[Anum_falcon_foreign_server_is_leader - 1] = BoolGetDatum(is_leader);
    values[Anum_falcon_foreign_server_is_valid - 1] = BoolGetDatum(is_valid);

    heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
    CatalogTupleInsert(rel, heapTuple);

    CommandCounterIncrement();
}

static inline void DeleteForeignServerByRel(Relation rel, const int32_t serverId)
{
    SetUpScanCaches();
    ScanKeyData scanKey[1];
    scanKey[0] = ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ];
    scanKey[0].sk_argument = Int32GetDatum(serverId);

    SysScanDesc scanDescriptor = systable_beginscan(rel, ForeignServerRelationIndexId(), true, NULL, 1, scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    if (!HeapTupleIsValid(heapTuple)) {
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "serverId " INT32_PRINT_SYMBOL " is not existed.", serverId);
    } else {
        CatalogTupleDelete(rel, &heapTuple->t_self);
        CommandCounterIncrement();
    }
    systable_endscan(scanDescriptor);
}

static inline void UpdateForeignServerByRel(Relation rel, const int32_t serverId, const char *host, const int32_t port)
{
    SetUpScanCaches();
    ScanKeyData scanKey[1];
    scanKey[0] = ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ];
    scanKey[0].sk_argument = Int32GetDatum(serverId);

    SysScanDesc scanDescriptor = systable_beginscan(rel, ForeignServerRelationIndexId(), true, NULL, 1, scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    if (!HeapTupleIsValid(heapTuple)) {
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "serverId " INT32_PRINT_SYMBOL " is not existed.", serverId);
    } else {
        Datum datumArray[Natts_falcon_foreign_server];
        bool isNullArray[Natts_falcon_foreign_server];
        memset(isNullArray, 0, sizeof(isNullArray));
        bool doUpdateArray[Natts_falcon_foreign_server];
        memset(doUpdateArray, 0, sizeof(doUpdateArray));
        datumArray[Anum_falcon_foreign_server_host - 1] = CStringGetTextDatum(host);
        doUpdateArray[Anum_falcon_foreign_server_host - 1] = true;
        datumArray[Anum_falcon_foreign_server_port - 1] = Int32GetDatum(port);
        doUpdateArray[Anum_falcon_foreign_server_port - 1] = true;
        datumArray[Anum_falcon_foreign_server_is_valid - 1] = BoolGetDatum(true);
        doUpdateArray[Anum_falcon_foreign_server_is_valid - 1] = true;
        HeapTuple updatedTuple =
            heap_modify_tuple(heapTuple, RelationGetDescr(rel), datumArray, isNullArray, doUpdateArray);
        CatalogTupleUpdate(rel, &updatedTuple->t_self, updatedTuple);
        heap_freetuple(updatedTuple);

        CommandCounterIncrement();
    }
    systable_endscan(scanDescriptor);
}

// first remove then add
static inline void UpdateForeignServerMultipleByRel(Relation rel, const char **added, int added_count,
                                                    const char **removed, int removed_count, const int32_t group_id)
{
    SetUpScanCaches();
    ScanKeyData scanKey[1];
    scanKey[0] = ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_GROUP_ID_EQ];
    scanKey[0].sk_argument = Int32GetDatum(group_id);

    // scan on group_id index
    SysScanDesc scanDescriptor = systable_beginscan(rel, ForeignServerRelationGroupIdIndexId(), true, NULL, 1, scanKey);

    // if ["*"], remove all
    bool removeAll = (removed_count == 1 && strcmp(removed[0], "*") == 0);
    // index of new server to add in added
    int added_index = 0;
    HeapTuple heapTuple;
    
    // scan all row, match removed and !is_valid
    while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor))) {
        bool isNull;
        Datum hostDatum = heap_getattr(heapTuple, Anum_falcon_foreign_server_host, RelationGetDescr(rel), &isNull);
        char *currentHost = isNull ? "" : TextDatumGetCString(hostDatum);
        Datum portDatum = heap_getattr(heapTuple, Anum_falcon_foreign_server_port, RelationGetDescr(rel), &isNull);
        int32_t currentPort = isNull ? 0 : DatumGetInt32(portDatum);
        Datum isValidDatum = heap_getattr(heapTuple, Anum_falcon_foreign_server_is_valid, RelationGetDescr(rel), &isNull);
        bool currentIsValid = !isNull && DatumGetBool(isValidDatum);

        // check need to remove this row or not
        bool needRemove = false;
        if (removeAll) {
            needRemove = true;
        } else {
            // check host:port in removed
            char currentPair[256];
            snprintf(currentPair, sizeof(currentPair), "%s:%d", currentHost, currentPort);
            for (int i = 0; i < removed_count; i++) {
                if (strcmp(currentPair, removed[i]) == 0) {
                    needRemove = true;
                    if (!currentIsValid) {
                        elog(WARNING, "UpdateForeignServerMultipleByRel: removing already invalid replica server %s:%d", currentHost, currentPort);
                    }
                    break;
                }
            }
        }

        // how to update this row: remove + add, remove only, add only, do nothing
        bool removeThenAdd = false, removeOnly = false, addOnly = false;
        if (needRemove) {
            if (added_index < added_count) {
                // server left to add
                // valid row, remove then add
                removeThenAdd = currentIsValid;
                // invalid row, add only
                addOnly = !currentIsValid;
            } else {
                // no server to add, remove if valid
                removeOnly = currentIsValid;
            }
        } else if (added_index < added_count) {
            // no need to remove, server left to add, add on invalid row
            addOnly = !currentIsValid;
        } else {
            // no need to remove, no server to add, do nothing 
        }

        // update info
        Datum datumArray[Natts_falcon_foreign_server];
        bool isNullArray[Natts_falcon_foreign_server];
        memset(isNullArray, 0, sizeof(isNullArray));
        bool doUpdateArray[Natts_falcon_foreign_server];
        memset(doUpdateArray, 0, sizeof(doUpdateArray));

        // any add, update row directly
        if (removeThenAdd || addOnly) {
            // update this row to first elemet left in added
            const char *added_value = added[added_index];
            char *colon = strchr(added_value, ':');
            
            if (colon) {
                *colon = '\0'; // set ':' to be end of string
                const char *host = added_value;
                int32_t port = atoi(colon + 1);
                *colon = ':';
                // update: host, port, is_valid
                datumArray[Anum_falcon_foreign_server_host - 1] = CStringGetTextDatum(host);
                doUpdateArray[Anum_falcon_foreign_server_host - 1] = true;
                datumArray[Anum_falcon_foreign_server_port - 1] = Int32GetDatum(port);
                doUpdateArray[Anum_falcon_foreign_server_port - 1] = true;
                datumArray[Anum_falcon_foreign_server_is_valid - 1] = BoolGetDatum(true);
                doUpdateArray[Anum_falcon_foreign_server_is_valid - 1] = true;
                if (removeThenAdd) {
                    elog(INFO, "Reused removed row for group_id %d with host: %s", group_id, added_value);
                } else {
                    elog(INFO, "Added row for group_id %d with host: %s", group_id, added_value);
                }
            } else if (removeThenAdd) {
                // element in added doesn't contain ':', add failed
                // remove only
                removeOnly = true;
                elog(WARNING, "Invalid added host format: %s, will remove later", added_value);
            }
            // pass one element in added
            added_index++;
        }
        // remove only, set is_valid to false
        if (removeOnly) {
            datumArray[Anum_falcon_foreign_server_is_valid - 1] = BoolGetDatum(false);
            doUpdateArray[Anum_falcon_foreign_server_is_valid - 1] = true;
            elog(INFO, "Removed row for group_id %d", group_id);
        }
        
        // any update
        if (removeThenAdd || removeOnly || addOnly) {
            HeapTuple updatedTuple =
                heap_modify_tuple(heapTuple, RelationGetDescr(rel), datumArray, isNullArray, doUpdateArray);
            
            CatalogTupleUpdate(rel, &updatedTuple->t_self, updatedTuple);
            heap_freetuple(updatedTuple);
            
            CommandCounterIncrement();
        }
    }
    systable_endscan(scanDescriptor);

    if (added_index < added_count) {
        elog(WARNING, "Not all added elements used for group_id %d: %d remaining", group_id, added_count - added_index);
    }
}

void InsertForeignServer(const int32_t serverId,
                         const char *serverName,
                         const char *host,
                         const int32_t port,
                         const bool isLocal,
                         const char *userName,
                         const int32_t group_id,
                         const bool is_leader,
                         const bool is_valid)
{
    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    InsertForeignServerByRel(rel, serverId, serverName, host, port, isLocal, userName, group_id, is_leader, is_valid);
    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();
}

void DeleteForeignServer(const int32_t serverId)
{
    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    DeleteForeignServerByRel(rel, serverId);
    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();
}

void UpdateForeignServer(const int32_t serverId, const char *host, const int32_t port)
{
    Relation rel = table_open(ForeignServerRelationId(), RowExclusiveLock);
    UpdateForeignServerByRel(rel, serverId, host, port);
    table_close(rel, RowExclusiveLock);
    InvalidateForeignServerShmemCache();
}

void CleanupForeignServerConnections(void)
{
    HASH_SEQ_STATUS status;
    ForeignServerConnection *foreignServerConnection;
    hash_seq_init(&status, ForeignServerConnectionManager);
    while ((foreignServerConnection = hash_seq_search(&status)) != NULL) {
        int serverId = foreignServerConnection->serverId;
        bool found;

        PQfinish(foreignServerConnection->conn);
        foreignServerConnection->conn = NULL;
        hash_search(ForeignServerConnectionManager, &serverId, HASH_REMOVE, &found);
    }
}

static void RenewForeignServerLocalCache(const bool needLock)
{
    if (needLock)
        LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);

    // 1. remove foreign server connection through ForeignServerShmemCache info which is updated by cm
    HASH_SEQ_STATUS status;
    ForeignServerConnection *foreignServerConnection;
    FormData_falcon_foreign_server *foreignServerInfo;
    hash_seq_init(&status, ForeignServerConnectionManager);
    while ((foreignServerConnection = hash_seq_search(&status)) != NULL) {
        int serverId = foreignServerConnection->serverId;
        bool found;
        foreignServerInfo = hash_search(ForeignServerShmemCache, &serverId, HASH_FIND, &found);
        if (!found || !foreignServerConnection->conn || // deleted server
            strcmp(foreignServerConnection->conn->connhost->hostaddr, foreignServerInfo->host) != 0 ||
            atoi(foreignServerConnection->conn->connhost->port) != foreignServerInfo->port) // server ip or port changed
        {
            PQfinish(foreignServerConnection->conn);
            foreignServerConnection->conn = NULL;
            hash_search(ForeignServerConnectionManager, &serverId, HASH_REMOVE, &found);
        }
    }

    // 2. renew local variable
    if (LocalServerId == -1) {
        hash_seq_init(&status, ForeignServerShmemCache);
        while ((foreignServerInfo = hash_seq_search(&status)) != NULL) {
            if (foreignServerInfo->is_local && foreignServerInfo->is_leader) {
                LocalServerId = foreignServerInfo->server_id;
                strcpy(LocalServerName, foreignServerInfo->server_name);
                hash_seq_term(&status);
                break;
            }
        }
    }

    // 3. update local version
    ForeignServerLocalConnectionManagerVersion = *ForeignServerShmemCacheVersion;

    if (needLock)
        LWLockRelease(&ForeignServerShmemControl->lock);
}

int32_t GetForeignServerCount()
{
    while (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
        ReloadForeignServerShmemCache();
    }
    LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);
    int32_t result = hash_get_num_entries(ForeignServerShmemCache);
    LWLockRelease(&ForeignServerShmemControl->lock);
    return result;
}

List *GetForeignServerInfo(List *foreignServerIdList)
{
    while (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
        ReloadForeignServerShmemCache();
    }
    LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);

    List *result = NIL;
    for (int i = 0; i < list_length(foreignServerIdList); ++i) {
        // int32_t serverId = list_nth_int(foreignServerIdList, i);
        ArrayType *serverId_list = list_nth(foreignServerIdList, i);
        Oid element_type = INT4OID;
        int16 elmlen;
        bool elmbyval;
        char elmalign;
        get_typlenbyvalalign(INT4OID, &elmlen, &elmbyval, &elmalign);
        Datum *elements;
        bool *nulls;
        int nelems;
        deconstruct_array(serverId_list, element_type, elmlen, elmbyval, elmalign, &elements, &nulls, &nelems);
        for (int j = 0; j < nelems; j++) {
            if (!nulls[j]) continue;
            int32_t serverId = DatumGetInt32(elements[j]);

            bool found;
            FormData_falcon_foreign_server *foreignServerInfo =
                hash_search(ForeignServerShmemCache, &serverId, HASH_FIND, &found);

            FormData_falcon_foreign_server *dataCopy = palloc(sizeof(FormData_falcon_foreign_server));
            memcpy(dataCopy, foreignServerInfo, sizeof(FormData_falcon_foreign_server));
            result = lappend(result, dataCopy);
        }
        pfree(elements);
        pfree(nulls);
    }

    LWLockRelease(&ForeignServerShmemControl->lock);
    return result;
}

static bool PGconnPrepare(PGconn *conn)
{
    PGresult *res = NULL;
    do {
        if (!PQenterPipelineMode(conn))
            break;
        if (!PQsendQueryParams(conn, "select falcon_prepare_commands();", 0, NULL, NULL, NULL, NULL, 0))
            break;
        if (!PQpipelineSync(conn))
            break;

        res = PQgetResult(conn);
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1 || PQnfields(res) != 1 ||
            strcmp(PQgetvalue(res, 0, 0), "0") != 0)
            break;
        PQclear(res);
        res = PQgetResult(conn);
        if (res != NULL)
            break;
        res = PQgetResult(conn);
        if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
            break;
        PQclear(res);
        res = PQgetResult(conn);
        if (res != NULL)
            break;
        return true;
    } while (0);
    if (res)
        PQclear(res);
    return false;
}

/* accept only list of int, not list of list */
List *GetForeignServerConnection(List *foreignServerIdList)
{
    while (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
        ReloadForeignServerShmemCache();
    }

    LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);
    if (ForeignServerLocalConnectionManagerVersion < *ForeignServerShmemCacheVersion) {
        RenewForeignServerLocalCache(false);
    }
    List *result = NIL;
    List *newStartedConn = NIL;
    ForeignServerConnection *foreignServerConnection;
    bool found;
    StringInfo connInfo = makeStringInfo();
    for (int i = 0; i < list_length(foreignServerIdList); ++i) {
        int32_t serverId = list_nth_int(foreignServerIdList, i);
        foreignServerConnection = hash_search(ForeignServerConnectionManager, &serverId, HASH_ENTER, &found);
        if (!found) {
            foreignServerConnection->serverId = serverId;
            foreignServerConnection->conn = NULL;
        }
        if (foreignServerConnection->conn && foreignServerConnection->conn->status == CONNECTION_BAD) {
            PQfinish(foreignServerConnection->conn);
            foreignServerConnection->conn = NULL;
        }
        if (foreignServerConnection->conn == NULL) {
            FormData_falcon_foreign_server *foreignServerInfo =
                hash_search(ForeignServerShmemCache, &serverId, HASH_FIND, &found);
            if (!found) {
                FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "trying to ask for invalid server, server id: %d.", serverId);
            }

            foreignServerConnection->transactionState = FALCON_REMOTE_TRANSACTION_NONE;
            resetStringInfo(connInfo);
            appendStringInfo(connInfo,
                             "hostaddr=%s port=%d user=%s dbname=postgres",
                             foreignServerInfo->host,
                             foreignServerInfo->port,
                             foreignServerInfo->user_name);
            foreignServerConnection->conn = PQconnectStart(connInfo->data);

            newStartedConn = lappend_int(newStartedConn, 1);
        } else {
            newStartedConn = lappend_int(newStartedConn, 0);
        }
        result = lappend(result, foreignServerConnection);
    }

    LWLockRelease(&ForeignServerShmemControl->lock);

    bool connectSucceed = true;
    for (int i = 0; i < list_length(result); ++i) {
        foreignServerConnection = list_nth(result, i);
        if (list_nth_int(newStartedConn, i)) // new started connection
        {
            bool validConnection = true;

            ConnStatusType status;
            while ((status = PQstatus(foreignServerConnection->conn)) != CONNECTION_OK) {
                if (status == CONNECTION_BAD) {
                    validConnection = false;
                    break;
                }
                pg_usleep(5);

                PQconnectPoll(foreignServerConnection->conn);
            }

            if (status == CONNECTION_OK && !PGconnPrepare(foreignServerConnection->conn)) {
                validConnection = false;
            }

            if (!validConnection) {
                PQfinish(foreignServerConnection->conn);
                foreignServerConnection->conn = NULL;
                connectSucceed = false;
            }
        }
    }
    if (!connectSucceed) {
        if (FalconIsInAbortProgress()) {
            FALCON_ELOG_WARNING(PROGRAM_ERROR, "connection to some server failed while aborting.");
        } else {
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "error while trying to get connection.");
        }
    }
    return result;
}

// accept list of arrays
List *GetForeignServerConnectionInfo(List *foreignServerIdList)
{
    while (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
        ReloadForeignServerShmemCache();
    }
    LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);

    List *result = NIL;
    for (int i = 0; i < list_length(foreignServerIdList); ++i) {
        List *serverId_list = list_nth(foreignServerIdList, i);
        List *group_info = NIL;
        for (int j = 0; j < list_length(serverId_list); ++j) {
            int32_t serverId = list_nth_int(serverId_list, j);
            bool found;
            FormData_falcon_foreign_server *foreignServerInfo =
                hash_search(ForeignServerShmemCache, &serverId, HASH_FIND, &found);
            if (!found && serverId != -1) {
                FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "trying to ask for invalid server, server id: %d.", serverId);
            }

            ForeignServerConnectionInfo *info = palloc(sizeof(ForeignServerConnectionInfo));

            if (serverId != -1) {
                strcpy(info->host, foreignServerInfo->host);
                info->port = foreignServerInfo->port;
            } else {
                strcpy(info->host, "");
                info->port = -1;
            }
            group_info = lappend(group_info, info);
        }
        result = lappend(result, group_info);
    }

    LWLockRelease(&ForeignServerShmemControl->lock);
    return result;
}

int32_t GetLocalServerId(void)
{
    if (LocalServerId == -1) {
        if (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
            ReloadForeignServerShmemCache();
        }
        RenewForeignServerLocalCache(true);
    }
    return LocalServerId;
}

const char *GetLocalServerName(void)
{
    if (LocalServerId == -1) {
        if (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
            ReloadForeignServerShmemCache();
        }
        RenewForeignServerLocalCache(true);
    }
    return LocalServerName;
}

static inline ArrayType *ConvertListToArray(List *intList, Oid elementType)
{
    if (intList == NIL) {
        return construct_empty_array(elementType);
    }

    int count = list_length(intList);
    Datum *elements = (Datum *)palloc(sizeof(Datum) * count);
    ListCell *cell;
    int i = 0;

    foreach(cell, intList) {
        elements[i] = Int32GetDatum(lfirst_int(cell));
        i++;
    }

    ArrayType *array = construct_array(elements, count, elementType, 
                                      sizeof(int32), true, 'i');
    pfree(elements);

    return array;
}

/* get only id of leader */
List *GetAllForeignServerId(bool exceptSelf, bool exceptCn)
{
    while (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
        ReloadForeignServerShmemCache();
    }
    LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);

    List *result = NIL;
    HASH_SEQ_STATUS status;
    FormData_falcon_foreign_server *foreignServerInfo;
    hash_seq_init(&status, ForeignServerShmemCache);
    while ((foreignServerInfo = hash_seq_search(&status)) != NULL) {
        if (exceptSelf && foreignServerInfo->is_local)
            continue;
        if (exceptCn && foreignServerInfo->group_id == 0) // we assume 0 to be cn
            continue;
        if (!foreignServerInfo->is_leader)
            continue;
        result = lappend_int(result, foreignServerInfo->server_id);
    }

    LWLockRelease(&ForeignServerShmemControl->lock);
    return result;
}

List *GetAllForeignServerId_Group(bool exceptSelf, bool exceptCn)
{
    while (pg_atomic_read_u32(ForeignServerShmemCacheInvalid)) {
        ReloadForeignServerShmemCache();
    }
    LWLockAcquire(&ForeignServerShmemControl->lock, LW_SHARED);

    // 用于按group_id分组的临时哈希表
    HTAB *groupHash = NULL;
    HASHCTL hashCtl;
    memset(&hashCtl, 0, sizeof(hashCtl));
    hashCtl.keysize = sizeof(int); // group_id
    hashCtl.entrysize = sizeof(GroupServerEntry);
    hashCtl.hcxt = CurrentMemoryContext;
    groupHash = hash_create("ForeignServer Group Hash", 
                          32, &hashCtl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

    List *result = NIL;
    HASH_SEQ_STATUS status;
    hash_seq_init(&status, ForeignServerShmemCache);
    FormData_falcon_foreign_server *foreignServerInfo;
    while ((foreignServerInfo = hash_seq_search(&status)) != NULL) {
        if (exceptSelf && foreignServerInfo->is_local)
            continue;
        if (exceptCn && foreignServerInfo->group_id == 0) // we assume 0 to be cn
            continue;
        // save (group_id, server_id[]) in the groupHash
        int group_id = foreignServerInfo->group_id;
        bool found = false;

        GroupServerEntry *entry = (GroupServerEntry *)hash_search(groupHash, 
                                                                  &group_id,
                                                                  HASH_ENTER,
                                                                  &found);
        if (!found) {
            entry->group_id = group_id;
            entry->server_list = NIL;
        }
        if (foreignServerInfo->is_leader) {
            // leader the first; leader is always valid
            entry->server_list = list_insert_nth_int(entry->server_list, 0, foreignServerInfo->server_id);
        } else if (foreignServerInfo->is_valid) {
            entry->server_list = lappend_int(entry->server_list, foreignServerInfo->server_id);
        } else {
            // -1 for Null
            entry->server_list = lappend_int(entry->server_list, -1);
        }
    }

    HASH_SEQ_STATUS hashStatus;
    GroupServerEntry *groupEntry;
    hash_seq_init(&hashStatus, groupHash);
    while ((groupEntry = hash_seq_search(&hashStatus)) != NULL) {
        // copy array from list
        ArrayType *array = ConvertListToArray(groupEntry->server_list, INT4OID);
        result = lappend(result, array);
        // free the temp list
        list_free(groupEntry->server_list);
    }
    hash_destroy(groupHash);

    LWLockRelease(&ForeignServerShmemControl->lock);
    return result;
}

size_t ForeignServerShmemsize()
{
    return sizeof(ShmemControlData) + sizeof(int32_t) + sizeof(pg_atomic_uint32) +
           sizeof(FormData_falcon_foreign_server) * FOREIGN_SERVER_NUM_MAX;
}
void ForeignServerShmemInit()
{
    bool initialized;
    ForeignServerShmemControl = ShmemInitStruct("Foreign Server Control", ForeignServerShmemsize(), &initialized);
    ForeignServerShmemCacheVersion = (int32_t *)(ForeignServerShmemControl + 1);
    ForeignServerShmemCacheInvalid = (pg_atomic_uint32 *)(ForeignServerShmemCacheVersion + 1);
    HASHCTL info;
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(FormData_falcon_foreign_server);
    ForeignServerShmemCache = ShmemInitHash("Falcon Foreign Server Shmem Hash Table",
                                            FOREIGN_SERVER_NUM_MAX,
                                            FOREIGN_SERVER_NUM_MAX,
                                            &info,
                                            HASH_ELEM | HASH_BLOBS);
    if (!initialized) {
        ForeignServerShmemControl->trancheId = LWLockNewTrancheId();
        ForeignServerShmemControl->lockTrancheName = "Falcon Foreign Server Control";
        LWLockRegisterTranche(ForeignServerShmemControl->trancheId, ForeignServerShmemControl->lockTrancheName);
        LWLockInitialize(&ForeignServerShmemControl->lock, ForeignServerShmemControl->trancheId);

        *ForeignServerShmemCacheVersion = 0;
        pg_atomic_init_u32(ForeignServerShmemCacheInvalid, 1);
    }
}

void InvalidateForeignServerShmemCache() { pg_atomic_exchange_u32(ForeignServerShmemCacheInvalid, 1); }

void InvalidateForeignServerShmemCacheCallback(Datum argument, Oid relationId)
{
    if (relationId == InvalidOid || relationId == ForeignServerRelationId()) {
        InvalidateForeignServerShmemCache();
    }
}

void ReloadForeignServerShmemCache()
{
    LWLockAcquire(&ForeignServerShmemControl->lock, LW_EXCLUSIVE);

    if (!pg_atomic_exchange_u32(ForeignServerShmemCacheInvalid, 0)) {
        LWLockRelease(&ForeignServerShmemControl->lock);
        return;
    }

    hash_clear(ForeignServerShmemCache);

    bool exceedMaxNumOfForeignServer = false;
    Relation rel = table_open(ForeignServerRelationId(), AccessShareLock);
    SysScanDesc scanDesc = systable_beginscan(rel, ForeignServerRelationIndexId(), true, NULL, 0, NULL);
    TupleDesc tupleDesc = RelationGetDescr(rel);

    Datum datumArray[Natts_falcon_foreign_server];
    bool isNullArray[Natts_falcon_foreign_server];
    HeapTuple heapTuple;
    while (HeapTupleIsValid(heapTuple = systable_getnext(scanDesc))) {
        if (hash_get_num_entries(ForeignServerShmemCache) >= FOREIGN_SERVER_NUM_MAX) {
            exceedMaxNumOfForeignServer = true;
            break;
        }

        heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);
        int32_t serverId = DatumGetInt32(datumArray[Anum_falcon_foreign_server_server_id - 1]);
        bool found;
        FormData_falcon_foreign_server *entry = hash_search(ForeignServerShmemCache, &serverId, HASH_ENTER, &found);

        entry->server_id = serverId;
        text_to_cstring_buffer((text *)datumArray[Anum_falcon_foreign_server_server_name - 1],
                               entry->server_name,
                               FOREIGN_SERVER_NAME_MAX_LENGTH);
        text_to_cstring_buffer((text *)datumArray[Anum_falcon_foreign_server_host - 1], entry->host, HOST_MAX_LENGTH);
        entry->port = DatumGetInt32(datumArray[Anum_falcon_foreign_server_port - 1]);
        entry->is_local = DatumGetBool(datumArray[Anum_falcon_foreign_server_is_local - 1]);
        text_to_cstring_buffer((text *)datumArray[Anum_falcon_foreign_server_user_name - 1],
                               entry->user_name,
                               FOREIGN_SERVER_NAME_MAX_LENGTH);
        entry->group_id = DatumGetInt32(datumArray[Anum_falcon_foreign_server_group_id - 1]);
        entry->is_leader = DatumGetBool(datumArray[Anum_falcon_foreign_server_is_leader - 1]);
        entry->is_valid = DatumGetBool(datumArray[Anum_falcon_foreign_server_is_valid - 1]);
    }

    systable_endscan(scanDesc);
    table_close(rel, AccessShareLock);

    if (exceedMaxNumOfForeignServer) {
        InvalidateForeignServerShmemCache();
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "exceed max number of foreign server: %d.", FOREIGN_SERVER_NUM_MAX);
    }

    *ForeignServerShmemCacheVersion += 1;

    LWLockRelease(&ForeignServerShmemControl->lock);
}
