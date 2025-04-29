/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "control/hook.h"

#include "storage/ipc.h"

#include "dir_path_shmem/dir_path_hash.h"
#include "metadb/foreign_server.h"
#include "metadb/shard_table.h"
#include "transaction/transaction.h"
#include "utils/rwlock.h"
#include "utils/utils.h"

static void RegisterFalconPerBackendCallback(void);
static void ClearCachedDataIfNecessary(PlannedStmt *pstmt);
static void SavePreparedTransactionGid(PlannedStmt *pstmt);

ProcessUtility_hook_type pre_ProcessUtility_hook = NULL;
void falcon_ProcessUtility(PlannedStmt *pstmt,
                           const char *queryString,
                           bool readOnlyTree,
                           ProcessUtilityContext context,
                           ParamListInfo params,
                           QueryEnvironment *queryEnv,
                           DestReceiver *dest,
                           QueryCompletion *qc)
{
    RegisterFalconPerBackendCallback();
    standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);

    ClearCachedDataIfNecessary(pstmt);
}

ExecutorStart_hook_type pre_ExecutorStart_hook = NULL;
void falcon_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    RegisterFalconPerBackendCallback();

    standard_ExecutorStart(queryDesc, eflags);
}

object_access_hook_type pre_object_access_hook = NULL;
void falcon_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg) {}

void FalconCleanupOnExit(int code, Datum arg)
{
    RWLockReleaseAll(true);
    CleanupForeignServerConnections();
};

static void RegisterFalconPerBackendCallback(void)
{
    static bool registered = false;
    if (!registered) {
        before_shmem_exit(FalconCleanupOnExit, 0);

        registered = true;
    }
}

static void ClearCachedDataIfNecessary(PlannedStmt *pstmt)
{
    if (pstmt->utilityStmt->type != T_DropStmt)
        return;
    DropStmt *stmt = (DropStmt *)pstmt->utilityStmt;
    ListCell *cell;
    foreach (cell, stmt->objects) {
        Node *object = lfirst(cell);
        if (object->type != T_String)
            continue;
        String *s = castNode(String, object);
        if (strcmp(s->sval, "falcon") == 0) {
            memset(CachedRelationOid, 0, sizeof(CachedRelationOid));
            ClearDirPathHash();
            InvalidateForeignServerShmemCache();
            InvalidateShardTableShmemCache();
            break;
        }
    }
}

static void __attribute__((unused)) SavePreparedTransactionGid(PlannedStmt *pstmt)
{
    PreparedTransactionGid[0] = '\0';
    if (pstmt->utilityStmt->type != T_TransactionStmt) {
        return;
    }
    TransactionStmt *stmt = (TransactionStmt *)pstmt->utilityStmt;
    if (stmt->gid)
        strcpy(PreparedTransactionGid, stmt->gid);
}
