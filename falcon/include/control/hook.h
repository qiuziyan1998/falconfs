/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_HOOL_H
#define FALCON_HOOL_H

#include "postgres.h"

#include "catalog/objectaccess.h"
#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "tcop/utility.h"

extern ProcessUtility_hook_type pre_ProcessUtility_hook;
void falcon_ProcessUtility(PlannedStmt *pstmt,
                           const char *queryString,
                           bool readOnlyTree,
                           ProcessUtilityContext context,
                           ParamListInfo params,
                           QueryEnvironment *queryEnv,
                           DestReceiver *dest,
                           QueryCompletion *qc);

extern ExecutorStart_hook_type pre_ExecutorStart_hook;
void falcon_ExecutorStart(QueryDesc *queryDesc, int eflags);

extern object_access_hook_type pre_object_access_hook;
void falcon_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg);

void FalconCleanupOnExit(int code, Datum arg);

#endif
