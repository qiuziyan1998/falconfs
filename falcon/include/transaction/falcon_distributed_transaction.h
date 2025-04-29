/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_DISTRIBUTED_TRANSACTION_H
#define FALCON_DISTRIBUTED_TRANSACTION_H

#include "postgres.h"

typedef struct FormData_falcon_distributed_transaction
{
    int nodeId;
    text gid;
} FormData_falcon_distributed_transaction;
typedef FormData_falcon_distributed_transaction *Form_falcon_distributed_transaction;

#define Anum_falcon_distributed_transaction_nodeid 1
#define Anum_falcon_distributed_transaction_gid 2
#define Natts_falcon_distributed_transaction 2

Oid FalconDistributedTransactionRelationId(void);
Oid FalconDistributedTransactionRelationIndexId(void);

#endif
