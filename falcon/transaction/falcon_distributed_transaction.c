/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "transaction/falcon_distributed_transaction.h"

#include "catalog/pg_namespace_d.h"
#include "utils/lsyscache.h"

#include "utils/utils.h"

const char *DistributedTransactionTableName = "falcon_distributed_transaction";
const char *DistributedTransactionTableIndexName = "falcon_distributed_transaction_index";

Oid FalconDistributedTransactionRelationId(void)
{
    GetRelationOid(DistributedTransactionTableName, &CachedRelationOid[CACHED_RELATION_DISTRIBUTED_TRANSACTION_TABLE]);
    return CachedRelationOid[CACHED_RELATION_DISTRIBUTED_TRANSACTION_TABLE];
}

Oid FalconDistributedTransactionRelationIndexId(void)
{
    GetRelationOid(DistributedTransactionTableIndexName,
                   &CachedRelationOid[CACHED_RELATION_DISTRIBUTED_TRANSACTION_TABLE_INDEX]);
    return CachedRelationOid[CACHED_RELATION_DISTRIBUTED_TRANSACTION_TABLE_INDEX];
}
