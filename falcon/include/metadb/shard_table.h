/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_SHARD_TABLE_H
#define FALCON_SHARD_TABLE_H

#include "postgres.h"

#include "nodes/pg_list.h"

#include "foreign_server.h"

#define SHARD_COUNT_MAX 100000000
#define SHARD_TABLE_RANGE_MIN 0
#define SHARD_TABLE_RANGE_MAX INT32_MAX

typedef struct {
    int32_t servers[FOREIGN_SERVER_GROUP_NUM_MAX];
    int32_t count;
} ServerList;

typedef struct FormData_falcon_shard_table
{
    int32_t range_point;
    ServerList server_ids;
} FormData_falcon_shard_table;
typedef FormData_falcon_shard_table *Form_falcon_shard_table;

#define Natts_falcon_shard_table 2
#define Anum_falcon_shard_table_range_point 1
#define Anum_falcon_shard_table_server_ids 2

Oid ShardRelationId(void);
Oid ShardRelationIndexId(void);

void SearchShardInfoByShardValue(uint64_t shardColValue, int32_t *rangePoint, int32_t *serverId);
void SearchShardInfoByShardValue_Group(uint64_t shardColValue, int32_t *rangePoint, int32_t *serverId, int32_t *count);
List *GetShardTableData(void);
int32_t GetShardTableSize(void);

size_t ShardTableShmemsize(void);
void ShardTableShmemInit(void);

void InvalidateShardTableShmemCacheCallback(Datum argument, Oid relationId);
void InvalidateShardTableShmemCache(void);
void ReloadShardTableShmemCache(void);
ServerList ExtractServersFromArray(Datum arrayDatum);

typedef struct ShardInfo
{
    int32_t rangeMin;
    int32_t rangeMax;

    int32_t count;
    char hosts[FOREIGN_SERVER_GROUP_NUM_MAX][HOST_MAX_LENGTH];
    int32_t ports[FOREIGN_SERVER_GROUP_NUM_MAX];
    int32_t serverIds[FOREIGN_SERVER_GROUP_NUM_MAX];
} ShardInfo;

#endif
