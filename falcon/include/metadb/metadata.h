/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef DFS_METADATA_H
#define DFS_METADATA_H

#include "postgres.h"

#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "pg_config_manual.h"

#include "limits.h"

#define TYPE_DIR 'D'
#define TYPE_REG 'R'
#define TYPE_BLK 'B'

#define InodeIdIsValid(inodeid) ((inodeid) > LAST_ERROR_CODE)

// args that dfs functions might used
#define FILENAMELENGTH 256
#define KEYLENGTH 128

typedef enum FalconOpenModeMask {
    FALCON_OPEN_MODE_MASK_TYPE = 0x3,
    FALCON_OPEN_MODE_MASK_GET_REAL_FILE = 1 << 2,
    FALCON_OPEN_MODE_MASK_GET_DATA_ADDR = 1 << 3
} FalconOpenModeMask;

typedef enum FalconOpenType {
    FALCON_OPEN_TYPE_CREATE = 0,
    FALCON_OPEN_TYPE_DELETE = 1,
    FALCON_OPEN_TYPE_LOOKUP = 2,
} FalconOpenType;

typedef struct XattrInfo
{
    char xkey[KEYLENGTH];
    char xvalue[FILENAMELENGTH];
} XattrInfo;

typedef struct ServerHashInfo
{
    int32_t serverId;
    List *info;
} ServerHashInfo;

typedef struct ShardHashInfo
{
    int32_t shardId;
    List *info;
} ShardHashInfo;

#endif
