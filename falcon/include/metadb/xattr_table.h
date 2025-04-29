/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_XATTR_TABLE_H
#define FALCON_XATTR_TABLE_H

#include "postgres.h"

#include "lib/stringinfo.h"

#define FILENAMELENGTH 256
#define KEYLENGTH 128
#define DEFAULT 0
#define XATTR_CREATE 1
#define XATTR_REPLACE 2

typedef struct FormData_falcon_xattr_table
{
    char name[FILENAMELENGTH];
    int64 parentid_partid;
    char xkey[KEYLENGTH];
    char xvalue[FILENAMELENGTH];
} FormData_falcon_xattr_table;
typedef FormData_falcon_xattr_table *Form_falcon_xattr_table;

#define Natts_falcon_xattr_table 4
#define Anum_falcon_xattr_table_parentid_partid 1
#define Anum_falcon_xattr_table_name 2
#define Anum_falcon_xattr_table_xkey 3
#define Anum_falcon_xattr_table_xvalue 4

typedef enum FalconXattrTableScankeyType {
    XATTR_TABLE_PARENT_ID_PART_ID_GT = 0,
    XATTR_TABLE_PARENT_ID_PART_ID_GE,
    XATTR_TABLE_PARENT_ID_PART_ID_LE,
    XATTR_TABLE_PARENT_ID_PART_ID_EQ,
    XATTR_TABLE_NAME_GT,
    XATTR_TABLE_NAME_EQ,
    XATTR_TABLE_XKEY_GT,
    XATTR_TABLE_XKEY_EQ,
    LAST_FALCON_XATTR_TABLE_SCANKEY_TYPE
} FalconXattrTableScankeyType;

extern const char *XattrTableName;
void ConstructCreateXattrTableCommand(StringInfo command, const char *name);

#endif
