/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/xattr_table.h"

#include "metadb/metadata.h"

const char *XattrTableName = "falcon_xattr_table";

void ConstructCreateXattrTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command,
                     "CREATE TABLE falcon.%s(parentid_partid bigint,"
                     "name text,"
                     "xKey text,"
                     "xValue text);"
                     "CREATE UNIQUE INDEX %s_index ON falcon.%s USING btree(parentid_partid, name, xKey);"
                     "ALTER TABLE falcon.%s SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.%s TO public;"
                     "ALTER EXTENSION falcon ADD TABLE %s;",
                     name,
                     name,
                     name,
                     name,
                     name,
                     name);
}
