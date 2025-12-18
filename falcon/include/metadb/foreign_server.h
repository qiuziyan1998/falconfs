/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_FOREIGN_SERVER_H
#define FALCON_FOREIGN_SERVER_H

#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

#include "transaction/transaction.h"

#define FALCON_CN_SERVER_ID 0

#define FOREIGN_SERVER_NAME_MAX_LENGTH 128
#define HOST_MAX_LENGTH 16
#define FOREIGN_SERVER_NUM_EXPECT 8
#define FOREIGN_SERVER_NUM_MAX 128
#define FOREIGN_SERVER_GROUP_NUM_MAX 4

typedef struct FormData_falcon_foreign_server
{
    int32_t server_id;
    char server_name[FOREIGN_SERVER_NAME_MAX_LENGTH];
    char host[HOST_MAX_LENGTH];
    int32_t port;
    bool is_local;
    char user_name[FOREIGN_SERVER_NAME_MAX_LENGTH];
    int32_t group_id;
    bool is_leader;
    bool is_valid;
} FormData_falcon_foreign_server;
typedef FormData_falcon_foreign_server *Form_falcon_foreign_server;

#define Natts_falcon_foreign_server 9
#define Anum_falcon_foreign_server_server_id 1
#define Anum_falcon_foreign_server_server_name 2
#define Anum_falcon_foreign_server_host 3
#define Anum_falcon_foreign_server_port 4
#define Anum_falcon_foreign_server_is_local 5
#define Anum_falcon_foreign_server_user_name 6
#define Anum_falcon_foreign_server_group_id 7
#define Anum_falcon_foreign_server_is_leader 8
#define Anum_falcon_foreign_server_is_valid 9

Oid ForeignServerRelationId(void);
Oid ForeignServerRelationIndexId(void);
Oid ForeignServerRelationGroupIdIndexId(void);

size_t ForeignServerShmemsize(void);
void ForeignServerShmemInit(void);

void InvalidateForeignServerShmemCache(void);
void InvalidateForeignServerShmemCacheCallback(Datum argument, Oid relationId);
void ReloadForeignServerShmemCache(void);

void ForeignServerCacheInit(void);

// c api to insert foreign server
void InsertForeignServer(const int32_t serverId,
                         const char *serverName,
                         const char *host,
                         const int32_t port,
                         const bool isLocal,
                         const char *userName,
                         const int32_t group_id,
                         const bool is_leader,
                         const bool is_valid);
void DeleteForeignServer(const int32_t serverId);
void UpdateForeignServer(const int32_t serverId, const char *host, const int32_t port);

int32_t GetForeignServerCount(void);
List *GetForeignServerInfo(List *foreignServerIdList);
List *GetForeignServerConnection(List *foreignServerIdList);
List *GetForeignServerConnectionInfo(List *foreignServerIdList);
int32_t GetLocalServerId(void);
const char *GetLocalServerName(void);
List *GetAllForeignServerId(bool exceptSelf, bool exceptCn);
List *GetAllForeignServerId_Group(bool exceptSelf, bool exceptCn);

void CleanupForeignServerConnections(void);

typedef struct ForeignServerConnection
{
    int32_t serverId;
    PGconn *conn;
    FalconRemoteTransactionState transactionState;
} ForeignServerConnection;

typedef struct ForeignServerConnectionInfo
{
    char host[HOST_MAX_LENGTH];
    int32_t port;
} ForeignServerConnectionInfo;

typedef enum FalconForeignServerTableScankeyType {
    FOREIGN_SERVER_TABLE_SERVER_ID_EQ = 0,
    FOREIGN_SERVER_TABLE_GROUP_ID_EQ,
    LAST_FALCON_FOREIGN_SERVER_TABLE_SCANKEY_TYPE
} FalconForeignServerTableScankeyType;

typedef struct {
    int32_t group_id;
    List *server_list;
} GroupServerEntry;

#endif
