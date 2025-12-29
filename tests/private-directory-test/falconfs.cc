#include "dfs.h"
#include "falcon_meta.h"

#include <cstdlib>
#include <iostream>
#include <vector>
#include <atomic>

using namespace std;

#define SERVER_IP "10.0.3.1"
#define SERVER_PORT "56910"

#define PROGRAM_ERROR 17

static int s_clientNumber = 0;
static std::vector<std::shared_ptr<Router>> routers;
static std::atomic<uint64_t> routerIndex = 0;
static bool readMetaStandby = false;

int dfs_init(int client_number)
{
    if (getenv("SERVER_IP") == nullptr || getenv("SERVER_PORT") == nullptr || getenv("READ_STANDBY") == nullptr) {
        cout << "env SERVER_IP or SERVER_PORT or READ_STANDBY is empty" << endl;
        return -1;
    }
    std::string serverIp = getenv("SERVER_IP");
    std::string serverPort = getenv("SERVER_PORT");
    readMetaStandby = std::atoi(getenv("READ_STANDBY")) != 0;
    ServerIdentifier coordinator(serverIp, std::stoi(serverPort));
    s_clientNumber = client_number;
    while (client_number-- > 0) {
        routers.emplace_back(std::make_shared<Router>(coordinator));
    }
    return 0;
}

int dfs_open(const char *path, int flags, mode_t mode)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    struct stat st;
    int errorCode = SERVER_FAULT;
    std::shared_ptr<Connection> conn;

    uint64_t primaryLsn = 0;
    if (readMetaStandby) {
        std::vector<std::shared_ptr<Connection>> conns = routers[index]->GetWorkerConnByPath_Backup(std::string(path));
        if (conns.size() != 2) {
            std::cout << "FalconOpen: route error, no backup conn\n";
            return PROGRAM_ERROR;
        }
        conn = conns[1];

        primaryLsn = UINT64_MAX;
        if (conns[0]->cachedPrimaryLsn->get(primaryLsn)) {
            errorCode = conns[0]->Open(path, primaryLsn, inodeId, size, nodeId, &st);
        }
        if (errorCode != SUCCESS) {
            errorCode = conns[1]->Open(path, primaryLsn, inodeId, size, nodeId, &st);
        }
    } else {
        conn = routers[index]->GetWorkerConnByPath(std::string(path));
        if (!conn) {
            std::cout << "route error.\n";
            return PROGRAM_ERROR;
        }
        uint64_t primaryLsn = 0;
        errorCode = conn->Open(path, primaryLsn, inodeId, size, nodeId, &st);
    }
    return errorCode;
}

int dfs_read(int fd, void *buf, size_t count, off_t offset)
{
    assert(0);
    return pread(fd, buf, count, offset);
}
int dfs_write(int fd, const void *buf, size_t count, off_t offset)
{
    assert(0);
    return pwrite(fd, buf, count, offset);
}
int dfs_close(int fd, const char* path)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn)
    {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Close(path, size, 0, nodeId);
    return errorCode;
}
int dfs_mkdir(const char *path, mode_t mode)
{
    std::string dirPath(path);
    if (dirPath.length() > 1 && dirPath.back() == '/') {
        dirPath.pop_back();
    }

    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetCoordinatorConn();
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Mkdir(dirPath.c_str());
    return errorCode;
}
int dfs_rmdir(const char *path)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetCoordinatorConn();
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Rmdir(path);

    return errorCode;
}
int dfs_create(const char *path, mode_t mode)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn)
    {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    uint64_t inodeId;
    int32_t nodeId;
    struct stat st;
    memset(&st, 0, sizeof(st));
    int errorCode = conn->Create(path, inodeId, nodeId, &st);
    return errorCode;
}
int dfs_unlink(const char *path)
{   
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Unlink(path, inodeId, size, nodeId);
    return errorCode;
}
int dfs_stat(const char *path, struct stat *stbuf)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    int errorCode = SERVER_FAULT;
    std::shared_ptr<Connection> conn;
    uint64_t primaryLsn = 0;
    if (readMetaStandby) {
        std::vector<std::shared_ptr<Connection>> conns = routers[index]->GetWorkerConnByPath_Backup(std::string(path));
        if (conns.size() != 2) {
            std::cout << "dfs_stat: route error, no backup conn\n";
            return PROGRAM_ERROR;
        }
        conn = conns[1];
        primaryLsn = UINT64_MAX;
        if (conns[0]->cachedPrimaryLsn->get(primaryLsn)) {
            errorCode = conns[0]->Stat(path, primaryLsn, stbuf);
        }
        if (errorCode != SUCCESS) {
            errorCode = conns[1]->Stat(path, primaryLsn, stbuf);
        }        
    } else {
        conn = routers[index]->GetWorkerConnByPath(std::string(path));
        if (!conn) {
        std::cout << "route error.\n";
            return PROGRAM_ERROR;
        }
        uint64_t primaryLsn = 0;
        errorCode = conn->Stat(path, primaryLsn, stbuf);
    }
    return errorCode;
}

void dfs_shutdown()
{
    return;
}
