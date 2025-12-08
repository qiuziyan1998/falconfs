#include "test_connection.h"
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "utils.h"

std::string ConnectionUT::deployPath;
ServerIdentifier ConnectionUT::coordinator;
bool ConnectionUT::inited = false;
std::shared_ptr<Router> ConnectionUT::router;
std::shared_ptr<Connection> ConnectionUT::conn;

TEST_F(ConnectionUT, Init)
{
    router = std::make_shared<Router>(coordinator);
    ASSERT_NE(router, nullptr);
}

TEST_F(ConnectionUT, CreatePrimaryOnly)
{
    conn = router->GetWorkerConnByPath(path);
    ASSERT_NE(conn, nullptr);
    uint64_t inodeId;
    int32_t nodeId;
    int errorCode = conn->Create(path.c_str(), inodeId, nodeId, &stbuf);
    EXPECT_EQ(errorCode, SUCCESS);
}

TEST_F(ConnectionUT, StatPrimary)
{
    uint64_t primaryLsn = UINT64_MAX;
    // primaryLsn == UINT64_MAX, update cachedPrimaryLsn by stat and open
    EXPECT_EQ(conn->cachedPrimaryLsn->get(primaryLsn), false);
    int errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    EXPECT_EQ(errorCode, SUCCESS);
    EXPECT_EQ(conn->cachedPrimaryLsn->get(primaryLsn), true);
    std::cout << "primaryLsn = " << primaryLsn << std::endl;
}

TEST_F(ConnectionUT, StatStandbyReady)
{
    // definitely <= then any meta server's lsn
    uint64_t primaryLsn = 0;
    int errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    EXPECT_EQ(errorCode, SUCCESS);
}

TEST_F(ConnectionUT, StatCombine)
{
    // cache valid
    int errorCode = SERVER_FAULT;
    uint64_t primaryLsn = UINT64_MAX;
    if (conn->cachedPrimaryLsn->get(primaryLsn)) {
        errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    }
    if (errorCode != SUCCESS) {
        errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    }
    EXPECT_EQ(errorCode, SUCCESS);

    
    // cache invalid
    sleep(1);
    errorCode = SERVER_FAULT;
    primaryLsn = UINT64_MAX;
    if (conn->cachedPrimaryLsn->get(primaryLsn)) {
        errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    }
    if (errorCode != SUCCESS) {
        errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    }
    EXPECT_EQ(errorCode, SUCCESS);
}

TEST_F(ConnectionUT, StatStandbyTimeout)
{
    // definitely > then any meta server's lsn
    uint64_t primaryLsn = UINT64_MAX - 1;
    int errorCode = conn->Stat(path.c_str(), primaryLsn, &stbuf);
    EXPECT_EQ(errorCode, WAIT_LSN_TIMEDOUT);
}

TEST_F(ConnectionUT, OpenPrimary)
{
    // primaryLsn == UINT64_MAX, update cachedPrimaryLsn by stat and open
    uint64_t primaryLsn = UINT64_MAX;
    // just timeout, should be invalid
    EXPECT_EQ(conn->cachedPrimaryLsn->get(primaryLsn), false);

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    EXPECT_EQ(errorCode, SUCCESS);
}

TEST_F(ConnectionUT, OpenStandbyReady)
{
    // definitely <= then any meta server's lsn
    uint64_t primaryLsn = 0;

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    EXPECT_EQ(errorCode, SUCCESS);
}

TEST_F(ConnectionUT, OpenStandbyTimeout)
{
    // definitely > then any meta server's lsn
    uint64_t primaryLsn = UINT64_MAX - 1;

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    EXPECT_EQ(errorCode, WAIT_LSN_TIMEDOUT);
}

TEST_F(ConnectionUT, OpenCombine)
{
    // cache valid
    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = SERVER_FAULT;
    uint64_t primaryLsn = UINT64_MAX;
    if (conn->cachedPrimaryLsn->get(primaryLsn)) {
        errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    }
    if (errorCode != SUCCESS) {
        errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    }
    EXPECT_EQ(errorCode, SUCCESS);
    
    // cache invalid
    sleep(1);
    errorCode = SERVER_FAULT;
    primaryLsn = UINT64_MAX;
    if (conn->cachedPrimaryLsn->get(primaryLsn)) {
        errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    }
    if (errorCode != SUCCESS) {
        errorCode = conn->Open(path.c_str(), primaryLsn, inodeId, size, nodeId, &stbuf);
    }
    EXPECT_EQ(errorCode, SUCCESS);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
