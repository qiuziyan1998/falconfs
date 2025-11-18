#include "test_router.h"
#include "router.h"
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "utils.h"

std::shared_ptr<Router> router;

TEST_F(RouterUT, Start)
{
    std::shared_ptr<Connection> conn;
    router = std::make_shared<Router>(conn);
}

flatbuffers::DetachedBuffer CreatePlainCommandResponseSimple(int row, int col, std::vector<std::string> &data_strings) {
    flatbuffers::FlatBufferBuilder builder;

    std::vector<flatbuffers::Offset<flatbuffers::String>> data_vector;
    for (const auto& str : data_strings) {
        data_vector.push_back(builder.CreateString(str));
    }

    auto data_vec = builder.CreateVector(data_vector);

    // shardMinValue, shardMaxValue, ip0, ip1, ip2, port0, port1, port2, id0, id1, id2
    auto response = falcon::meta_fbs::CreatePlainCommandResponse(builder, row, col, data_vec);
    builder.Finish(response);

    return builder.Release();
}

TEST_F(RouterUT, TestFetchServerFromSingleRowSingleServer)
{
    std::vector<std::string> data_strings = {"0", "1000", "127.0.0.1", "1000", "0"};
    auto buffer = CreatePlainCommandResponseSimple(1, 5, data_strings);
    auto response = flatbuffers::GetRoot<falcon::meta_fbs::PlainCommandResponse>(buffer.data());

    int col = response->col();
    for (uint32_t row = 0; row < response->row(); row++) {
        auto servers = router->FetchServerFromRow(row, col, response);

        for (uint32_t i = 0; i < servers.size(); i++) {
            EXPECT_EQ(servers[i].ip, data_strings[row * col + 2 + i]);
            EXPECT_EQ(servers[i].port, std::stoi(data_strings[row * col + 2 + servers.size() + i]) + 10);
            EXPECT_EQ(servers[i].id, std::stoi(data_strings[row * col + 2 + servers.size() * 2 + i]));
        }
    }
}

TEST_F(RouterUT, TestFetchServerFromSingleRowMutipleServer)
{
    std::vector<std::string> data_strings = {"0", "1000", "127.0.0.1", "127.0.0.2", "1000", "1001", "0", "1"};
    auto buffer = CreatePlainCommandResponseSimple(1, 8, data_strings);
    auto response = flatbuffers::GetRoot<falcon::meta_fbs::PlainCommandResponse>(buffer.data());

    int col = response->col();
    for (uint32_t row = 0; row < response->row(); row++) {
        auto servers = router->FetchServerFromRow(row, col, response);

        for (uint32_t i = 0; i < servers.size(); i++) {
            EXPECT_EQ(servers[i].ip, data_strings[row * col + 2 + i]);
            EXPECT_EQ(servers[i].port, std::stoi(data_strings[row * col + 2 + servers.size() + i]) + 10);
            EXPECT_EQ(servers[i].id, std::stoi(data_strings[row * col + 2 + servers.size() * 2 + i]));
        }
    }
}

TEST_F(RouterUT, TestFetchServerFromSingleRowMutipleServerWithInvalid)
{
    int serversPerRow = 2;
    std::vector<std::string> data_strings = {"0", "1000", "127.0.0.1", "127.0.0.2", "1000", "1001", "0", "-1"};
    auto buffer = CreatePlainCommandResponseSimple(1, 8, data_strings);
    auto response = flatbuffers::GetRoot<falcon::meta_fbs::PlainCommandResponse>(buffer.data());

    int col = response->col();
    for (uint32_t row = 0; row < response->row(); row++) {
        auto servers = router->FetchServerFromRow(row, col, response);

        for (uint32_t i = 0, j = 0; i < servers.size(); i++, j++) {
            if (std::stoi(data_strings[row * col + 2 + serversPerRow * 2 + j]) == -1) j++;
            EXPECT_EQ(servers[i].ip, data_strings[row * col + 2 + j]);
            EXPECT_EQ(servers[i].port, std::stoi(data_strings[row * col + 2 + serversPerRow + j]) + 10);
            EXPECT_EQ(servers[i].id, std::stoi(data_strings[row * col + 2 + serversPerRow * 2 + j]));
        }
    }
}

TEST_F(RouterUT, TestFetchServerFromMultipleRowSingleServer)
{
    std::vector<std::string> data_strings = {"0", "1000", "127.0.0.1", "1000", "0",
                                             "1000", "2000", "127.0.0.2", "1001", "1"};
    auto buffer = CreatePlainCommandResponseSimple(2, 5, data_strings);
    auto response = flatbuffers::GetRoot<falcon::meta_fbs::PlainCommandResponse>(buffer.data());

    int col = response->col();
    for (uint32_t row = 0; row < response->row(); row++) {
        auto servers = router->FetchServerFromRow(row, col, response);

        for (uint32_t i = 0; i < servers.size(); i++) {
            EXPECT_EQ(servers[i].ip, data_strings[row * col + 2 + i]);
            EXPECT_EQ(servers[i].port, std::stoi(data_strings[row * col + 2 + servers.size() + i]) + 10);
            EXPECT_EQ(servers[i].id, std::stoi(data_strings[row * col + 2 + servers.size() * 2 + i]));
        }
    }
}

TEST_F(RouterUT, TestFetchServerFromMultipleRowMutipleServer)
{
    std::vector<std::string> data_strings = {"0", "1000", "127.0.0.1", "127.0.0.2", "1000", "1001", "0", "1",
                                             "1000", "2000", "127.0.0.3", "127.0.0.4", "1002", "1003", "2", "3"};
    auto buffer = CreatePlainCommandResponseSimple(2, 8, data_strings);
    auto response = flatbuffers::GetRoot<falcon::meta_fbs::PlainCommandResponse>(buffer.data());

    int col = response->col();
    for (uint32_t row = 0; row < response->row(); row++) {
        auto servers = router->FetchServerFromRow(row, col, response);

        for (uint32_t i = 0; i < servers.size(); i++) {
            EXPECT_EQ(servers[i].ip, data_strings[row * col + 2 + i]);
            EXPECT_EQ(servers[i].port, std::stoi(data_strings[row * col + 2 + servers.size() + i]) + 10);
            EXPECT_EQ(servers[i].id, std::stoi(data_strings[row * col + 2 + servers.size() * 2 + i]));
        }
    }
}

TEST_F(RouterUT, TestFetchServerFromMultipleRowMutipleServerWithInvalid)
{
    int serversPerRow = 2;
    std::vector<std::string> data_strings = {"0", "1000", "127.0.0.1", "127.0.0.2", "1000", "1001", "-1", "1",
                                             "1000", "2000", "127.0.0.3", "127.0.0.4", "1002", "1003", "2", "-1"};
    auto buffer = CreatePlainCommandResponseSimple(2, 8, data_strings);
    auto response = flatbuffers::GetRoot<falcon::meta_fbs::PlainCommandResponse>(buffer.data());

    int col = response->col();
    for (uint32_t row = 0; row < response->row(); row++) {
        auto servers = router->FetchServerFromRow(row, col, response);

        for (uint32_t i = 0, j = 0; i < servers.size(); i++, j++) {
            if (std::stoi(data_strings[row * col + 2 + serversPerRow * 2 + j]) == -1) j++;
            EXPECT_EQ(servers[i].ip, data_strings[row * col + 2 + j]);
            EXPECT_EQ(servers[i].port, std::stoi(data_strings[row * col + 2 + serversPerRow + j]) + 10);
            EXPECT_EQ(servers[i].id, std::stoi(data_strings[row * col + 2 + serversPerRow * 2 + j]));
        }
    }
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
