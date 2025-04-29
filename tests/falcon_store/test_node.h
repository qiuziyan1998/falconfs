#pragma once

#include <sstream>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "brpc/brpc_server.h"
#include "conf/falcon_property_key.h"
#include "init/falcon_init.h"

class NodeUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {
        std::cout << "Calling SetUpTestSuite!" << std::endl;
        int ret = GetInit().Init();
        if (ret != 0) {
            exit(1);
        }
        config = GetInit().GetFalconConfig();

        try {
            falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
            std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);
            std::stringstream ss(clusterView);
            while (ss.good()) {
                std::string substr;
                getline(ss, substr, ',');
                views.push_back(substr);
            }
            int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
            localEndpoint = views[nodeId];
            server.endPoint = localEndpoint;
            std::cout << "brpc endpoint = " << server.endPoint << std::endl;
            std::thread brpcServerThread(&falcon::brpc_io::RemoteIOServer::Run, &server);
            {
                std::unique_lock<std::mutex> lk(server.mutexStart);
                server.cvStart.wait(lk, [&server]() { return server.isStarted; });
            }
            brpcServerThread.detach();
            server.SetReadyFlag();
        } catch (const std::exception &e) {
            std::cerr << "发生错误: " << e.what() << std::endl;
            exit(1);
        }
    }
    static void TearDownTestSuite()
    {
        falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
        server.Stop();
    }
    void SetUp() override {}
    void TearDown() override {}
    static std::shared_ptr<FalconConfig> config;
    static std::string localEndpoint;
    static std::vector<std::string> views;
};
