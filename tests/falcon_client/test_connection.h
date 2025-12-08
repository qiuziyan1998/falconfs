#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <cstdio>
#include <cstdlib>

#include <memory>
#include <chrono>
#include <thread>
#include <functional>
#include <iostream>
#include <format>

#include "connection.h"
#include "router.h"

class ConnectionUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {
        char *path = std::getenv("DEPLOY_PATH");
        if (!path) {
            std::cout << "env DEPLOY_PATH not set!" << std::endl;
            return;
        }
        deployPath = path;

        if (getenv("SERVER_IP") == nullptr || getenv("SERVER_PORT") == nullptr) {
            std::cout << "env SERVER_IP or SERVER_PORT is empty" << std::endl;
            return;
        }
        std::string serverIp = getenv("SERVER_IP");
        std::string serverPort = getenv("SERVER_PORT");
        coordinator = ServerIdentifier(serverIp, std::stoi(serverPort));

        // set up pg db
        auto command = std::format(
            "/bin/bash -c 'source \"{0}/falcon_env.sh\" && bash \"{1}/meta/falcon_meta_start.sh\"'", 
            deployPath, deployPath
        );
        int ret = std::system(command.c_str());
        if (ret != 0) {
            std::cout << "meta pg backend start failed!" << std::endl;
            return;
        }
        sleep(3);
        inited = true;
    }
    static void TearDownTestSuite() {
        if (deployPath.empty()) {
            std::cout << "env DEPLOY_PATH not set!" << std::endl;
            return;
        }
        auto command = std::format("bash {0}/meta/falcon_meta_stop.sh", deployPath);
        int ret = std::system(command.c_str());
        if (ret != 0) {
            std::cout << "meta pg backend stop failed!" << std::endl;
        }
    }
    void SetUp() override {
        if (!inited) {
            GTEST_SKIP() << "Database not inited, skipping all tests in ConnectionUT.";
        }
    }
    void TearDown() override {}

    static std::string deployPath;
    static ServerIdentifier coordinator;
    static bool inited;
    static std::shared_ptr<Router> router;
    static std::shared_ptr<Connection> conn;
    std::string path = "/test";
    struct stat stbuf;

};