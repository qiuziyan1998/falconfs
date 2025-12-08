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

class ConnectionUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {

    }
    static void TearDownTestSuite() {

    }
    void SetUp() override {}
    void TearDown() override {}

};