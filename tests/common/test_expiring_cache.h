#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <chrono>
#include <thread>
#include <functional>

class ExpiringCacheUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {
    }
    static void TearDownTestSuite() {}
    void SetUp() override {}
    void TearDown() override {}

    static int ttl_ms;
};