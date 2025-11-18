#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <chrono>
#include <thread>
#include <functional>
#include <iostream>

class RouterUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {
    }
    static void TearDownTestSuite() {}
    void SetUp() override {}
    void TearDown() override {}
};