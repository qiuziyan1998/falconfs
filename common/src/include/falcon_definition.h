/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <functional>
#include <string>

const std::string FALCON_DEFAULT_LOG_DIR = "/tmp/";

enum FalconLogLevel { LOG_TRACE = -2, LOG_DEBUG = -1, LOG_INFO = 0, LOG_WARNING = 1, LOG_ERROR = 2, LOG_FATAL = 3 };

using FalconLogHandler = std::function<void(FalconLogLevel level, const char *file, int line, const char *logContent)>;
