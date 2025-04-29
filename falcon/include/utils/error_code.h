/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_ERROR_CODE_H
#define FALCON_ERROR_CODE_H

#include "remote_connection_utils/error_code_def.h"

extern const char *FalconErrorCodeToString[LAST_FALCON_ERROR_CODE + 1];

FalconErrorCode FalconErrorMsgAnalyse(const char *originalErrorMsg, const char **errorMsg);

#endif
