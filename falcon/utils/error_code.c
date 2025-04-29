/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "utils/error_log.h"

#undef FALCON_ERROR_CODE
#define FALCON_ERROR_CODE(code) #code,
const char *FalconErrorCodeToString[LAST_FALCON_ERROR_CODE + 1] = {FALCON_ERROR_CODE_LIST};

FalconErrorCode FalconErrorMsgAnalyse(const char *originalErrorMsg, const char **errorMsg)
{
    if (originalErrorMsg == NULL) {
        if (errorMsg)
            *errorMsg = NULL;
        return PROGRAM_ERROR;
    }
    char errorCodeChar = originalErrorMsg[8];
    if (errorCodeChar > 64 && errorCodeChar < (64 + LAST_FALCON_ERROR_CODE)) {
        *errorMsg = originalErrorMsg + 9;
        return (FalconErrorCode)(errorCodeChar - 64);
    } else {
        *errorMsg = originalErrorMsg;
        return UNKNOWN;
    }
}
