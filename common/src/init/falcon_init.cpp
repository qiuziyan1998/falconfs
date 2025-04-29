/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "init/falcon_init.h"

#include "conf/falcon_property_key.h"
#include "falcon_code.h"
#include "log/logging.h"

int32_t FalconModuleInit::Init()
{
    if (inited) {
        return FALCON_SUCCESS;
    }
    std::function<int32_t()> falconInitStepOps[] = {[&] { return InnerInit(); },
                                                    [&] { return InitConf(); },
                                                    [&] { return InitLog(); }};

    for (auto &initStep : falconInitStepOps) {
        int32_t ret = initStep();
        if (ret != OK) {
            return FALCON_ERR_INNER_FAILED;
        }
    }
    inited = true;
    FALCON_LOG(LOG_INFO) << "Init FALCON client successfully";
    return FALCON_SUCCESS;
}

int32_t FalconModuleInit::InnerInit()
{
    if (falconConfig == nullptr) {
        falconConfig = std::make_unique<FalconConfig>();
    }

    return OK;
}

int32_t FalconModuleInit::InitConf()
{
    if (configDir.empty()) {
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    return falconConfig->InitConf(configDir);
}

int32_t FalconModuleInit::InitLog()
{
    auto logMaxSize = falconConfig->GetUint32(FalconPropertyKey::FALCON_LOG_MAX_SIZE_MB);
    auto logDir = falconConfig->GetString(FalconPropertyKey::FALCON_LOG_DIR);
    if (logDir.empty()) {
        logDir = FALCON_DEFAULT_LOG_DIR;
    }

    auto logLevelString = falconConfig->GetString(FalconPropertyKey::FALCON_LOG_LEVEL);
    std::unordered_map<std::string, FalconLogLevel> logLevelMap = {{"TRACE", LOG_TRACE},
                                                                   {"DEBUG", LOG_DEBUG},
                                                                   {"INFO", LOG_INFO},
                                                                   {"WARNING", LOG_WARNING},
                                                                   {"ERROR", LOG_ERROR},
                                                                   {"FATAL", LOG_FATAL}};
    auto logLevel = (logLevelMap.count(logLevelString) ? logLevelMap[logLevelString] : LOG_INFO);

    uint reserved_num = falconConfig->GetUint32(FalconPropertyKey::FALCON_LOG_RESERVED_NUM);

    uint reserved_time = falconConfig->GetUint32(FalconPropertyKey::FALCON_LOG_RESERVED_TIME);

    int32_t ret =
        FalconLog::GetInstance()->InitLog(logLevel, GLOGGER, logDir, "falcon", logMaxSize, reserved_num, reserved_time);
    if (ret != OK) {
        FALCON_LOG(LOG_ERROR) << "Falcon init failed caused by init log failed, error code: " << ret;
        return ret;
    }

    FALCON_LOG(LOG_INFO) << "Init Falcon Log successfully";
    return OK;
}

std::shared_ptr<FalconConfig> &FalconModuleInit::GetFalconConfig() { return falconConfig; }

FalconModuleInit &GetInit()
{
    char *CONFIG_FILE = std::getenv("CONFIG_FILE");
    if (!CONFIG_FILE) {
        FALCON_LOG(LOG_ERROR) << "CONFIG_FILE not set";
    }
    std::string configFile = CONFIG_FILE ? CONFIG_FILE : "";
    static FalconModuleInit instance(configFile);
    return instance;
}
