/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "inner_falcon_meta.h"

#include "falcon_store/falcon_store.h"

int InnerFalconWrite(OpenInstance *openInstance, const char *buffer, size_t size, off_t offset)
{
    return FalconStore::GetInstance()->WriteFile(openInstance, buffer, size, offset);
}

int InnerFalconTmpClose(OpenInstance *openInstance, bool isFlush, bool isSync)
{
    return FalconStore::GetInstance()->CloseTmpFiles(openInstance, isFlush, isSync);
}

int InnerFalconRead(OpenInstance *openInstance, char *buffer, size_t size, off_t offset)
{
    return FalconStore::GetInstance()->ReadFile(openInstance, buffer, size, offset);
}

int InnerFalconReadSmallFiles(OpenInstance *openInstance)
{
    return FalconStore::GetInstance()->ReadSmallFiles(openInstance);
}

int InnerFalconStatFS(struct statvfs *vfsbuf) { return FalconStore::GetInstance()->StatFS(vfsbuf); }

int InnerFalconCopydata(const std::string &srcName, const std::string &dstName)
{
    return FalconStore::GetInstance()->CopyData(srcName, dstName);
}

int InnerFalconDeleteDataAfterRename(const std::string &objectName)
{
    int deleteRet = FalconStore::GetInstance()->DeleteDataAfterRename(objectName);
    if (deleteRet != 0) {
        FALCON_LOG(LOG_ERROR) << "delete object: " << objectName << " in rename failed!";
    }
    return deleteRet;
}

int InnerFalconTruncateOpenInstance(OpenInstance *openInstance, off_t size)
{
    return FalconStore::GetInstance()->TruncateOpenInstance(openInstance, size);
}

int InnerFalconTruncateFile(OpenInstance *openInstance, off_t size)
{
    return FalconStore::GetInstance()->TruncateFile(openInstance, size);
}

int InnerFalconUnlink(uint64_t inodeId, int nodeId, std::string path)
{
    return FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
}
