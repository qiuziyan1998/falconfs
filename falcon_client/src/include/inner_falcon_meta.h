/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>

#include "buffer/open_instance.h"

struct BatchCreatePrams
{
    std::vector<std::string> paths;
    std::vector<std::string> etags;
    std::vector<uint64_t> sizes;
    std::vector<int64_t> mtimes;

    bool operator==(const BatchCreatePrams &other) const { return paths == other.paths; }
};

int InnerFalconUnlink(uint64_t inodeId, int nodeId, std::string path);
int InnerFalconWrite(OpenInstance *openInstance, const char *buffer, size_t size, off_t offset);
int InnerFalconTmpClose(OpenInstance *openInstance, bool isFlush, bool isSync);
int InnerFalconRead(OpenInstance *openInstance, char *buffer, size_t size, off_t offset);
int InnerFalconAsyncCopy(uint64_t inodeId, int &backupNodeId);

int InnerFalconReadSmallFiles(OpenInstance *openInstance);
int InnerFalconStatFS(struct statvfs *vfsbuf);
int InnerFalconCopydata(const std::string &srcName, const std::string &dstName);
int InnerFalconDeleteDataAfterRename(const std::string &objectName);
int InnerFalconTruncateOpenInstance(OpenInstance *openInstance, off_t size);
int InnerFalconTruncateFile(OpenInstance *openInstance, off_t size);
