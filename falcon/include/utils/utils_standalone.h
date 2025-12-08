/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_UTILS_UTILS_STANDALONE_H
#define FALCON_UTILS_UTILS_STANDALONE_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

typedef struct {
    char **elements;
    int count;
} StringArray;

int64_t GetCurrentTimeInUs(void);

int64_t StringToInt64(const char *data);
uint64_t StringToUint64(const char *data);
int32_t StringToInt32(const char *data);
uint32_t StringToUint32(const char *data);

int pathcmp(const char *p1, const char *p2);

StringArray parse_text_array_direct(const char *array_str);

void free_string_array(StringArray *arr);

#endif
