/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_CONTROL_FLAG_H
#define FALCON_CONTROL_FLAG_H

#include "postgres.h"

size_t FalconControlShmemsize(void);
void FalconControlShmemInit(void);

bool CheckFalconBackgroundServiceStarted(void);

bool FalconIsInAbortProgress(void);
void FalconEnterAbortProgress(void);
void FalconQuitAbortProgress(void);

#endif
