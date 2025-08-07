#!/bin/bash

BIN_DIR="/root/falconfs/build/tests/private-directory-test"
TEST_PROGRAM="test_posix"
MOUNT_DIR="/mnt/data/" # falconfs mount path, end with /
FILE_PER_THREAD=1000
PORT=1111
FILE_SIZE=1572864
CLIENT_NUM=10
THREAD_NUM_PER_CLIENT=12
ROUND_INDEX=0
ROUND_NAME=("workload_init" "workload_create" "workload_stat" "workload_open" "workload_close" "workload_delete" "workload_mkdir" "workload_rmdir" "workload_open_write_close" "workload_open_write_close_nocreate" "workload_open_read_close" "workload_uninit")
CLIENT_ID=0
MOUNT_PER_CLIENT=1
CLIENT_CACHE_SIZE=16384
SERVER_IP="" # meta server ip
SERVER_PORT="" #meta server port

LD_LIBRARY_PATH=/usr/local/lib64/:$LD_LIBRARY_PATH $BIN_DIR/$TEST_PROGRAM $MOUNT_DIR $FILE_PER_THREAD $THREAD_NUM_PER_CLIENT $ROUND_INDEX $CLIENT_ID $MOUNT_PER_CLIENT $CLIENT_CACHE_SIZE $PORT $FILE_SIZE $CLIENT_NUM &

python3 send_signal.py 127.0.0.1 $PORT
