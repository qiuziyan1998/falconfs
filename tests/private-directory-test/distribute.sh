#/bin/bash

ALL_SERVERS=(
    "pro0"
    "pro1"
    "pro2"
    "pro3"
    "tea4"
    "tea5"
    "tea6"
    "tea7"
    "val16"
    "val17"
    "val18"
    "tea1"
    "tea3"
)

for SERVER in ${ALL_SERVERS[@]}
do
    rsync -a build/test_falcon root@$SERVER:/root
    rsync -a build/test_posix root@$SERVER:/root
done