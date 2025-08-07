#!/bin/bash

ALL_SERVERS=(
    # "pro0"
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

for ((i=0; i<${#ALL_SERVERS[@]}; i++))
do
    SERVER=${ALL_SERVERS[$i]}
    ssh $SERVER "sudo pkill -f test_"
done