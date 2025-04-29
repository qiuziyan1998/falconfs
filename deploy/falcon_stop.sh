#!/usr/bin/env bash

DIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}"))

$DIR/client/falcon_client_stop.sh
$DIR/meta/falcon_meta_stop.sh
