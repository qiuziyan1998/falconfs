#!/usr/bin/env bash
DIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}"))

$DIR/meta/falcon_meta_start.sh
$DIR/client/falcon_client_start.sh
