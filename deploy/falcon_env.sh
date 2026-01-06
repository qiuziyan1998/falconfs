#!/usr/bin/env bash

SCRIPT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")")"
export PG_INSTALL_DIR="${PG_INSTALL_DIR:-$HOME/metadb}"

export FALCONFS_HOME="${SCRIPT_DIR}/.."
echo "Setting FALCONFS_HOME to ${FALCONFS_HOME} and updating PATH/LD_LIBRARY_PATH/PYTHONPATH"
export PATH=$PG_INSTALL_DIR/bin:$PATH
export LD_LIBRARY_PATH=$PG_INSTALL_DIR/lib:${LD_LIBRARY_PATH-}
export CONFIG_FILE=$FALCONFS_HOME/config/config.json
