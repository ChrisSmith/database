#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

root_dir=$(dirname $(realpath $0))

pushd "${root_dir}/clickhouse"

nohup clickhouse server --config-file=config.xml > clickhouse.log 2>&1 &

echo "Clickhouse started in $(pwd)"
