#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

script_dir=$(dirname $(realpath $0))

cd src/Benchmarks

dotnet publish -c Release -o ${script_dir}/bin/
