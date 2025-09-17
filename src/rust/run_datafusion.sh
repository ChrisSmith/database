#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
# set -o xtrace

# for loop 1 to 22
for i in {1..22}; do
    id=$(printf "%02d" ${i})
    if ! target/release/db-rust "/Users/chris/src/database/src/Database.BenchmarkRunner/Queries/query_${id}.sql"; then
        echo "Query ${id} failed" 
    fi
done
