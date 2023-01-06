#!/bin/bash
set -euxo pipefail

db=$1
table=$2

. ./env.sh
. ./log.sh

log_debug "[test] to delete db: ${db}, to delete table: ${table}"

## impala env: produce
eval $(get_impala_env_info_by_envname "produce")
impala_host=$tmp_impala_host
impala_port=$tmp_impala_port

main() {
    if [ -n ${db} ] && [ -n ${table} ]; then
        impala-shell -i ${impala_host}:${impala_port} -d ${db} -q "drop table ${table}" 2>/dev/null || true
        kudu table delete ${kudu_master_hosts[0]} impala::${db}.${table} || true
    fi
}

main