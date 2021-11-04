#!/bin/bash
set -euxo pipefail

## get input to search table name. If input table name is empty, will use table name defined in env.sh
input_db_name=""
if [ $# -eq 1 ] && [ -n $1 ]; then
	input_db_name=$1
fi
if [ -z $input_db_name ]; then
    input_db_name="test"
fi

. ./env.sh
. ./log.sh

# implement import table by sql

if [ ! -e $impala_create_table_sql_file_path ]; then
    log_error "The sql file is not exists!"
    exit
fi

# use impala shell to execute
import_ret=`impala-shell -i $impala_host:$impala_port -d $input_db_name -f $impala_create_table_sql_file_path`

log_info "impala table ret: $import_ret"
