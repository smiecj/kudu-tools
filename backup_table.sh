#!/bin/bash
#set -euxo pipefail

## get input to search table name. If input table name is empty, will use table name defined in env.sh
source_db_name=""
target_db_name=""
if [ $# -eq 2 ] && [ -n $1 ] && [ -n $2 ]; then
    source_db_name=$1
    target_db_name=$2
fi
if [ -z $source_db_name ]; then
    source_db_name="test"
    target_db_name="test_dest"
fi

. ./env.sh
. ./log.sh

# execute impala upsert into, put data from source table to dest table
## execute env: test
eval $(get_impala_env_info_by_envname "test")
impala_host=$tmp_impala_host
impala_port=$tmp_impala_port

## export table name and table structure base on dest database
impala_tables_str=`impala-shell -i ${impala_host}:${impala_port} -d $target_db_name -q 'show tables' 2>/dev/null`
log_debug "get all table from $target_db_name ret: $impala_tables_str"
impala_table_arr=($(echo $impala_tables_str | tr "," "\n"))
backup_table_count=0
for table_name in ${impala_table_arr[@]}
do
    if [[ "$table_name" =~ $impala_table_filter ]]; then
        table_name=`echo $table_name | tr -d " " | tr -d "|"`
        log_debug "after filter table name: $table_name"

        ## remove special character, exclude without "," line, remove last line, and add "," to each has column name's line except last line
        table_field_str=`impala-shell -i $impala_host:$impala_port -d $target_db_name -q "show create table $table_name" 2>/dev/null | tr -d "|" | tr -d "-" | tr -d "+" | sed "s/result   *//g" | sed '/,/!d' | sed '$ d' | sed 's/ [A-Z].*//g' | sed '$ ! s/$/,/g' | tr -d "\n"`
        echo "table field: $table_field_str"

        ## check whether this table is in source db, if not, upsert will not execute
        show_table_ret=`impala-shell -i $impala_host:$impala_port -d $source_db_name -q "show create table $table_name" 2>/dev/null`
        if [ $? -nq 0 ]; then
            log_warn "source db doesn't have table: $table_name, will not execute upsert"
            continue
        fi
        upsert_table_sql="upsert into table $target_db_name.$table_name ($table_field_str) select $table_field_str from $source_db_name.$table_name"
        log_info "upsert table $table_name sql: $upsert_table_sql"

        upsert_table_ret=`impala-shell -i $impala_host:$impala_port -d $target_db_name -q "$upsert_table_sql" 2>/dev/null`
        log_info "upsert table $table_name result: $upsert_table_ret"
        backup_table_count=$((backup_table_count+1))
    fi
done

log_info "backup table finish, total backup table count: $backup_table_count"
