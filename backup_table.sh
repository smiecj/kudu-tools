#!/bin/bash
set -euxo pipefail

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
. ./common.sh

## common func: get timestamp column name
get_timestamp_column_name() {
    local table_field_str=$1

    if [[ "$table_field_str" =~ .*create_time_.* ]]; then
        echo "create_time_"
    elif [[ "$table_field_str" =~ .*update_time.* ]]; then
        echo "update_time"
    elif [[ "$table_field_str" =~ .*create_time.* ]]; then
        echo "create_time"
    else
        echo ""
    fi
}

# execute impala upsert into, put data from source table to dest table
## execute env: test
eval $(get_impala_env_info_by_envname "test")
impala_host=$tmp_impala_host
impala_port=$tmp_impala_port

## get all kudu tables
impala_tables_str=`impala-shell -i ${impala_host}:${impala_port} -d $target_db_name -q 'show tables' 2>/dev/null`
log_debug "get all table from $target_db_name ret: $impala_tables_str"
impala_table_arr=($(echo $impala_tables_str | tr "," "\n"))
backup_table_count=0
not_sync_table_arr=()
for table_name in ${impala_table_arr[@]}
do
    table_name=$(filter_table $table_name)
    if [[ -n "$table_name" ]]; then
        table_name=`echo $table_name | tr -d " " | tr -d "|"`
        log_debug "after filter table name: $table_name"

        if [ -n "$backup_table_allowlist" ] && [[ ! $backup_table_allowlist =~ $table_name ]]; then
            log_info "table: $table_name not in allow list, will not backup"
            continue
        fi

        ## remove special character, exclude without "," line, remove last line, and add "," to each has column name's line except last line
        table_field_str=`impala-shell -i $impala_host:$impala_port -d $target_db_name -q "show create table $table_name" 2>/dev/null \
            | tr -d "|" | tr -d "-" | tr -d "+" | sed "s/result   *//g" | sed '/,/!d' | sed '$ d' | sed 's/ [A-Z].*//g' \
            | sed '$ ! s/$/,/g' | tr -d "\n" | tr -d " " | sed 's/,/\`,\`/g' | sed 's/^/\`/g' | sed 's/$/\`/g'`
        log_debug "table field: $table_field_str"

        ## check whether this table is in source db, if not, upsert will not execute
        source_table_name=$(get_source_table_name $source_db_name $table_name $impala_host $impala_port)
        if [ -z $source_table_name ]; then
            log_warn "source db doesn't have table: $table_name, will not execute upsert"
            not_sync_table_arr+=($table_name)
            continue
        fi

        upsert_table_sql="upsert into table $target_db_name.$table_name ($table_field_str) select $table_field_str from $source_db_name.$source_table_name WHERE 1 = 1"
        if [ -n "$backup_before_date" ]; then
            timestamp_column=$(get_timestamp_column_name $table_field_str)
            if [ -n "$timestamp_column" ]; then
                ### finally need check time column value is not NULL
                time_value_str=`impala-shell -i $impala_host:$impala_port -d $source_db_name -q "SELECT $timestamp_column FROM $source_table_name ORDER BY $timestamp_column DESC LIMIT 1" 2>/dev/null | tr -d "|" | tr -d "-" | tr -d "+" | sed "s/$timestamp_column*//g" | tr -d " " | tr -d "\n"`
                if [ "NULL" == "$time_value_str" ]; then
                    upsert_table_sql="$upsert_table_sql AND $source_db_name.$source_table_name.$timestamp_column / 1000 < UNIX_TIMESTAMP(\"$backup_before_date\")"
                else
                    log_warn "table: $table_name, time column value is NULL"
                fi
            fi
        fi
        log_info "upsert table $table_name sql: $upsert_table_sql"

        upsert_table_ret=`impala-shell -i $impala_host:$impala_port -d $target_db_name -q "$upsert_table_sql" 2>&1 | grep "Modified" | awk -F' {1,}' '{print $2}'`
        log_info "upsert table $table_name result: update rows: $upsert_table_ret"
        backup_table_count=$((backup_table_count+1))
    fi
done

if (( ${#not_sync_table_arr[@]} )); then
    log_info "[tables] not sync table (without stg table)"
    for table_name in ${not_sync_table_arr[@]}
    do
    log_info $table_name
    done
fi

log_info "backup table finish, total backup table count: $backup_table_count"
