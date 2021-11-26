#!/bin/bash
set -euxo pipefail

## get input to search table name. If input table name is empty, will use table name defined in env.sh
db_name=""
source_db_name=""
if [ $# -eq 2 ] && [ -n $1 ] && [ -n $2 ]; then
    db_name=$1
    source_db_name=$2
fi
if [ -z $source_db_name ]; then
    db_name="test"
    source_db_name="stg"
fi
. ./env.sh
. ./log.sh
. ./common.sh

eval $(get_impala_env_info_by_envname "test")
test_impala_host=$tmp_impala_host
test_impala_port=$tmp_impala_port

eval $(get_impala_env_info_by_envname "produce")
produce_impala_host=$tmp_impala_host
produce_impala_port=$tmp_impala_port

## check all kudu table
impala_tables_str=`impala-shell -i ${test_impala_host}:${test_impala_port} -d $db_name -q 'show tables' 2>/dev/null`
log_debug "get all table from $db_name ret: $impala_tables_str"
impala_table_arr=($(echo $impala_tables_str | tr "," "\n"))

# 1: kudu sync success table, count match produce env
declare -a sync_success_table

# 2: kudu syncing table, count not match produce env -- need check table data count
## actual env (for example: test env) with less data need pay more attention
declare -a syncing_table_with_stg_table_less

declare -a syncing_table_with_stg_table_more

# 3: kudu syncing table, without stg table -- need sync stg task
declare -a syncing_without_stg_table

# 4: size not 0 table, and without sync task -- no need atention
declare -a syncing_without_sync_task_table

# 5: stg level not exist table, but with sync task -- need sync stg task
declare -a size_0_with_sync_task_table

# 6: stg level not exist table, and has no sync task -- no need atention
declare -a size_0_without_sync_task_table

## record table data count
declare -A source_table_data_count_map
declare -A target_table_data_count_map

for table_name in ${impala_table_arr[@]}
do
    if [[ "$table_name" =~ $impala_table_filter ]]; then
        table_name=`echo $table_name | tr -d " " | tr -d "|"`
        log_debug "after filter table name: $table_name"

        test_table_count_ret=`impala-shell -i $test_impala_host:$test_impala_port -d $db_name -q "SELECT COUNT(*) FROM $table_name" 2>/dev/null | tr -d "|" | tr -d "-" | tr -d "+" | sed "s/.*count.*//g" | tr -d " " | tr -d "\n"`
        log_debug "test table count ret: $test_table_count_ret"
        
        produce_table_count_ret=`impala-shell -i $produce_impala_host:$produce_impala_port -d $db_name -q "SELECT COUNT(*) FROM $table_name" 2>/dev/null | tr -d "|" | tr -d "-" | tr -d "+" | sed "s/.*count.*//g" | tr -d " " | tr -d "\n"`
        log_debug "produce table count ret: $produce_table_count_ret"

        source_table_data_count_map["$table_name"]=$produce_table_count_ret
        target_table_data_count_map["$table_name"]=$test_table_count_ret

        if [ "$test_table_count_ret" == "$produce_table_count_ret" ]; then
            log_info "table $table_name data count match"
            sync_success_table+=($table_name)
        elif [ $test_table_count_ret -gt 0 ]; then
            find_task_ret=`cat $sync_task_tables_file | grep $table_name || true`
            source_table_name=$(get_source_table_name $source_db_name $table_name $test_impala_host $test_impala_port)
            show_table_ret=`impala-shell -i $test_impala_host:$test_impala_port -d $source_db_name -q "show create table $source_table_name" 2>/dev/null || true`
            if [ -z "$find_task_ret" ]; then
                log_info "table $table_name data count not match and without sync task"
                syncing_without_sync_task_table+=($table_name)
            elif [ -z "$show_table_ret" ]; then
                log_info "table $table_name syncing, and without stg table"
                syncing_without_stg_table+=($table_name)
            else
                log_info "table $table_name syncing, and with stg table"
                if [ $test_table_count_ret -gt $produce_table_count_ret ]; then
                    syncing_table_with_stg_table_more+=($table_name)
                else
                    syncing_table_with_stg_table_less+=($table_name)
                fi
            fi
        else
            find_task_ret=`cat $sync_task_tables_file | grep $table_name || true`
            if [ -n "$find_task_ret" ]; then
                size_0_with_sync_task_table+=($table_name)
            else
                size_0_without_sync_task_table+=($table_name)
            fi
        fi
    fi
done

if (( ${#sync_success_table[@]} )); then
    log_info "[tables] sync success table (count match): ${#sync_success_table[@]}"
    for table_name in ${sync_success_table[@]}
    do
    log_info $table_name
    done
fi

if (( ${#syncing_table_with_stg_table_more[@]} )); then
    log_info "[tables] syncing and with stg table (count not match, data more then produce): ${#syncing_table_with_stg_table_more[@]}"
    for table_name in ${syncing_table_with_stg_table_more[@]}
    do
    log_info "$table_name data count: ${source_table_data_count_map[$table_name]} => ${target_table_data_count_map[$table_name]}"
    done
fi

if (( ${#syncing_table_with_stg_table_less[@]} )); then
    log_info "[tables] syncing and with stg table (count not match, data less then produce): ${#syncing_table_with_stg_table_less[@]}"
    for table_name in ${syncing_table_with_stg_table_less[@]}
    do
    log_info "$table_name data count: ${source_table_data_count_map[$table_name]} => ${target_table_data_count_map[$table_name]}"
    done
fi

if (( ${#syncing_without_stg_table[@]} )); then
    log_info "[tables] syncing but without stg table: ${#syncing_without_stg_table[@]}"
    for table_name in ${syncing_without_stg_table[@]}
    do
    log_info $table_name
    done
fi

if (( ${#syncing_without_sync_task_table[@]} )); then
    log_info "[tables] syncing but without sync task: ${#syncing_without_sync_task_table[@]}"
    for table_name in ${syncing_without_sync_task_table[@]}
    do
    log_info $table_name
    done
fi

if (( ${#size_0_with_sync_task_table[@]} )); then
    log_info "[tables] size 0 but with sync task (without stg table): ${#size_0_with_sync_task_table[@]}"
    for table_name in ${size_0_with_sync_task_table[@]}
    do
    log_info $table_name
    done
fi

if (( ${#size_0_without_sync_task_table[@]} )); then
    log_info "[tables] size 0 and without sync task table: ${#size_0_without_sync_task_table[@]}"
    for table_name in ${size_0_without_sync_task_table[@]}
    do
    log_info $table_name
    done
fi
