#!/bin/bash
set -exo pipefail

source_db_name="test"
target_db_name="stream"
target_table_name=""

if [ $# -ge 2 ] && [ -n $2 ]; then
	source_db_name=$1
    target_db_name=$2
fi

if [ $# -eq 3 ] && [ -n "${actual_table_name}" ] ; then
    target_table_name=$3
fi

. ./env.sh
. ./log.sh
. ./common.sh

log_info "[test] target table: $target_table_name"

log_debug "[test] source db: ${source_db_name}, target db: ${target_db_name}"

mkdir -p $impala_table_sql_store_folder
#echo "create database if not exists $source_db_name" > $impala_transform_table_sql_file_path
echo "" > $impala_transform_table_sql_file_path

## impala env: produce
eval $(get_impala_env_info_by_envname "produce")
impala_host=$tmp_impala_host
impala_port=$tmp_impala_port

main() {
    # refresh metadata if needed
    if [ -n ${actual_table_name} ]; then
        impala-shell -i ${impala_host}:${impala_port} -d ${source_db_name} -q "invalidate metadata ${actual_table_name}"
    fi

    # get all table name in specify db name
    impala_tables_str=`impala-shell -i ${impala_host}:${impala_port} -d $source_db_name -q 'show tables' 2>/dev/null | sed "s/.* name .*//g" | tr -d '+' | tr -d ' ' | tr -d '|' | tr -d '-' | sed -r '/^\s*$/d'`
    log_debug "get all table ret: $impala_tables_str"
    impala_table_arr=($(echo $impala_tables_str))
    create_table_str=""
    for table_name in ${impala_table_arr[@]}
    do
        log_debug "current table name: $table_name"
        table_name=$(filter_table "$table_name")
        log_debug "after filter table name: $table_name"
        if [[ -n "${table_name}" ]]; then
            table_name=`echo $table_name | tr -d " " | tr -d "|"`
            log_debug "after filter table name: $table_name"

            # get all table create sql, and output to log file
            create_table_str=`impala-shell -i $impala_host:$impala_port -d $source_db_name -q "show create table $table_name" 2>/dev/null || true`
            if [ -z "$create_table_str" ]; then
                log_warn "table $table_name get schema null"
                continue
            fi
            create_table_str=`echo $create_table_str | tr -d "|" | tr -d "-" | tr -d "+" | tr -d "\n" | sed "s/result   *//g"`
            ## get primary key from fist field and set not null
            primary_key=`echo $create_table_str | sed 's/.* ( //g' | sed 's/ .*//g'`
            log_debug "current table primary key: $primary_key"
            create_table_str=`echo $create_table_str | sed "s/ $primary_key BIGINT / $primary_key BIGINT NOT /g"`
            ### primary key type maybe string
            create_table_str=`echo $create_table_str | sed "s/ $primary_key STRING / $primary_key STRING NOT /g"`
            create_table_str=`echo $create_table_str | sed "s/) WITH SERDEPROPERTIES/, PRIMARY KEY($primary_key) ) WITH SERDEPROPERTIES/g"`

            if [ -n "$target_table_name" ]; then
                log_info "[test] target table name: ${target_table_name}"
                create_table_str=`echo "$create_table_str" | sed "s#$source_db_name.$table_name (#$source_db_name.$target_table_name (#g"`
            fi

            log_debug "current create table sql: $create_table_str"
        fi
    done

    if [ -z "${create_table_str}" ]; then
        exit 0
    fi

    # At last we need to replace some keyword and add semicolon to every end of line
    create_table_str=`echo "$create_table_str" | sed "s/ data / \\\`data\\\` /g"`
    create_table_str=`echo "$create_table_str" | sed "s/ date / \\\`date\\\` /g"`
    create_table_str=`echo "$create_table_str" | sed "s/ group / \\\`group\\\` /g"`

    # hive to impala create sql script
    create_table_str=`echo "$create_table_str" | sed "s/CREATE EXTERNAL TABLE/CREATE TABLE/g"`
    create_table_str=`echo "$create_table_str" | sed "s/CREATE TABLE $source_db_name/CREATE TABLE $target_db_name/g"`
    create_table_str=`echo "$create_table_str" | sed "s/COMMENT 'from deserializer'/NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION/g"`
    create_table_str=`echo "$create_table_str" | sed "s/WITH SERDEPROPERTIES.*/$kudu_table_suffix/g"`

    # echo "[test] create table str: ${create_table_str}"
    create_table_ret=`impala-shell -i $impala_host:$impala_port -d $source_db_name -q "${create_table_str}" 2>/dev/null`
    log_info "[transform and execute] create table ret: ${create_table_ret}"
}

main
