#!/bin/bash
set -euxo pipefail

## get input to search table name. If input table name is empty, will use table name defined in env.sh
source_db_name="test"
target_db_name="stream"
if [ $# -eq 2 ] && [ -n $2 ]; then
	source_db_name=$1
    target_db_name=$2
fi

. ./env.sh
. ./log.sh

mkdir -p $impala_table_sql_store_folder
#echo "create database if not exists $source_db_name" > $impala_transform_table_sql_file_path
echo "" > $impala_transform_table_sql_file_path

## impala env: produce
eval $(get_impala_env_info_by_envname "produce")
impala_host=$tmp_impala_host
impala_port=$tmp_impala_port

main() {
    # get all table name in specify db name
    impala_tables_str=`impala-shell -i ${impala_host}:${impala_port} -d $source_db_name -q 'show tables'`
    log_debug "get all table ret: $impala_tables_str"
    impala_table_arr=($(echo $impala_tables_str | tr "," "\n"))
    for table_name in ${impala_table_arr[@]}
    do
        log_debug "current table name: $table_name"
        if [[ "$table_name" =~ $impala_table_filter ]]; then
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
            log_debug "current create table sql: $create_table_str"
            echo "" >> $impala_transform_table_sql_file_path
            echo "$create_table_str" >> $impala_transform_table_sql_file_path
        fi
    done

    # At last we need to replace some keyword and add semicolon to every end of line
    sed -i "s/ data / \`data\` /g" $impala_transform_table_sql_file_path
    sed -i "s/ date / \`date\` /g" $impala_transform_table_sql_file_path
    # sed -i "s/)\n/);\n/g" $impala_transform_table_sql_file_path

    # hive to impala create sql script
    sed -i "s/CREATE EXTERNAL TABLE/CREATE TABLE/g" $impala_transform_table_sql_file_path
    sed -i "s/CREATE TABLE $source_db_name/CREATE TABLE $target_db_name/g" $impala_transform_table_sql_file_path
    sed -i "s/COMMENT 'from deserializer'/NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION/g" $impala_transform_table_sql_file_path
    sed -i "s/WITH SERDEPROPERTIES.*/$kudu_table_suffix/g" $impala_transform_table_sql_file_path
}

main
