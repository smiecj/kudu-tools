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

mkdir -p $impala_table_sql_store_folder
echo "create database if not exists $input_db_name\n" > $impala_create_table_sql_file_path
rm -f $impala_create_table_sql_file_path

main() {
    # get all table name in specify db name
    impala_tables_str=`impala-shell -i ${impala_host}:${impala_port} -d $input_db_name -q 'show tables'`
    log_debug "get all table ret: $impala_tables_str"
    # IFS='\n' read -r -a impala_table_arr <<< $impala_tables_str
    impala_table_arr=($(echo $impala_tables_str | tr "," "\n"))
    for table_name in ${impala_table_arr[@]}
    do
        log_debug "current table name: $table_name"
        if [[ "$table_name" =~ $impala_table_filter ]]; then
            table_name=`echo $table_name | tr -d " " | tr -d "|"`
            log_debug "after filter table name: $table_name"

            # get all table create sql, and output to log file
            create_table_str=`impala-shell -i $impala_host:$impala_port -d $input_db_name -q "show create table $table_name" 2>/dev/null`
            create_table_str=`echo $create_table_str | tr -d "|" | tr -d "-" | tr -d "+" | tr -d "\n" | sed "s/result   *//g"`
            log_debug "current create table sql: $create_table_str"
            echo $create_table_str >> $impala_create_table_sql_file_path
        fi
    done

    # At last we need to replace some keyword and add semicolon to every end of line
    sed -i "s/ data / `data` /g" $impala_create_table_sql_file_path
    sed -i "s/\n/;\n/g" $impala_create_table_sql_file_path
}

main
