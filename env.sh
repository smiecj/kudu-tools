## produce env
produce_impala_host=produce_impala_host
produce_impala_port=21000

## test env
test_impala_host=test_impala_host
test_impala_port=21000

## get specific env's information
### output: 
get_impala_env_info_by_envname() {
     local env_name=$1
     case "$env_name" in
     "produce")
          echo "tmp_impala_host=$produce_impala_host"
          echo "tmp_impala_port=$produce_impala_port"
          ;;
     "test")
          echo "tmp_impala_host=$test_impala_host"
          echo "tmp_impala_port=$test_impala_port"
          ;;
     esac
}

impala_table_filter=".*stg*"
impala_table_sql_store_folder=sqls
impala_create_table_sql_file_name=create_table.sql
impala_transform_table_sql_file_name=transform_table.sql
impala_create_table_sql_file_path=$impala_table_sql_store_folder/$impala_create_table_sql_file_name
impala_transform_table_sql_file_path=$impala_table_sql_store_folder/$impala_transform_table_sql_file_name

#backup_before_date=2021-11-18
backup_before_date=""
#backup_table_allowlist="(backup_allow_task_name)"
backup_table_allowlist=""

sync_task_tables_file=datalink_task.txt

kudu_master_hosts=("kudu_master_node1" "kudu_master_node2" "kudu_master_node3")
for index in "${!kudu_master_hosts[@]}"
do
     if [ $index -ge 1 ]; then
          kudu_master_hosts_str="$kudu_master_hosts_str,${kudu_master_hosts[$index]}"
     else
          kudu_master_hosts_str="${kudu_master_hosts[$index]}"
     fi
done
kudu_master_hosts_str="'$kudu_master_hosts_str'"
kudu_table_suffix="COMMENT '' STORED AS KUDU TBLPROPERTIES ('kudu.master_addresses'=$kudu_master_hosts_str);"
kudu_port=7051
kudu_webui_port=8051
kudu_table_server_port=7050
kudu_execute_host=${kudu_master_hosts[0]}
kudu_data_dir=/opt/kudu/tserver/data
kudu_wal_dir=/opt/kudu/tserver/wal
ssh_port=22
tmp_folder=tmp
remote_replica_ret_home="./remote_ret"
remote_replica_ret_folder="$remote_replica_ret_home/$$"
search_table_name="impala::default.table_name"
