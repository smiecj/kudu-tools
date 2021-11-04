home_path=/home/tools/kudu-tools

impala_host="impalad_host"
impala_port="impalad_port"
impala_table_filter=".*stg*"
impala_table_sql_store_folder=sqls
impala_create_table_sql_file_name=create_table.sql
impala_create_table_sql_file_path=$impala_table_sql_store_folder/$impala_create_table_sql_file_name

kudu_hosts=("kudu_master_node1" "kudu_master_node2" "kudu_master_node3")
kudu_port=7051
kudu_webui_port=8051
kudu_table_server_port=7050
kudu_execute_host=${kudu_hosts[0]}
kudu_data_dir=/opt/kudu/tserver/data
kudu_wal_dir=/opt/kudu/tserver/wal
ssh_port=22
tmp_folder=tmp
remote_replica_ret_home="./remote_ret"
remote_replica_ret_folder="$remote_replica_ret_home/$$"
search_table_name="impala::default.table_name"
