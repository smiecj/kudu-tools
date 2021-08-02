# check master node count
get_kudu_tserver() {
	## get all table server by execute command in one kudu master node
	ret=`ssh $kudu_execute_host -p $ssh_port "kudu tserver list $1 -format=json"`
	json_parse_ret=$(util_parse_json_array_return_joinstr "$ret" 'rpc-addresses')
	echo "kudu_tservers=$json_parse_ret"
}

# join kudu master node
join_master_node() {
	tmp_kudu_master_str=""
	for host in ${kudu_hosts[@]}
	do
		if [[ $tmp_kudu_master_str != "" ]]; then
			tmp_kudu_master_str=`echo $tmp_kudu_master_str,$host:$kudu_port`
		else
			tmp_kudu_master_str=`echo $host:$kudu_port`
		fi
	done
	echo "kudu_nodes_str=$tmp_kudu_master_str"
}

# get kudu data recent update time by tablet metadata's timestamp
## notice the file maybe not exist
get_kudu_tablet_timestamp() {
	local teblet_id=$1
	local tablet_server=$2
	local metadata_filename=$kudu_wal_dir/tablet-meta/$tablet_id
	
	local timestamp=`sh $kudu_tserver_host -p $ssh_port "date '+%Y-%m-%d %H:%M:%S' -r $metadata_filename 2>/dev/null"`
	echo $timestamp
}

# get kudu tablet list string(join by ',') by table condition (table name or table id)
## kudu_tserver_host: kudu table server host, used for ssh and execute kudu command
## kudu_fs_list_cond: kudu fs list condition, eg: -table_id="table id"
get_kudu_tablet_ids() {
	local kudu_tserver_host=$1
	local kudu_fs_list_cond=$2

	local fs_ret=`ssh $kudu_tserver_host -p $ssh_port "kudu fs list $kudu_fs_list_cond -fs_wal_dir=$kudu_wal_dir -fs_data_dirs=$kudu_data_dir -format=json 2>/dev/null"`
	local fs_ret_code=$?
	local tablet_ids=$(util_parse_json_array_return_joinstr "$fs_ret" 'tablet-id')
	echo $tablet_ids
	return $fs_ret_code
}

# write kudu tablet size to remote replica ret folder
## kudu_tserver_host: kudu table server host, used for ssh and execute kudu command
write_kudu_table_size() {
	local kudu_tserver_host=$1

	local data_size_ret=`ssh $kudu_execute_host -p $ssh_port "kudu remote_replica list $kudu_tserver_host:$kudu_table_server_port 2>/dev/null"`
	echo "$data_size_ret" > $remote_replica_ret_folder/$kudu_tserver_host
}

# get table recent update date
## kudu_tablet_id_join_str: kudu tablet ids, format: id1,id2...
## kudu_table_server_host: kudu table server host
get_kudu_tablet_recent_update_date() {
	local kudu_tablet_id_join_str=$1
	local kudu_table_server_host=$2

	if [ -z $kudu_tablet_id_join_str ] || [ -z $kudu_table_server_host ]; then
		echo ""
		return 1
	fi

	tablet_id_arr=($(echo $kudu_tablet_id_join_str | tr "," "\n" | sort | uniq))

	local recent_date=""
	for current_tablet_id in "${tablet_id_arr[@]}"
	do
		local current_date=`ssh $kudu_table_server_host -p $ssh_port "find $kudu_wal_dir -name '$current_tablet_id*' | xargs -I {} date '+%Y-%m-%d' -r {} | sort -r | sed -n '1p' 2>/dev/null"`
		if [ "$current_date" \> "$recent_date" ]; then
			recent_date=$current_date
		fi
	done
	echo $recent_date
}

# util: parse json array element and join by ','
## input: [{"test": "1"}, {"test": "2"}] 'test' return: 1,2
### todo: use more convinent tool: jq
util_parse_json_array_return_joinstr() {
	input_str=$1
	field_name=$2
	local join_str="`echo "import sys, json; jsonArr = json.loads('$input_str'); strArr = [jsonObj['$field_name'] for jsonObj in jsonArr]; \
		join_str = ','; ret_str = join_str.join(strArr); print ret_str;" | python`"
	echo $join_str
	return $?
}

# util: get kudu fs list condition by search table name
## <= kudu 1.7 must use kudu table id, >= kudu 1.8 can directly use kudu table name
util_get_kudu_fs_list_cond() {
	kudu_table_name=$1
	for host in ${kudu_hosts[@]}
	do
		local parse_id_ret="`python kudu_table_id_parser.py --kudu_host $host --kudu_port $kudu_webui_port --table_name $kudu_table_name`"
		### if current kudu node is not master node, will not able to get table id, skip it
		if [[ $parse_id_ret =~ ^error.* ]]; then
			continue
		else
			echo "-table_id=$parse_id_ret"
			return
		fi
	done
	echo ""
	return
}