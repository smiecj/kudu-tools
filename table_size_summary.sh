#!/bin/bash
#set -euxo pipefail

## get input to search table name. If input table name is empty, will use table name defined in env.sh
input_table_name=""
if [ $# -eq 1 ] && [ -n $1 ]; then
	input_table_name=$1
fi
. ./env.sh
if [ "" != "$input_table_name" ]; then
	search_table_name=$input_table_name
fi

pushd $home_path

mkdir -p $tmp_folder > /dev/null 2>&1
mkdir -p $remote_replica_ret_folder > /dev/null 2>&1

. ./log.sh
. ./kudu_table_summary.sh

main() {
	## get all kudu master node
	kudu_nodes_str=""
	eval $(join_master_node)

	## get all kudu table server node
	kudu_tservers=""
	eval $(get_kudu_tserver $kudu_nodes_str)

	## get kudu fs list condition
	kudu_fs_list_cond=$(util_get_kudu_fs_list_cond "$search_table_name")
	if [ -z $kudu_fs_list_cond ]; then
		log_error "table: $search_table_name, get table size failed, please check table name or kudu connection"
		exit
	fi

	## get table storage by log in each table server and get kudu table summary
	### update: in the loop, will not get storage, only summary table recent update date
	kudu_tserver_arr=($(echo $kudu_tservers | tr "," "\n"))
	total_table_size=0
	table_recent_update_date=""
	total_tablet_arr=()
	log_info "kudu tablet server count: ${#kudu_tserver_arr[@]}"
	log_info "kudu tablet server: ${kudu_tserver_arr[@]}"
	for tablet_server_index in "${!kudu_tserver_arr[@]}";
		kudu_tserver_address=${kudu_tserver_arr[$tablet_server_index]}
		kudu_tserver_host_and_port=($(echo $kudu_tserver_address | tr ":" "\n"))
		kudu_tserver_host=${kudu_tserver_host_and_port[0]}
        
		## get table's tablet id
        log_info "tablet host num: $tablet_server_index, table name: $search_table_name get fs list from $kudu_tserver_host ..."
		tablet_ids=$(get_kudu_tablet_ids $kudu_tserver_host $kudu_fs_list_cond)

		get_tablet_ids_ret=$?
		if [ ! 0 -eq $get_tablet_ids_ret ]; then
			log_warn "tablet host num: $tablet_server_index, table name: $search_table_name get fs list from $kudu_tserver_host failed"
			continue
		fi

        ## if kudu tablet id get from current table server is empty, skip
		tablet_id_arr=($(echo $tablet_ids | tr "," "\n"))
		if [ ${#tablet_id_arr[@]} -eq 0 ]; then
			log_info "tablet host num: $tablet_server_index, get fs list from $kudu_tserver_host is empty"
			continue
		fi

		## tablet id de-duplicated
		tablet_id_arr=($(echo ${tablet_id_arr[*]} | sed 's/ /\n/g' |sort |uniq))
		
		if [ ${#total_tablet_arr[@]} -eq 0 ]; then
			total_tablet_arr=${tablet_id_arr}
		else
			total_tablet_arr=(${total_tablet_arr[@]} ${tablet_id_arr[@]})
		fi

		log_info "table name: $search_table_name, kudu host: $kudu_tserver_host, tablet count: ${#tablet_id_arr[@]}"
		log_debug "table name: $search_table_name, kudu host: $kudu_tserver_host, tablet: ${tablet_id_arr[*]}"

		## get tablet recent update date
		current_table_recent_update_date=$(get_kudu_tablet_recent_update_date $tablet_ids $kudu_tserver_host)
		log_debug "table name: $search_table_name, recent update date: $current_table_recent_update_date, tablet: ${tablet_id_arr[*]}"
		if [ "$current_table_recent_update_date" \> "$table_recent_update_date" ]; then
			table_recent_update_date=$current_table_recent_update_date
		fi

		write_kudu_table_size $kudu_tserver_host
	done

	## get tablet table size from all tablet 
	total_table_size=`python kudu_table_remote_parser.py --table_name "$search_table_name" --remote_replica_ret_path "$remote_replica_ret_folder"`

	## print main information
	### tablet number
	### total size
	### recent update date
	total_tablet_arr=($(echo ${total_tablet_arr[*]} | sed 's/ /\n/g' |sort |uniq))
	total_table_size="`echo "import sys; a = float('$total_table_size'); \
		print '%.4f' % (a / 1024);" | python`"
	log_info "table: $search_table_name, tablet count: ${#total_tablet_arr[@]}, table size: $total_table_size MB, recent update date: $table_recent_update_date"
}

test_get_kudu_fs_list_cond() {
	cond=$(util_get_kudu_fs_list_cond "$search_table_name")
	echo "cond = $cond"
}

test_sum_table_size() {
	totalSize=$(util_sum_table_size "1.1111,2.22222")
	echo $totalSize
}

#test_unit_transform
#test_get_kudu_fs_list_cond
#test_sum_table_size
main

popd