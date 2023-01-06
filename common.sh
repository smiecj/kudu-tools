get_source_table_name() {
    ## check whether this table is in source db, if not, upsert will not execute
    local source_db_name=$1
    local to_find_table_name=$2
    local impala_host=$3
    local impala_port=$4
    local ret_table_name=$to_find_table_name
    show_table_ret=`impala-shell -i $impala_host:$impala_port -d $source_db_name -q "show create table $ret_table_name" 2>/dev/null || true`
    if [ -z "$show_table_ret" ]; then
        ## special logic: check again with short prefix name
        ret_table_name=`echo $ret_table_name | sed 's/stg_stream/stg/g'`
        show_table_ret=`impala-shell -i $impala_host:$impala_port -d $source_db_name -q "show create table $ret_table_name" 2>/dev/null || true`
        if [ -z "$show_table_ret" ]; then
            echo ""
            return 0
        fi
    fi
    echo "$ret_table_name"
}

filter_table() {
    local input_table_name=$1
    if [ -n "${actual_table_name}" ]; then
        if [ "${actual_table_name}" == "${input_table_name}" ]; then
            echo "${input_table_name}"
        else
            echo ""
        fi
        return
    fi
    if [[ "$table_name" =~ $impala_table_filter ]]; then
        echo "${input_table_name}"
        return
    fi
    echo ""
}