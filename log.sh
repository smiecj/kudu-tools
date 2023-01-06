# log util
## log file: log_name_id.log

LOG=TRUE

LOG_LEVEL_DEBUG=3
LOG_LEVEL_INFO=2
LOG_LEVEL_WARN=1
LOG_LEVEL_ERROR=0

log_level=$LOG_LEVEL_DEBUG
log_name="kudu_summary"
log_folder=logs

mkdir -p $log_folder 2>/dev/null

log_id=1
## get log id: recent biggest log id plus one
find_id_ret=`find ./$log_folder -maxdepth 1 -printf "%f\n" | grep -E "$log_name.*.log" | awk '{n=split($0,a,"[_.]");print a[n-1]}' | sort -nr | sed -n '1p' || true`
if [ -n $find_id_ret ]; then
    log_id=$((find_id_ret + 1))
fi
log_file=$log_folder/"$log_name"_"$log_id".log

if [[ "FALSE" == "${LOG}" ]]; then
    log_file=/dev/null
fi

log_debug() {
    if [ $log_level -ge $LOG_LEVEL_DEBUG ]; then
        echo `date "+%Y-%m-%d %H:%M:%S [DEBUG] "`"$*" >> $log_file
    fi
}

log_info() {
    if [ $log_level -ge $LOG_LEVEL_INFO ]; then
        echo `date "+%Y-%m-%d %H:%M:%S [INFO] "`"$*" >> $log_file
    fi
}

log_warn() {
    if [ $log_level -ge $LOG_LEVEL_WARN ]; then
        echo `date "+%Y-%m-%d %H:%M:%S [WARN] "`"$*" >> $log_file
    fi
}

log_error() {
    if [ $log_level -ge $LOG_LEVEL_ERROR ]; then
        echo `date "+%Y-%m-%d %H:%M:%S [ERROR] "`"$*" >> $log_file
    fi
}