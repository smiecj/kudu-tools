#!/bin/bash
. ./env.sh

. ./log.sh

script_full_path=$(realpath $0)
home_path=$(dirname $script_full_path)
pushd $home_path

rm -rf $remote_replica_ret_folder
rm -rf $log_folder
rm -rf $remote_replica_ret_home

popd