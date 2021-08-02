#!/bin/bash
. ./env.sh

. ./log.sh

pushd $home_path

rm -rf $remote_replica_ret_folder
rm -rf $log_folder
rm -rf $remote_replica_ret_home

popd