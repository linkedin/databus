#!/bin/bash

table_name=$1
table_id=$2

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-add-source.sh -s ${table_name} -i ${table_id} -p ${conf_dir}/bootstrap-add-source.properties
