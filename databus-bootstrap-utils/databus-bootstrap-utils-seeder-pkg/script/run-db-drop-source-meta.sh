#!/bin/bash

table_name=$1

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-drop-source.sh -s ${table_name} -p ${conf_dir}/bootstrap-drop-source.properties
