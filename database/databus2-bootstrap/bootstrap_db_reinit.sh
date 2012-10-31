#!/bin/bash

script_dir=`dirname $0`
$script_dir/createUser/cr_databus2.sh
$script_dir/bootstrap_drop_tables.sh
$script_dir/bootstrap_create_tables.sh
