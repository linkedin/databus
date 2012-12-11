#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc

kill `cat ${espresso_client_pid_file}`