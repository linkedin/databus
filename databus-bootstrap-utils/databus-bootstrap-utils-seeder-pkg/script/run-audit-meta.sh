#!/bin/bash

fabric=$1
source_name=$2
validation_type=$3

if [ -z "$fabric" ] ; then 
  echo USAGE: $0 fabric source_name validation_type
  exit 1
fi

if [ -z "$source_name" ] ; then 
  echo USAGE: $0 fabric source_name validation_type
  exit 1
fi

if [ -z "$validation_type" ] ; then 
	validation_type="normal";
fi

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

if [ ! -f ${sources_conf} ] ; then
    echo ERROR: Unable to find sources configuration for fabric ${fabric}: ${source_conf}
    exit 2
fi

seeder_log_file=${logs_dir}/${source_name}_${fabric}_audit.log
$script_dir/run-audit.sh -c ${sources_conf} -p ${conf_dir}/bootstrap-seeder-config.properties -v ${validation_type} | tee -a ${seeder_log_file}
 
