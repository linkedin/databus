#!/bin/bash

fabric=$1
source_name=$2

if [ -z "$fabric" ] ; then 
  echo USAGE: $0 fabric source_name
  exit 1
fi

if [ -z "$source_name" ] ; then 
  echo USAGE: $0 fabric source_name
  exit 1
fi

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

if [ ! -f ${sources_conf} ] ; then
    echo ERROR: Unable to find sources configuration for fabric ${fabric}: ${sources_conf}
    exit 2
fi

seeder_log_file=${logs_dir}/${source_name}_${fabric}_seeding.log
$script_dir/run-avro-seeder.sh -c ${sources_conf} -p ${conf_dir}/bootstrap-avro-seeder-config.properties | tee -a ${seeder_log_file}
 
