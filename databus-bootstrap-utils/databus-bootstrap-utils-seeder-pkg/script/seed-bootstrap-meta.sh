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
    echo ERROR: Unable to find sources configuration for fabric ${fabric}: ${source_conf}
    exit 2
fi

if [ "${fabric}" == "dev" ] ; then
    MKDIR="mkdir -p"
else
    MKDIR="sudo -u app mkdir -p"
fi

if grep REPLACEME ${sources_conf} ; then
    echo Please make sure you have updated ${sources_conf} with the database connection string.
    exit 3
fi 


#echo "******* Cleaning up old state in the bootstrap *******"
#${script_dir}/run-db-drop-source-meta.sh | tee ${logs_dir}/db-drop-source.log | grep Exec
#if [ $? != 0 ] ; then
#    echo Failure. Please check ${logs_dir}/db-drop-source.log for more info.
#    exit 4
#fi

echo "******* Creating checkpoints directory *******"
${MKDIR} -p /export/content/data/databus2-bootstrap-producer-${source_name}/i001/checkpoints/

echo "******* Starting the seeding *******"
${script_dir}/run-seeder-meta.sh ${fabric} ${source_name}
