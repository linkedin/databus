#!/bin/bash
#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

cd `dirname $0`/..


source_name=$1
if [ -z "${source_name}" ] ; then
  echo "USAGE: $0 source_name [client_args]"
  exit 1 
fi

shift

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/setup-client.inc

cli_overrides=

# DEFAULT VALUES
client_type=default
jvm_gc_log=${logs_dir}/client-gc.log

# JVM ARGUMENTS
jvm_direct_memory_size=40g
jvm_direct_memory="-XX:MaxDirectMemorySize=${jvm_direct_memory_size}"
jvm_min_heap_size="1024m"
jvm_min_heap="-Xms${jvm_min_heap_size}"
jvm_max_heap_size="1024m"
jvm_max_heap="-Xmx${jvm_max_heap_size}"

jvm_gc_options="-XX:NewSize=512m -XX:MaxNewSize=512m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:SurvivorRatio=6 -XX:MaxTenuringThreshold=7"
jvm_gc_log_option="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution "
if [ ! -z "${jvm_gc_log}" ] ; then
  jvm_gc_log_option="${jvm_gc_log_option} -Xloggc:${jvm_gc_log}"
fi

jvm_arg_line="-d64 ${jvm_direct_memory} ${jvm_min_heap} ${jvm_max_heap} ${jvm_gc_options} ${jvm_gc_log_option} -ea"

log4j_file_option="-l ${conf_dir}/client_log4j.properties"
config_file_option="-p ${conf_dir}/client_${source_name}.properties"

java_arg_line="${config_file_option} ${log4j_file_option}"

if [ ! -z "$cli_overrides" ] ; then
   cli_overrides="-c '$cli_overrides'"
fi

case "${source_name}" in 
  "person" ) main_class=com.linkedin.databus.client.example.PersonClientMain ;;
esac

cmdline="java -cp ${cp} ${jvm_arg_line} ${main_class} ${java_arg_line} $cli_overrides $*"
echo $cmdline
$cmdline 2>&1 > ${client_out_file} &
echo $! > ${client_pid_file}
