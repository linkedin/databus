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

script_dir=`cd $(dirname $0) && pwd`
source $script_dir/setup.inc
source $script_dir/setup-bst-producer.inc

# DEFAULT VALUES

# JVM ARGUMENTS

# Memory configuration
jvm_mem_options=
if [ ! -z "${heap_size}" ] ; then
	jvm_mem_options="${jvm_mem_options} -Xms${heap_size}"
fi
if [ ! -z "${max_heap_size}" ] ; then
	jvm_mem_options="${jvm_mem_options} -Xmx${max_heap_size}"
fi
if [ ! -z "${new_size}" ] ; then
	jvm_mem_options="${jvm_mem_options} -XX:NewSize=${heap_size}"
fi
if [ ! -z "${max_new_size}" ] ; then
	jvm_mem_options="${jvm_mem_options} -XX:MaxNewSize=${max_heap_size}"
fi
if [ ! -z "${direct_mem_size}" ] ; then
	jvm_mem_options="${jvm_mem_options} -XX:MaxDirectMemorySize=${direct_mem_size}"
fi

# GC configuration
jvm_gc_options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly"
jvm_gc_log_option="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution "
jvm_gc_log_option="${jvm_gc_log_option} -Xloggc:${jvm_gc_log}"


jvm_arg_line="-d64 ${jvm_mem_options} ${jvm_gc_options} ${jvm_gc_log_option} "

# CLASS ARGUMENTS

log4j_file_option="-l ${conf_dir}/databus-bst-producer_log4j.properties"
config_file_option="-p ${conf_dir}/databus-bst-producer.properties"
java_arg_line="${config_file_option} ${log4j_file_option}"

main_class=com.linkedin.databus.bootstrap.producer.DatabusBootstrapProducer

cmdline="java -cp ${cp} ${jvm_arg_line} ${main_class} ${java_arg_line} $*"
echo "Starting $cmdline" > ${out_file}
echo "------------------------------------" >> ${out_file}
(cd $script_dir/.. && $cmdline 2>&1) >> ${out_file} &
echo $! > ${pid_file}
