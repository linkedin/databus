#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc

# DEFAULT VALUES
jvm_gc_log=${logs_dir}/gc.log

# JVM ARGUMENTS
jvm_direct_memory_size=2048m
jvm_direct_memory="-XX:MaxDirectMemorySize=${jvm_direct_memory_size}"
jvm_min_heap_size="100m"
jvm_min_heap="-Xms${jvm_min_heap_size}"
jvm_max_heap_size="512m"
jvm_max_heap="-Xmx${jvm_max_heap_size}"

jvm_gc_log_option=
if [ ! -z "${jvm_gc_log}" ] ; then
  jvm_gc_log_option="-Xloggc:${jvm_gc_log}"
fi

jvm_arg_line="-d64 ${jvm_direct_memory} ${jvm_min_heap} ${jvm_max_heap} ${jvm_gc_log_option} -ea"

main_class=com.linkedin.databus3.espresso.client.test.EspressoTestDatabusClient

log4j_file_option="-l ${conf_dir}/espresso_client_log4j.properties"
config_file_option="-p ${conf_dir}/espresso_client.properties"
java_arg_line="${config_file_option} ${log4j_file_option}"

cmdline="java -cp ${cp} ${jvm_arg_line} ${main_class} ${java_arg_line} $*"
echo $cmdline

$cmdline 2>&1 > ${espresso_client_out_file} &
echo $! > ${espresso_client_pid_file}