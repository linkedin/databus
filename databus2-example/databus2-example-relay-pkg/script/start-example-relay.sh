#!/bin/bash

cd `dirname $0`/..


source_name=$1
if [ -z "${source_name}" ] ; then
  echo "USAGE: $0 source_name [relay_args]"
  exit 1 
fi

shift

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/setup-relay.inc

cli_overrides=

# DEFAULT VALUES
relay_type=default
jvm_gc_log=${logs_dir}/gc.log
db_relay_config=
#overwriting conf_dir for this package, because PersonalRelayServer uses config
conf_dir="${root_dir}/config"

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

log4j_file_option="-l ${conf_dir}/relay_log4j.properties"
config_file_option="-p ${conf_dir}/relay_${source_name}.properties"

java_arg_line="${config_file_option} ${log4j_file_option}"

if [ ! -z "$cli_overrides" ] ; then
   cli_overrides="-c '$cli_overrides'"
fi

case "${source_name}" in 
  "person" ) main_class=com.linkedin.databus.relay.example.PersonRelayServer ;;
esac

cmdline="java -cp ${cp} ${jvm_arg_line} ${main_class} ${java_arg_line} $cli_overrides $*"
echo $cmdline
$cmdline 2>&1 > ${relay_out_file} &
echo $! > ${relay_pid_file}
