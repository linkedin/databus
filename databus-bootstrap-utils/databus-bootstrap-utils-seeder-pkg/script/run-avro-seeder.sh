#!/bin/bash

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

cp="."
for f in ${lib_dir}/*.jar ; do
  cp="${cp}:${f}"
done

echo "Using classpath: $cp"

if [ -z "$JVM_ARGS" ] ; then
  JVM_ARGS="-Xms1g -Xmx2g"
fi

java $JVM_ARGS -cp ${cp} com.linkedin.databus.bootstrap.utils.BootstrapAvroFileSeederMain $*
