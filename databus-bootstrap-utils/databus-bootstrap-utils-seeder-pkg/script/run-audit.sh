#!/bin/bash

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

echo "Using classpath: $cp"

java -cp ${cp} com.linkedin.databus.bootstrap.utils.BootstrapAuditMain $*
