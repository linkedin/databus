#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

java -cp ${cp} com.linkedin.databus.bootstrap.utils.BootstrapDBReader $*
