#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc

java -cp ${cp} com.linkedin.databus.bootstrap.utils.BootstrapDBCleanerMain $*
