#!/bin/bash

script_dir=`dirname $0`
source $script_dir/setup.inc

main_class=com.linkedin.databus.util.SchemaGeneratorMain

java -cp ${cp} ${main_class} $*