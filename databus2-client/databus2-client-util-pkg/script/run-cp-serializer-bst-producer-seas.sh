#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.sas.Creatives  -C $script_dir/../conf/cp3-bootstrap-producer-seas.properties -f cp3 $*
