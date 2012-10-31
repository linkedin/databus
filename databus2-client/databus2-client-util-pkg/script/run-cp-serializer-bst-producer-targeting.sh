#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.targeting.LixTarget -C $script_dir/../conf/cp3-bootstrap-producer-targeting.properties -f cp3 $*
