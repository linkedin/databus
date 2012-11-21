#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.bizfollow.bizfollow.BizFollow  $*  -C $script_dir/../conf/cp3-bootstrap-producer-bizfollow.properties -f cp3