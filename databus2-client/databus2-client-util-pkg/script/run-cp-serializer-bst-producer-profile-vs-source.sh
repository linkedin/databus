#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S  com.linkedin.events.profile.flexstore.PROFILEVSSOURCE -C $script_dir/../conf/cp3-bootstrap-producer-profile.properties -f cp3 $*
