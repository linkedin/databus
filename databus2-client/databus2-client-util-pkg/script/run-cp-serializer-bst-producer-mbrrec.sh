#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.mbrrec.MemberReferences,com.linkedin.events.mbrrec.Recommendations -C $script_dir/../conf/cp3-bootstrap-producer-mbrrec.properties -f cp3 $*
