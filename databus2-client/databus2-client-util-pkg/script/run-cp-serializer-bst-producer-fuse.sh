#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.fuse.MemberRestriction -C $script_dir/../conf/cp3-bootstrap-producer-fuse.properties -f cp3 $*
