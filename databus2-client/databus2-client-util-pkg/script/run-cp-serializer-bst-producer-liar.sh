#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.liar.memberrelay.LiarMemberRelay,com.linkedin.events.liar.jobrelay.LiarJobRelay -C $script_dir/../conf/cp3-bootstrap-producer-liar.properties -f cp3 $*