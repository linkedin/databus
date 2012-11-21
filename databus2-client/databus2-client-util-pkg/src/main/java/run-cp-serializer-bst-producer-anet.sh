#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.anet.AnetMailDomain,com.linkedin.events.anet.AnetMembers,com.linkedin.events.anet.Anets,com.linkedin.events.anet.RestrictedMailDomain -C $script_dir/../conf/cp3-bootstrap-producer-anet.properties -f cp3 $*
