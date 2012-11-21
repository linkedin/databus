#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S  com.linkedin.events.espresso.ucp.UcpApp,com.linkedin.events.espresso.ucp.UcpAppFeed,com.linkedin.events.espresso.ucp.UcpObjectSchema,com.linkedin.events.espresso.ucp.UcpSequence,com.linkedin.events.espresso.ucp.UcpSharedFeed,com.linkedin.events.espresso.ucp.UcpTranslatableContent,com.linkedin.events.espresso.ucp.UcpVerbSchema -C $script_dir/../conf/cp3-bootstrap-producer-espresso-ucp.properties -f cp3 $*
