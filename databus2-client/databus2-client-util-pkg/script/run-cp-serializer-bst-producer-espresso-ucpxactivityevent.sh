#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.espresso.ucp.UcpxActivityEvent -C $script_dir/../conf/cp3-bootstrap-producer-espresso-ucpxactivityevent.properties -f cp3 $*
