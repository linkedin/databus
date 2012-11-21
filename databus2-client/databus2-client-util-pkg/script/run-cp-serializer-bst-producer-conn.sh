#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.conns.Connections,com.linkedin.events.conns.ConnectionsCnt -C $script_dir/../conf/cp3-bootstrap-producer-conn.properties -f cp3 $*
