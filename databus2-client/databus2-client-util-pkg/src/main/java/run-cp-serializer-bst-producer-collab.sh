#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.collab.TeamRelationships,com.linkedin.events.collab.Teams,com.linkedin.events.collab.MemberData -C $script_dir/../conf/cp3-bootstrap-producer-collab.properties -f cp3 $*
