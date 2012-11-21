#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.cappr.ProspectHistory,com.linkedin.events.cappr.ProspectDocuments -C $script_dir/../conf/cp3-bootstrap-producer-cappr.properties -f cp3 $*

