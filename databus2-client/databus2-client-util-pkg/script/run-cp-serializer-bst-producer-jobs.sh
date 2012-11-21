#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.jobs.Jobs,com.linkedin.events.jobs.JobMatchUpdate -C $script_dir/../conf/cp3-bootstrap-producer-jobs.properties -f cp3 $*
