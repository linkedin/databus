#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.news.TaggedSlices -C $script_dir/../conf/cp3-bootstrap-producer-news.properties -f cp3 $*
