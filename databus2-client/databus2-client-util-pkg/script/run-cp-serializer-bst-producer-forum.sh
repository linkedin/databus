#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.forum.CommentWithDiscussionDTO,com.linkedin.events.forum.DiscussionDTO  -C $script_dir/../conf/cp3-bootstrap-producer-forum.properties -f cp3 $*
