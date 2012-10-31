#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.espresso.ucp.UcpxComment,com.linkedin.events.espresso.ucp.UcpxCommentThread,com.linkedin.events.espresso.ucp.UcpxLyke,com.linkedin.events.espresso.ucp.UcpxObjectLikeSummary,com.linkedin.events.espresso.ucp.UcpxRelevancePreferences,com.linkedin.events.espresso.ucp.UcpxComment2,com.linkedin.events.espresso.ucp.UcpxCommentThread2,com.linkedin.events.espresso.ucp.UcpxContentEvent -C $script_dir/../conf/cp3-bootstrap-producer-espresso-ucpx.properties -f cp3 $*
