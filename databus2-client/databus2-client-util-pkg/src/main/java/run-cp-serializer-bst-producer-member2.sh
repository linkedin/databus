#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.member2.privacy.PrivacySettings,com.linkedin.events.member2.account.MemberAccount -C $script_dir/../conf/cp3-bootstrap-producer-member2.properties -f cp3 $*
