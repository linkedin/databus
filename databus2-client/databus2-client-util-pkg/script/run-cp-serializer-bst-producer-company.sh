#!/bin/bash

script_dir=`dirname $0`
$script_dir/run-cp-serializer.sh -S com.linkedin.events.company.Companies,com.linkedin.events.company.CompanyNameSearch,com.linkedin.events.company.Products,com.linkedin.events.company.Profiles,com.linkedin.events.company.Reviews,com.linkedin.events.company.EmailDomains,com.linkedin.events.company.Flagging  -C $script_dir/../conf/cp3-bootstrap-producer-company.properties -f cp3 $*

