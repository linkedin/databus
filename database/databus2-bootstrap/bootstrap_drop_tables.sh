#!/bin/bash

script_dir=`dirname $0`

$script_dir/createSchema/dropSchema.sh bootstrap/bootstrap@localhost
