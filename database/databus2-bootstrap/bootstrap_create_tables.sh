#!/bin/bash

script_dir=`dirname $0`

mysql -ubootstrap -pbootstrap < $script_dir/createSchema/schema/cdsddl.tab

