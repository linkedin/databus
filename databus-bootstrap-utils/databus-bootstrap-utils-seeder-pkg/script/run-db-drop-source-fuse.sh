#!/bin/bash

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-drop-source.sh -s 1001 -p ${conf_dir}/bootstrap-drop-source.properties
#!/bin/bash

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-drop-source.sh -s 1001 -p ${conf_dir}/bootstrap-drop-source.properties
