#!/bin/bash

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-drop-source.sh -s ${forum_DiscussionDTO_srcid} -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s ${forum_CommentWithDiscussionDTO_srcid} -p ${conf_dir}/bootstrap-drop-source.properties