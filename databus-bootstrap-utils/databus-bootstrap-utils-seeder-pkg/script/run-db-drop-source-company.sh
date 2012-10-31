#!/bin/bash

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-drop-source.sh -s 301 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 302 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 303 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 304 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 305 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 306 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 307 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 308 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 309 -p ${conf_dir}/bootstrap-drop-source.properties
