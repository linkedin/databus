#!/bin/bash
#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

script_dir=`dirname $0`
source $script_dir/setup.inc
source $script_dir/setup-bst-server.inc

if [ -f ${pid_file} ] ; then
  kill `cat ${pid_file}`
else
  echo "$0: unable to find PID file ${pid_file}"
fi
