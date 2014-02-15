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

# Usage:
#
#  dropSchema.sh [user [password]]

if [ "$#" -lt 1 ]
then
	echo "Usage: "
	echo "       $0 username/password@hostname"
	echo ""
	exit 1
fi

DB=$1

LOG_DIR=${2:-logs}

HOST=`echo $DB|awk -F'@' '{print $2}'`

USER_PASS=`echo $DB|awk -F'@' '{print $1}'`

USER=`echo $USER_PASS|awk -F'/' '{print $1}'`

PASS=`echo $USER_PASS|awk -F'/' '{print $2}'`

echo "Executing Command mysql -h$HOST -u$USER -p$PASS DROP DATABASE IF EXISTS bootstrap;"

mysql -h$HOST -u$USER -p$PASS << __EOF__
DROP DATABASE IF EXISTS bootstrap;
__EOF__
