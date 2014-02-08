#!/bin/bash -vx
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

#Usage: $0 [username] [password] [database] [hostname]

script_dir=`dirname $0`
hostname=`hostname`

if [ "$1" == "" ] ; then
   user=bootstrap
else
   user=$1
fi

if [ "$2" == "" ] ; then
   pass=bootstrap
else
   pass=$2
fi

if [ "$3" == "" ] ; then
   db=bootstrap
else
   db=$3
fi

mysql_root_pwd=
if [ ! -z "${MYSQL_ROOT_PWD}" ] ; then
   mysql_root_pwd="-p${MYSQL_ROOT_PWD}"
fi

MYSQL="mysql -uroot ${mysql_root_pwd}"

${MYSQL} -e "create database if not exists $db;"

if [ "$4" == "" ] ; then
   ${MYSQL} -e "grant all privileges on $db.* to $user@'localhost' identified by '$pass';"
else
   host=$4
   ${MYSQL} -e "grant all privileges on $db.* to $user@'$host' identified by '$pass';"
fi

${MYSQL} -e "grant all privileges on $db.* to $user@'$hostname' identified by '$pass';"
${MYSQL} -e "grant all privileges on $db.* to $user@'127.0.0.1' identified by '$pass';"
${MYSQL} -e "grant all privileges on $db.* to $user@'%.linkedin.com' identified by '$pass';"
${MYSQL} -e "grant all privileges on $db.* to $user@'%.linkedin.biz' identified by '$pass';"
${MYSQL} -e "grant all privileges on $db.* to $user@'172.16.%' identified by '$pass';"
${MYSQL} -e "grant all privileges on $db.* to $user@'rdb%.prod.linkedin.com' identified by '$pass';"

${MYSQL} -e "show errors;"
${MYSQL} -e "show warnings;"
