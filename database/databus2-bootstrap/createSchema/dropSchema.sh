#!/bin/bash

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
