#!/bin/bash
# Usage:
#
#  createSchema.sh [user [password | --nopass [logdir]]]

if [ "$#" -lt 1 ]
then
	echo "Usage: "
	echo "       $0 username/password@hostname [logdir]"
	echo ""
	exit 1
fi

DB=$1

LOG_DIR=${2:-logs}

HOST=`echo $DB|awk -F'@' '{print $2}'`

USER_PASS=`echo $DB|awk -F'@' '{print $1}'`

USER=`echo $USER_PASS|awk -F'/' '{print $1}'`

PASS=`echo $USER_PASS|awk -F'/' '{print $2}'`

mkdir -p $LOG_DIR

mysql -h$HOST -u$USER -p$PASS < schema/cdsddl.tab > $LOG_DIR/schema.log
