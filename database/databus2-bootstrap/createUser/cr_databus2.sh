#!/bin/bash -vx

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

mysql -uroot -e "create database if not exists $db;"

if [ "$4" == "" ] ; then
   mysql -uroot -e "grant all privileges on $db.* to $user@'localhost' identified by '$pass';"
else
   host=$4
   mysql -uroot -e "grant all privileges on $db.* to $user@'$host' identified by '$pass';"
fi

mysql -uroot -e "grant all privileges on $db.* to $user@'$hostname' identified by '$pass';"
mysql -uroot -e "grant all privileges on $db.* to $user@'127.0.0.1' identified by '$pass';"
mysql -uroot -e "grant all privileges on $db.* to $user@'%.linkedin.com' identified by '$pass';"
mysql -uroot -e "grant all privileges on $db.* to $user@'%.linkedin.biz' identified by '$pass';"
mysql -uroot -e "grant all privileges on $db.* to $user@'172.16.%' identified by '$pass';"
mysql -uroot -e "grant all privileges on $db.* to $user@'rdb%.prod.linkedin.com' identified by '$pass';"

mysql -uroot -e "show errors;"
mysql -uroot -e "show warnings;"
