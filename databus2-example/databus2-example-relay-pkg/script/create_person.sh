#!/bin/bash
script_dir=`dirname $0`
#Setup this to point appropriately to MySQL instance
MYSQL='/usr/bin/mysql --protocol=tcp --port=33066'
SQL_DIR=../sql

${MYSQL} -uroot -e 'CREATE DATABASE IF NOT EXISTS or_test;';
${MYSQL} -uroot -e "CREATE USER 'or_test'@'localhost' IDENTIFIED BY 'or_test';";
${MYSQL} -uroot -e "GRANT ALL ON or_test.* TO 'or_test'@'localhost';"
${MYSQL} -uroot -e "GRANT ALL ON *.* TO 'or_test'@'localhost';"
${MYSQL} -uroot -e "GRANT ALL ON *.* TO 'or_test'@'127.0.0.1';"

${MYSQL} -uor_test -por_test -Dor_test < ${SQL_DIR}/create_person.sql
${MYSQL} -uor_test -por_test -Dor_test < ${SQL_DIR}/insert_person_test_data_1.sql
${MYSQL} -uroot -e 'RESET MASTER;'
