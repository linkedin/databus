#!/bin/sh
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
 
if [ "$#" -lt 1 ]
then
        echo "Usage: $0 <username/password[@SID]>"
	exit 1
fi

DB=$1


sqlplus -S $DB << __EOF__ | sqlplus -S $DB 
SET PAGES 0 TRIMS ON ECHO OFF VERIFY OFF FEEDBACK OFF TERMOUT OFF LINESIZE 3000
SELECT 'set feedback off' from dual;

SELECT 'exec dbms_aqadm.stop_queue('||chr(39)|| name ||chr(39)|| ');' 
   FROM  user_queues;
   
SELECT 'exec dbms_aqadm.drop_queue('||chr(39)|| name ||chr(39)|| ');' 
   FROM user_queues; 
   
SELECT 'exec dbms_aqadm.drop_queue_table(queue_table=>'||chr(39)|| queue_table ||chr(39)||', force=>TRUE);'
   FROM user_queue_tables;
  
-- Disable any continually-running jobs by triggering their business logic to exit
-- Give the jobs a chance to exit
--call dbms_lock.sleep(10);

-- Disable any scheduled jobs so that no new runs are started while we are dropping
SELECT 'exec dbms_scheduler.disable('||chr(39)||job_name||chr(39)||', true);' 
    from user_scheduler_jobs;

-- Stop any running jobs
-- First stop with "force" set to "false"
SELECT 'exec dbms_scheduler.stop_job('||chr(39)||job_name||chr(39)||','||'FALSE'||');' 
    from user_scheduler_jobs
    where state = 'RUNNING';


connect system/manager@$ORACLE_SID
SELECT 'exec dbms_scheduler.stop_job('||chr(39)||job_name||chr(39)||','||'TRUE'||');' 
    from user_scheduler_jobs
    where state = 'RUNNING';
connect $DB

-- Now stop with "force" set to "true" - however this will typically fail
-- unless run as a higher-privileged user (such as sytem or sys)
SELECT 'exec dbms_scheduler.stop_job('||chr(39)||job_name||chr(39)||','||'TRUE'||');' 
    from user_scheduler_jobs
    where state = 'RUNNING';

SELECT 'exec dbms_scheduler.drop_job('||chr(39)||job_name||chr(39)||');' 
    from user_scheduler_jobs;

SELECT 'exec dbms_scheduler.drop_program('||chr(39)||program_name||chr(39)||');'
    from user_scheduler_programs;
 
__EOF__

sqlplus -S $DB << __EOF__ | sqlplus -S $DB
SET PAGES 0 TRIMS ON ECHO OFF VERIFY OFF FEEDBACK OFF TERMOUT OFF LINESIZE 3000
SELECT 'set feedback off' from dual;
SELECT 'drop view ' || view_name || ';'
   FROM user_views;
SELECT 'drop sequence ' || sequence_name || ';' 
   FROM user_sequences;
   
SELECT 'drop trigger ' || trigger_name || ';' 
   FROM user_triggers;
   
SELECT distinct 'drop '||object_type||' '|| object_name || ';' 
   FROM user_objects where object_type in ('FUNCTION','PROCEDURE');

SELECT  distinct 'drop package '|| object_name ||';' 
   FROM user_objects where object_type='PACKAGE';

SELECT 'drop table ' || table_name || ' cascade constraints;' 
   FROM user_all_tables WHERE NESTED='NO' and (IOT_TYPE='IOT' or IOT_TYPE is  null);

SELECT 'drop type ' || type_name || ' force;' from user_types;

__EOF__
