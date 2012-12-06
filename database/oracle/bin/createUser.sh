usage() {
  echo "Usage: $0 username password dbname tablespace dbdir"
}

# Username
USER=$1

#Password
PASSWD=$2

#DBNAME
DBNAME=$3

#TableSpace
TBS=$4

#DBDIR
DBDIR=$5

# Default Log Dir
DEFAULT_LOG_DIR=logs

if [ "x$USER" = "x" ] || [ "x$PASSWD" = "x" ] || [ "x$DBNAME" = "x" ] || [ "x$TBS" = "x" ] || [ "x$DBDIR" = "x" ] 
then
  usage
  exit 1
fi

TBS_UC=`echo $TBS | tr '[A-Z]' '[a-z]'`
TBS_LC=`echo $TBS | tr '[a-z]' '[A-Z]'`

echo "INFO: creating user and tablespace"
sqlplus system/manager\@${DBNAME} as sysdba << __EOF__ 
drop tablespace ${TBS_UC};
drop tablespace ${TBS_UC}_IDX;
create tablespace ${TBS_UC} datafile '${DBDIR}/${TBS_LC}_01.dbf' size 50M reuse autoextend on next 50M maxsize unlimited extent management local uniform size 2M;
create tablespace ${TBS_UC}_IDX datafile '${DBDIR}/${TBS_LC}_idx_01.dbf' size 50M reuse autoextend on next 50M maxsize unlimited extent management local uniform size 2M;
drop user ${USER} cascade;
create user ${USER} identified by ${PASSWD} default tablespace ${TBS_UC} temporary tablespace temp1;
grant create session, create table, create view, create sequence, create procedure, create trigger, create type, create job  to ${USER};
grant query rewrite to ${USER};
grant execute on dbms_alert to ${USER};
grant execute on sys.dbms_lock to ${USER};
grant select on sys.v_\$database to ${USER};
grant execute on sys.dbms_aq to ${USER};
grant execute on sys.dbms_aqadm to ${USER};
grant execute on sys.dbms_aqin to ${USER};
grant execute on sys.dbms_aq_bqview to ${USER};
alter user ${USER} quota unlimited on ${TBS_UC};
alter user ${USER} quota unlimited on ${TBS_LC}_IDX;
show errors;
__EOF__

