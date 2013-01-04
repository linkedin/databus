if [ "$#" -lt 2 ]
then   
        echo "Usage: $0 <username/password[@SID]> <TBS>"
        exit 1
fi

DB=$1
tbs=$2

TBS_LC=`echo $tbs | tr '[a-z]' '[A-Z]'`

sqlplus $DB << __EOF__ 
PROMPT Creating table for version control
CREATE TABLE PATCH_VERCONTROL
 (ID            NUMBER,
  DB_VERSION    VARCHAR2(10),
  PATCH_NUMBER  VARCHAR2(15),
  TIMESTAMP     DATE
  )
  INITRANS 1
  MAXTRANS 255
  PCTUSED 80
  PCTFREE 10 TABLESPACE ${TBS_LC}
  NOCACHE
/


----------------- DATABUS TABLES -------------------

PROMPT creating sources table
create table sy\$sources (
  name    varchar2(30),
  bitnum  number constraint sy\$sources_n1 not null
  )
INITRANS 1
MAXTRANS 255
PCTUSED 80
PCTFREE 10 TABLESPACE ${TBS_LC}
/

PROMPT creating table sy\$txlog
create table sy\$txlog (
  txn    number,
  scn    number constraint sy\$txlog_n1 not null,
  mask   number,
  ts     timestamp default systimestamp constraint sy\$txlog_n2 not null
  ) rowdependencies
INITRANS 1
MAXTRANS 255
PCTUSED 80
PCTFREE 10 TABLESPACE ${TBS_LC}
/

PROMPT creating sync core settings table
PROMPT by setting the value in this table
PROMPT we can make the databus to be sync/async with insert/updates

create table sync_core_settings (
raise_dbms_alerts char(1) constraint sync_core_settings_n1 not null
)
INITRANS 1
MAXTRANS 255
PCTUSED 90
PCTFREE 5 TABLESPACE ${TBS_LC}
/
----------------------------------------------------------------------------
__EOF__
