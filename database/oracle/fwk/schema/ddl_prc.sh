if [ "$#" -lt 2 ]  
then    
        echo "Usage: $0 <username/password[@SID]> <SRC_TABLE>"
        exit 1                                                                                          
fi                                 
                      
DB=$1 
SRC_TABLE=$2

sqlplus $DB << __EOF__ 
REM this procedure is to compile all the invalid objects in current schema (user)

create or replace procedure compile_allobjects AUTHID CURRENT_USER is
cursor c1 is select 'alter '||decode(object_type,'PACKAGE BODY','PACKAGE',object_type)||' '||object_name||' '||  decode(object_type,'PACKAGE BODY','COMPILE BODY','COMPILE')
from user_objects where status='INVALID' order by object_type;
stmt varchar2(1000);
begin
open c1;
LOOP
fetch c1 into stmt;
exit when C1%NOTFOUNd;
EXECUTE IMMEDIATE stmt ;
end loop;
end compile_allobjects;
/
show errors



------------------------------------------------------------------------------
---------------------------- DATABUS PACKAGES --------------------------------
------------------------------------------------------------------------------
PROMPT creating sync_core package
-- the purpose of this package is to generate the txn and generates the
-- alerts

create or replace package sync_core as

function getTxn(source in varchar) return number;
function getScn(v_scn in number, v_ora_rowscn in number) return number;
procedure coalesce_log;
procedure signal_beep;
procedure unconditional_signal_beep;
end sync_core;
/
show errors

create or replace package body sync_core as

infinity constant number := 9999999999999999999999999999;
type source_bit_map_t is table OF number index by VARCHAR2(30);

lastTxID varchar2(50);
currentTxn number;
currentMask number;

source_bits source_bit_map_t;

function getScn(v_scn in number, v_ora_rowscn in number) return number as
begin
  if(v_scn = infinity) then
    return v_ora_rowscn;
  else
    return v_scn;
  end if;
end;

function getMask(source in varchar) return number as
   bitnum number;
begin
  if not source_bits.exists(source) then
    select bitnum into bitnum from sy\$sources where name = source;
    source_bits(source) := power(2,bitnum);
  end if;
  return source_bits(source);
end;

-- this is the 'magical' function which creates and returns the Txn (aka
-- transaction number)
function getTxn(source in varchar) return number as
  currentTxID varchar2(50);
  mask        number;
  txn_chk    char(1);
begin
  -- we can get the local transaction id (guaranteed to be unique for the
  -- current transaction)
  currentTxID := DBMS_TRANSACTION.LOCAL_TRANSACTION_ID();
  if currentTxID is null then
    currentTxID := DBMS_TRANSACTION.LOCAL_TRANSACTION_ID(TRUE);
  end if;

  mask := getMask(source);

  -- lastTxID is package scope => may have no value or a value from previous
  -- usage. If value is different it means we are in a new transaction
  if (lastTxID is null or lastTxID <> currentTxID) then
    select sy\$scn_seq.nextval into currentTxn from dual;
    currentMask := mask;
    lastTxID := currentTxID;
    insert into sy\$txlog(txn,scn,mask) values(currentTxn,infinity,currentMask);
    signal_beep();
  else
    begin
      select 'Y' into txn_chk from sy\$txlog where txn=currentTxn;
      exception when others then txn_chk :='N';
    end;
    if  txn_chk='N' then
      insert into sy\$txlog(txn,scn,mask) values(currentTxn,infinity,currentMask);
      signal_beep();
    else
      if (bitand(currentMask,mask) = 0) then
        currentMask := currentMask + mask;
      end if;
      update sy\$txlog set mask = currentMask where txn = currentTxn and mask <> currentMask;
      if SQL%ROWCOUNT > 0 then
        signal_beep();
      end if;
    end if;
  end if;

  return currentTxn;
end;

procedure coalesce_log
as
cursor cur_sytxlog_scn is
  select txn, ora_rowscn from sy\$txlog where scn=infinity for update;
  sytxlog1 cur_sytxlog_scn%ROWTYPE;
begin
  open cur_sytxlog_scn;
  LOOP
    fetch cur_sytxlog_scn into sytxlog1;
    exit when cur_sytxlog_scn%NOTFOUND;
    update sy\$txlog set scn=sytxlog1.ora_rowscn where scn=infinity and txn=sytxlog1.txn;
  END LOOP;
  close cur_sytxlog_scn;
  commit;
end;

procedure signal_beep
as
  v_raise_dbms_alerts    char(1);
begin
  select raise_dbms_alerts into v_raise_dbms_alerts from sync_core_settings;
  if v_raise_dbms_alerts = 'Y' then
    unconditional_signal_beep();
  end if;
end;

procedure unconditional_signal_beep
as
begin
  dbms_alert.signal('sy\$alert_${SRC_TABLE}', 'beep');
  exception when others then
  -- if we get an exception while raising the signal we ignore it
  null;
end;

end sync_core;
/

BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM(
program_name=>'P_COALESCE_LOG',
program_action=>'Begin
sync_core.coalesce_log;
end;',
program_type=>'PLSQL_BLOCK',
number_of_arguments=>0,
comments=>'New program used to update scn',
enabled=>TRUE);
DBMS_SCHEDULER.CREATE_JOB(
JOB_NAME => 'J_COALESCE_LOG',
PROGRAM_NAME => 'P_COALESCE_LOG',
REPEAT_INTERVAL  => 'FREQ=SECONDLY;INTERVAL=2',
START_DATE => systimestamp at time zone 'US/Pacific',
COMMENTS => 'this will update the scn on sy$txlog',
AUTO_DROP => FALSE,
ENABLED => FALSE);
DBMS_SCHEDULER.SET_ATTRIBUTE(NAME=>'J_COALESCE_LOG', attribute => 'restartable', value=>TRUE);
DBMS_SCHEDULER.SET_ATTRIBUTE( NAME =>'J_COALESCE_LOG', attribute =>'logging_level', value => DBMS_SCHEDULER.LOGGING_OFF);
DBMS_SCHEDULER.ENABLE('J_COALESCE_LOG');
END;
/

BEGIN
dbms_scheduler.create_job(
job_name => 'J_CALL_SIGNAL',
job_type => 'PLSQL_BLOCK',
job_action => 'begin
   sync_core.unconditional_signal_beep;
end;',
repeat_interval => 'FREQ=SECONDLY',
start_date =>  systimestamp at time zone 'US/Pacific',
job_class => 'DEFAULT_JOB_CLASS',
comments => 'Call sync_core.unconditional_signal_beep to signal that databus events MAY be available',
auto_drop => FALSE,
enabled => FALSE);
sys.dbms_scheduler.set_attribute( name => 'J_CALL_SIGNAL', attribute => 'job_priority', value => 1);
sys.dbms_scheduler.set_attribute( name => 'J_CALL_SIGNAL', attribute => 'logging_level', value => DBMS_SCHEDULER.LOGGING_OFF);
sys.dbms_scheduler.set_attribute( name => 'J_CALL_SIGNAL', attribute => 'job_weight', value => 1);
sys.dbms_scheduler.disable( 'J_CALL_SIGNAL' );
END;
/

show errors
__EOF__

