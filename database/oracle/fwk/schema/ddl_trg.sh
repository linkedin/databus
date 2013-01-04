if [ "$#" -lt 2 ]  
then    
        echo "Usage: $0 <username/password[@SID]> <SRC_TABLE>"
        exit 1                                                                                          
fi                                 
                      
DB=$1 
SRC_TABLE=$2


sqlplus $DB << __EOF__ 
PROMPT creating new trigger for ${SRC_TABLE}
CREATE TRIGGER ${SRC_TABLE}_TRG
  before insert or update on ${SRC_TABLE}
  referencing old as old new as new
  for each row
begin
  if (updating and :new.txn < 0) then
    :new.txn := -:new.txn;
  else
    :new.txn := sync_core.getTxn('${SRC_TABLE}');
  end if;
end;
/

show errors;
commit;
__EOF__
