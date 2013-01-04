if [ "$#" -lt 2 ]
then   
        echo "Usage: $0 <username/password[@SID]> <TBS>"
        exit 1
fi

DB=$1
tbs=$2

TBS_UC=`echo $tbs | tr '[A-Z]' '[a-z]'`

echo "INFO: creating Primary Key contstraints on sy\$txlog"
sqlplus $DB << __EOF__ 
-- Adding primary key constraints
ALTER TABLE SY\$TXLOG
 ADD (CONSTRAINT SY\$TXLOG_PK PRIMARY KEY
        (TXN)
USING INDEX
INITRANS 2
MAXTRANS 255
PCTFREE 5 TABLESPACE ${TBS_UC}_IDX)
/
__EOF__

