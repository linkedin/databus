if [ "$#" -lt 2 ]
then   
        echo "Usage: $0 <username/password[@SID]> <TBS>"
        exit 1
fi

DB=$1
tbs=$2

TBS_UC=`echo $tbs | tr '[A-Z]' '[a-z]'`

echo "INFO: creating index on SCN"
sqlplus $DB << __EOF__ 
PROMPT creating index on scn
create index sy\$txlog_I1 on sy\$txlog(scn)
INITRANS 2
MAXTRANS 255
PCTFREE 10 TABLESPACE ${TBS_UC}_IDX
/
__EOF__
