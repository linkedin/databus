
if [ "$#" -lt 1 ]  
then    
        echo "Usage: $0 <username/password[@SID]>"
        exit 1                                                                                          
fi                                 
                      
DB=$1 

sqlplus $DB << __EOF__ 
PROMPT Creating Sequence 'vercontrol_seq'
CREATE SEQUENCE vercontrol_seq
 INCREMENT BY 1
 START WITH 101
 MAXVALUE 999999999999999999999999999
 MINVALUE 1
 NOCYCLE
 NOCACHE
/

create sequence SY\$SCN_SEQ
 increment by 1
 start with 1000
 maxvalue 999999999999999999999999999
 minvalue 1000
 nocycle
 cache 20
/
__EOF__

