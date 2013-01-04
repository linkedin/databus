if [ "$#" -lt 1 ]  
then    
        echo "Usage: $0 <username/password[@SID]>"
        exit 1                                                                                          
fi                                 
                      
DB=$1 


sqlplus $DB << __EOF__ 
PROMPT Creating Databus view 

create or replace force view DB_MODE
as
select open_mode from sys.v_\$database
/

show errors;
__EOF__

