usage() {
  echo "Usage: $0 <username/password[@SID]> <num_records> <start_primary_key> [load|unload]"
}

# Database Connection Info
DB=$1

# numRecords
NUM_RECORDS=$2

# start Primary Key
START_ID=$3

CMD=$4

# Check that DB is set
if [ "x$DB" = "x" ]
then
  usage
  exit 1
fi

DBUSER=${DB%/*}
ORACLE_SID=${DB#*\@}
echo $DB   $DBUSER  $ORACLE_SID

# Check num_records set
if [ "x$NUM_RECORDS" = "x" ]
then
  usage
  exit 1
fi

# Check start_primary_key set
if [ "x$START_ID" = "x" ]
then
  usage
  exit 1
fi

# Check cmd
if [ "x$CMD" = "x" ]
then
  usage
  exit 1
fi

let b=${START_ID};
let e="${START_ID} + ${NUM_RECORDS}";

echo "$b..$e"

echo "INFO: define and install load data routines"
sqlplus $DB << __EOF__
set serveroutput on 
Create or Replace Package TestPerson as
  Procedure loadTestPerson;
  Procedure unloadTestPerson;
End TestPerson;
/

Create or Replace Package Body TestPerson as
Procedure loadTestPerson is
  dob date;
begin
  dbms_output.put_line('started inserting');
for cntr IN $b..$e LOOP
  dbms_output.put_line(cntr);
  dob := SYSDATE - DBMS_RANDOM.VALUE(1,100000);
  insert into PERSON(id,first_name,last_name,birth_date,deleted) VALUES(cntr,dbms_random.string( 'u', 20 ),dbms_random.string( 'l', 20 ),dob,'FALSE');  
END LOOP;
  dbms_output.put_line('completed inserting');
end loadTestPerson;

-- Un-Loads the test news slices from the database 
Procedure unloadTestPerson is 
begin 
  delete from person where id >= $b and id <= $e; 
end unloadTestPerson; 
 
END TestPerson; 
/
show errors;
connect system/manager@$ORACLE_SID
grant /*+QA*/ create public synonym, drop public synonym to $DBUSER;
connect $DB
create or replace /*+QA*/ public synonym Test for test;
show errors;
__EOF__


if [ "$CMD" = "load" ];
then
   echo "INFO: Call load"
   sqlplus $DB << __EOF__
     set serveroutput on; 
     call TestPerson.loadTestPerson();
__EOF__
else
   echo "INFO: Call unload"
   sqlplus $DB << __EOF__
     set serveroutput on ;
     call TestPerson.unloadTestPerson();
__EOF__
fi
