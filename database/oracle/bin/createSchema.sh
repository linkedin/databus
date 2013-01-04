usage() {
  echo "Usage: $0 <username/password@SID> <SRC_VIEW_DIR>"
}

# Database Connection Info
DB=$1

# Log Directory
SRC_VIEW_DIR=$2

# Check that DB is set
if [ "x$DB" = "x" ]
then
  usage
  exit 1
fi

user=`echo $DB | perl -lane '{my $a = $_; $a =~ s/([^\/]*)\/.*/$1/; print $a;}'`;
password=`echo $DB | perl -lane '{my $a = $_; $a =~ s/[^\/]*\/([^@]*)\@.*/$1/; print $a;}'`;
sid=`echo $DB | perl -lane '{my $a = $_; $a =~ s/[^\/]*\/[^@]*\@(.*)/$1/; print $a;}'`;

if [ "x$user" = "x" ] || [ "x$password" = "x" ] || [ "x$sid" = "x" ]
then
  echo "First argument not in format <username/password@SID>"
  usage
  exit 1
fi

if [ ! -d "$SRC_VIEW_DIR" ]
then
  echo "Src view directory not found or not a directory"
  usage
  exit 1
fi

if [ ! -f "$SRC_VIEW_DIR/tablespace" ]
then
  echo "Tablespace file ($SRC_VIEW_DIR/tablespace) not provided"
  usage
  exit 1
fi

tbs=`cat $SRC_VIEW_DIR/tablespace`;
tbs_uc=`echo $tbs | tr '[A-Z]' '[a-z]'`
tbs_lc=`echo $tbs | tr '[a-z]' '[A-Z]'`


echo "User : $user"
echo "Password : $password"
echo "SID : $sid"
echo "DB : $DB"
echo "TBS : $tbs_uc"

../fwk/schema/ddl_sqs.sh "$DB"
../fwk/schema/ddl_tab.sh "$DB" "$tbs_uc"

for t in ${SRC_VIEW_DIR}/*\.tab; 
do
  echo "Processing Table definition File : $t"
  table=`echo $t | perl -lane '{ $a = $_; $a =~ s/.*\/(.*)\.tab/$1/; print $a; }'` 
  echo "Creating Table $table. File is $t"
  echo "sqlplus ${DB} "@$t" ${sid} ${user} ${password} ${tbs_lc}"
  sqlplus ${DB} << __EOF__
   @$t ${sid} ${user} ${password} ${tbs_lc}
__EOF__
done

../fwk/schema/ddl_con.sh "$DB" "$tbs_uc"
../fwk/schema/ddl_ind.sh "$DB" "$tbs_uc"

for t in ${SRC_VIEW_DIR}/*\.tab; 
do
  table=`echo $t | perl -lane '{ $a = $_; $a =~ s/.*\/(.*)\.tab/$1/; print $a; }'` 
  echo "Setting up Alerts for  $table. File is $t"
  ../fwk/schema/ddl_prc.sh "$DB" "$table"
done

../fwk/schema/ddl_vw.sh "$DB"

for t in ${SRC_VIEW_DIR}/*\.view; 
do
  echo "Processing View definition File : $t"
  view=`echo $t | perl -lane '{ $a = $_; $a =~ s/.*\/(.*)\.view/$1/; print $a; }'` 
  echo "Creating view sy\$$view"
  pattern="sy\\$";
  pattern+="${view}";
  wc=`grep -ic "${pattern}" $t`
  if [ $wc -lt 1 ];
  then
     echo "View names should start with sy\$. Offending view file ($t)";
     exit 1
  fi
  sqlplus ${DB}  << __EOF__
    @$t ${sid} ${user} ${password} ${tbs_lc};
__EOF__
done

## execute rest
for t in ${SRC_VIEW_DIR}/*\.tab; 
do
  table=`echo $t | perl -lane '{ $a = $_; $a =~ s/.*\/(.*)\.tab/$1/; print $a; }'` 
  echo "Creating trigger for source $table"
  ../fwk/schema/ddl_prc1.sh  "$DB" "$table"
done



for t in ${SRC_VIEW_DIR}/*\.tab; 
do
  table=`echo $t | perl -lane '{ $a = $_; $a =~ s/.*\/(.*)\.tab/$1/; print $a; }'` 
  echo "Creating trigger for source $table"
  ../fwk/schema/ddl_trg.sh "$DB" "$table"
done

echo "INFO: compiling types"
sqlplus $DB << __EOF__ 
exec compile_allobjects;
__EOF__


echo "INFO: Sync_core seeding "
sqlplus $DB << __EOF__ 
  insert into sync_core_settings values ('N');
  commit;
  show errors;
__EOF__


echo "INFO: sy\$SOURCES setup"
i=0;
for t in ${SRC_VIEW_DIR}/*\.tab; 
do
  echo "Processing Table definition File : $t"
  table=`echo $t | perl -lane '{ $a = $_; $a =~ s/.*\/(.*)\.tab/$1/; print $a; }'` 
  echo "Creating Table $table"
  sqlplus $DB << __EOF__ 
    insert into sy\$sources values('$table',$i);
__EOF__
  ((i++))
done
