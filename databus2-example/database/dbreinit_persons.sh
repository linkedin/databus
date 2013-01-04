mkdir -p logs

echo "cleanup old schema"
(cd ../../database/oracle/bin && ./dropSchema.sh person/person@DB > ../../../databus2-example/database/logs/person_drop_user.log)

echo "Setting up User and tablespace"
(cd ../../database/oracle/bin && ./createUser.sh person person DB tbs_person /mnt/u001/oracle/data/DB > ../../../databus2-example/database/logs/person_cr_user.log)

echo "Setting up tables"
(cd ../../database/oracle/bin && ./createSchema.sh person/person@DB ../../../databus2-example/database/person/ > ../../../databus2-example/database/logs/person_cr_schema.log)

