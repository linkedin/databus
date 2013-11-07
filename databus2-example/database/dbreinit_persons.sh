#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
mkdir -p logs

echo "cleanup old schema"
(cd ../../database/oracle/bin && ./dropSchema.sh person/person@DB > ../../../databus2-example/database/logs/person_drop_user.log)

echo "Setting up User and tablespace"
(cd ../../database/oracle/bin && ./createUser.sh person person DB tbs_person /mnt/u001/oracle/data/DB > ../../../databus2-example/database/logs/person_cr_user.log)

echo "Setting up tables"
(cd ../../database/oracle/bin && ./createSchema.sh person/person@DB ../../../databus2-example/database/person/ > ../../../databus2-example/database/logs/person_cr_schema.log)

