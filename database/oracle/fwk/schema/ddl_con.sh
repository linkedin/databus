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

