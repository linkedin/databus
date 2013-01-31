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
