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

