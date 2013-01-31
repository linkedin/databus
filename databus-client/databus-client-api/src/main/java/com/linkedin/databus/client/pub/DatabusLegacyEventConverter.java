package com.linkedin.databus.client.pub;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.List;

/*
 * Interface for converting Databus V2 Event Objects to V1 Event Objects
 * 
 */
public interface DatabusLegacyEventConverter<V1, V2>
{
  /*
   * Convert a V2 Event object to a V1  event Object
   */
  DatabusEventHolder<V1> convert(DatabusEventHolder<V2> event);
  
  
  /*
   * Convert a List<V2> Event objects  to a List of V1 events objects
   */
  List<DatabusEventHolder<V1>> convert(List<DatabusEventHolder<V2>> events);
}
