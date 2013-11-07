package com.linkedin.databus.client;
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


import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus2.core.filter.DbusKeyFilter;

public interface DatabusBootstrapConnection
    extends DatabusServerConnection
{
  void requestTargetScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse);

  void requestStartScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse, String sourceNames);

  void requestStream(String sourcesIdList, DbusKeyFilter filter, int freeBufferSpace, Checkpoint cp,
                     DatabusBootstrapConnectionStateMessage stateReuse);

}
