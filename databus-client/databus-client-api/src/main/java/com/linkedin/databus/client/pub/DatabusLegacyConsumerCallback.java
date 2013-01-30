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
 * Application call-back interface for processing Databus V1 event objects
 */
public interface DatabusLegacyConsumerCallback<V1>
{
    /* Process one V1 data Event */
    ConsumerCallbackResult onEvent(DatabusEventHolder<V1> event);
  
    /* Process a list of V1 data Events */
    ConsumerCallbackResult onEventsBatch(List<DatabusEventHolder<V1>> events);
    
    /* Process one V1 bootstrap Event */
    ConsumerCallbackResult onBootstrapEvent(DatabusEventHolder<V1> event);
    
    /* Process a list of V1 bootstrap Events */
    ConsumerCallbackResult onBootstrapEventsBatch(List<DatabusEventHolder<V1>> events);
}
