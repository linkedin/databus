package com.linkedin.databus.cluster;
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


import java.util.Map;
import java.util.List;

/**
 * 
 * @author snagaraj
 * Receive notifications about cluster meta data changes
 */
public interface DatabusClusterDataNotifier
{
    
        /**
         * 
         * @param list of activeNodes in the cluster
         * Return a list of nodes 
         */
        void onInstanceChange(List<String> activeNodes) ;
        
        
        /**
         * 
         * @param map of partition->id (node id) of those partitions actively serviced by nodes 
         */
        void onPartitionMappingChange(Map<Integer,String> activePartitionMap);
        
    
}
