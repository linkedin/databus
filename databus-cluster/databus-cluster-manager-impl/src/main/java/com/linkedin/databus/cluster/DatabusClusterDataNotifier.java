package com.linkedin.databus.cluster;

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
