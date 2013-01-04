package com.linkedin.databus.client.pub;

import java.util.Collection;

public interface DbusClusterConsumerFactory 
{
   /**
    * 
    * Factory to instantiate new consumer callbacks.
    * 
    * In the case of cluster-aware registration, when the client-library receives helix notification 
    *  to subscribe to a new partition, the dbus client library
    *  
    *  (a) invoke this callback to let client application create new consumer callbacks for this partiiton
    *  (b) Invoke DbusServerSideFilterFactory (if provided) to let client application setup server-side filter.
    *  (b) Construct a registration with the subscriptions provided during registerCluster() call and add the callbacks from (a) and filter from (b) to it.
    *  (c) invoke DbusPartitionListener.onAddPartition(..) to let the client set checkpoints and regId if needed
    *  (d) start the new registration.
    *   
    *  The client application is expected to create new instance of consumer callbacks and not reuse consumer callbacks that
    *  have been associated with other registrations or returned in previous createPartitionedConsumers() calls. 
    *  No synchronization will be provided to ensure thread-safety if consumer callbacks are reused.
    *   
    *  @param clusterInfo  : DbusClientClusterInfo provided by the client-app during registration.
    *  @param partitionInfo: DbusPartitionInfo corresponding to the cluster manager notification
    *  
    *  @return ConsumerCallback for this partition
    */
	Collection<DatabusCombinedConsumer> createPartitionedConsumers(DbusClusterInfo clusterInfo,
                                 								   DbusPartitionInfo partitionInfo);
	
}
