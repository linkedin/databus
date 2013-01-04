package com.linkedin.databus.client.pub;

/**
 * 
 * Databus Partition Listener interface. The client application is responsible for implementing a concrete class for this interface and 
 * registering in DatabusClient.registerCluster()
 * 
 * @author bvaradar
 *
 */
public interface DbusPartitionListener 
{
	/**
	 * 
	 * Listener interface triggered when this client instance starts listening to new partition
	 * Triggered when the registered is created with call-backs, subscriptions and server-side filter set. 
	 * Registration will be in INIT state. The client application can use this API to change  checkpoint/regId if needed.
	 *  
	 * @param partitionInfo : Databus Partition Info
	 * @param reg : Databus Registration controlling this stream partition.
	 */
	public void onAddPartition(DbusPartitionInfo partitionInfo, DatabusRegistration reg);
	
	/**
	 * Listener interface triggered when the client instance stops listening to a partition.
	 * Triggered after registration is shutdown but before deregistering.
	 * @param partitionInfo : Databus Partition Info
	 * @param reg : Databus Registration controlling this stream partition.
	 */
	public void onDropPartition(DbusPartitionInfo partitionInfo, DatabusRegistration reg);
}
