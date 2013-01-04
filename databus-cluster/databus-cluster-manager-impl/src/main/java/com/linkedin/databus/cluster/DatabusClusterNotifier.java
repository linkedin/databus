package com.linkedin.databus.cluster;

/** Notifier callback interface for receiving signals to member from cluster **/
public interface DatabusClusterNotifier
{

    /**
     * Called when ownership is granted
     * @param partition
     */
	void onGainedPartitionOwnership(int partition);
	
	/**
	 * Called when ownership is revoked
	 * @param partition
	 */
	void onLostPartitionOwnership(int partition);
	
	/**
	 * Called when there is an error
	 * @param partition
	 */
	void onError(int partition);
	
	/**
	 * Called when the ownership is being transferred , e.g. when this member is disconnected from cluster 
	 * @param partition
	 */
	void onReset(int partition);
	
}
