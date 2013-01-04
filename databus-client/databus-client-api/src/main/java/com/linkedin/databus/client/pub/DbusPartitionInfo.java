package com.linkedin.databus.client.pub;

/**
 * 
 * Databus Partition Info 
 *
 */
public interface DbusPartitionInfo 
{
	/**
	 * 
	 * @return numeric id of this partition
	 */
	public long getPartitionId();

	/**
	 * 
	 * Checks if other partition is equal to this instance
	 * @param other
	 * @return true if equal otherwise false
	 */
	public boolean equalsPartition(DbusPartitionInfo other);

}
