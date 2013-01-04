package com.linkedin.databus.client.pub;

/**
 * 
 * Databus Cluster Info
 */

public class DbusClusterInfo 
{
	  private final String _name;
	  private final long _totalPartitions;
	  private final long _minActiveNodes;
	  
	  /**
	   * 
	   * @param name Name of the cluster
	   * @param totalPartitions Total number of partitions for this cluster
	   * @param minActiveNodes Minimum number of nodes to become active before partition allocation starts.
	   */
	  public DbusClusterInfo( String name, long totalPartitions, long minActiveNodes)
	  {
		  _name = name;
		  _totalPartitions = totalPartitions;
		  _minActiveNodes = minActiveNodes;
	  }
	  
	 
	  /**  Name of the cluster that this registration wants to join */
	  public String getName()
	  {
		  return _name;
	  }

	  /**
	   * Total Number of partitions for the PEER cluster type.
	   */
	  public long getNumTotalPartitions()
	  {
		  return _totalPartitions;
	  }


	  /**
	   * Minimum number of client nodes to be Alive before this node starts listening to active partitions.
	   * At any time, if the number of nodes becomes less than this critical number, the nodes in the cluster will be suspended. 
	   */
	  public long getMinimumActiveNodes()
	  {
		  return _minActiveNodes;
	  }
}
