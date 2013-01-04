package com.linkedin.databus.client.registration;

public class ClusterRegistrationStaticConfig 
{
	/**
	 *  Cluster Name
	 */
	private final String _clusterName;
	
	/**
	 * ZK HostPort config seperated by colon
	 */
	private final String _zkAddr;
	
	/**
	 * Total number of partitions for this cluster.
	 */
	private final long _numPartitions;
	
	/**
	 * Minimum number of nodes to be active before partitions can be allocated.
	 */
	private final long _quorum;
	
	/**
	 * Number of checkpoints that can skipped before persisting the progress in ZooKeeper.
	 * This is an optimization to reduce the ZK overhead during checkpointing.
	 */
	private final int _maxCkptWritesSkipped;

	public ClusterRegistrationStaticConfig(String clusterName, String zkAddr,
			long numPartitions, long quorum, int maxCkptWritesSkipped) {
		super();
		this._clusterName = clusterName;
		this._zkAddr = zkAddr;
		this._numPartitions = numPartitions;
		this._quorum = quorum;
		this._maxCkptWritesSkipped = maxCkptWritesSkipped;
	}

	public String getClusterName() {
		return _clusterName;
	}

	public String getZkAddr() {
		return _zkAddr;
	}

	public long getNumPartitions() {
		return _numPartitions;
	}

	public long getQuorum() {
		return _quorum;
	}

	public int getMaxCkptWritesSkipped() {
		return _maxCkptWritesSkipped;
	}

	@Override
	public String toString() {
		return "ClusterRegistrationStaticConfig [clusterName=" + _clusterName
				+ ", zkAddr=" + _zkAddr + ", numPartitions=" + _numPartitions
				+ ", quorum=" + _quorum + ", maxCkptWritesSkipped="
				+ _maxCkptWritesSkipped + "]";
	}
}
