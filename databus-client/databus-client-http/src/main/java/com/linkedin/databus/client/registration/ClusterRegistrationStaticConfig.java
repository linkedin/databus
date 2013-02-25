package com.linkedin.databus.client.registration;
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
