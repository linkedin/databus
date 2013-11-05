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


import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class ClusterRegistrationConfig implements ConfigBuilder<ClusterRegistrationStaticConfig>
{
	private static final String DEFAULT_CLUSTER_NAME = "";
	private static final String DEFAULT_ZK_ADDR = "";
	private static final long DEFAULT_NUM_PARTITIONS = 1;
	private static final long DEFAULT_QUORUM = 1;
	private static final int DEFAULT_NUM_WRITES_SKIPPED = 0;
	private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 5*60*1000; //5 mins
	public static final long  MIN_CHECKPOINT_INTERVAL_MS = 5*60*1000; //5 mins;

	public static final int DEFAULT_CONNECTION_TIMEOUT_MSEC = 60 * 1000;
	public static final int DEFAULT_SESSION_TIMEOUT_MSEC = 30 * 1000;

    protected static final Logger LOG = Logger.getLogger(ClusterRegistrationConfig.class);

	/**
	 *  Cluster Name
	 */
	private String _clusterName;

	/**
	 * ZK HostPort config seperated by colon
	 */
	private String _zkAddr;

	/**
	 * Total number of partitions for this cluster.
	 */
	private long _numPartitions;

	/**
	 * Minimum number of nodes to be active before partitions can be allocated.
	 */
	private long _quorum;

	/**
	 * *DEPRECATED this has no effect use checkpointIntervalMs instead*
	 * Number of checkpoints that can skipped before persisting the progress in ZooKeeper.
	 * This is an optimization to reduce the ZK overhead during checkpointing.
	 */
	private int _maxCkptWritesSkipped;

	/**
	 * Minimum interval that will elapse in ms before a checkpoint is saved/persisted by the client library
	 * Minimum value is 1 minute
	 */
	private long _checkpointIntervalMs;

	/**
	 * ZK Session Timeout (in millis)
	 */
	private int _zkSessionTimeoutMs;

	/**
	 * ZK Connection Timeout (in millis)
	 */
	private int _zkConnectionTimeoutMs;


	/**
	 *  Cluster Name
	 */
	public String getClusterName() {
		return _clusterName;
	}

	public void setClusterName(String clusterName) {
		this._clusterName = clusterName;
	}

	/**
	 * ZK HostPort config seperated by colon
	 */
	public String getZkAddr() {
		return _zkAddr;
	}

	public void setZkAddr(String zkAddr) {
		this._zkAddr = zkAddr;
	}

	/**
	 * Total number of partitions for this cluster.
	 */
	public long getNumPartitions() {
		return _numPartitions;
	}

	public void setNumPartitions(long numPartitions) {
		this._numPartitions = numPartitions;
	}

	/**
	 * Minimum number of nodes to be active before partitions can be allocated.
	 */
	public long getQuorum() {
		return _quorum;
	}

	public void setQuorum(long quorum) {
		this._quorum = quorum;
	}

	/**
	 * Number of checkpoints that can skipped before persisting the progress in ZooKeeper.
	 * This is an optimization to reduce the ZK overhead during checkpointing.
	 */
	public int getMaxCkptWritesSkipped() {
		return _maxCkptWritesSkipped;
	}

	public void setMaxCkptWritesSkipped(int maxCkptWritesSkipped) {
		this._maxCkptWritesSkipped = maxCkptWritesSkipped;
	}

	public long getCheckpointIntervalMs() {
		return _checkpointIntervalMs;
	}

	public void setCheckpointIntervalMs(long checkpointIntervalMs) {
		if (checkpointIntervalMs >= MIN_CHECKPOINT_INTERVAL_MS)
		{
			_checkpointIntervalMs = checkpointIntervalMs;
		}
		else
		{
			LOG.warn("checkpointIntervalMs cannot be set to lower than" + MIN_CHECKPOINT_INTERVAL_MS + ". Setting to " + MIN_CHECKPOINT_INTERVAL_MS);
			_checkpointIntervalMs = MIN_CHECKPOINT_INTERVAL_MS;
		}
	}

	@Override
	public String toString() {
		return "ClusterRegistrationConfig [clusterName=" + _clusterName
				+ ", zkAddr=" + _zkAddr + ", numPartitions=" + _numPartitions
				+ ", quorum=" + _quorum + ", maxCkptWritesSkipped="
				+ _maxCkptWritesSkipped + ", checkpointIntervalMs=" + _checkpointIntervalMs +
				"]";
	}

	public ClusterRegistrationConfig() {
		super();
		_clusterName = DEFAULT_CLUSTER_NAME;
		_zkAddr = DEFAULT_ZK_ADDR;
		_numPartitions = DEFAULT_NUM_PARTITIONS;
		_quorum = DEFAULT_QUORUM;
		_maxCkptWritesSkipped = DEFAULT_NUM_WRITES_SKIPPED;
		_checkpointIntervalMs = DEFAULT_CHECKPOINT_INTERVAL_MS;
		_zkConnectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MSEC;
		_zkSessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MSEC;
	}

	@Override
	public ClusterRegistrationStaticConfig build()
			throws InvalidConfigException {
		return new ClusterRegistrationStaticConfig(_clusterName, _zkAddr, _numPartitions, _quorum, _maxCkptWritesSkipped,_checkpointIntervalMs, _zkSessionTimeoutMs, _zkConnectionTimeoutMs);
	}

	public int getZkSessionTimeoutMs() {
		return _zkSessionTimeoutMs;
	}

	public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
		this._zkSessionTimeoutMs = zkSessionTimeoutMs;
	}

	public int getZkConnectionTimeoutMs() {
		return _zkConnectionTimeoutMs;
	}

	public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
		this._zkConnectionTimeoutMs = zkConnectionTimeoutMs;
	}
}
