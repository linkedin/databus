package com.linkedin.databus.client.pub;
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

/**
 * Databus Partition Listener interface. The client application is responsible for implementing a concrete class for this
 * interface and registering in DatabusClient.registerCluster()
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
