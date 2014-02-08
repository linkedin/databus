package com.linkedin.databus.client.pub;

import com.linkedin.databus.core.data_model.PhysicalPartition;

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
 * An optional interface that the application may choose to implement to be able to get notifications
 * before a physical partition is added/dropped
 * 
 * Such a concrete implementation (say dbusPartitionListener) may be registered with the databus3 client as follows
 * databusHttpV3ClientImpl.registerCluster(clusterName, dbusConsumerFactory,source(s) ).withDbusv3PartitionListener(dbusPartitionListener))
 */
public interface DbusV3PartitionListener
{
	/**
	 * This method is invoked before the client instance starts listening to a new physicalPartition
	 * of Espresso
	 *
	 * The registration is created with subscriptions and is set to INIT state.
	 * The client application may change checkpoint if needed. Only the following operations are allowed on 
	 * @param reg {@link DatabusV3Registration#getLastPersistedCheckpoint()}, {@link DatabusV3Registration#storeCheckpoint(Checkpoint)
	 * 
	 * If the application is caching the child registration, it is expected to remove the cache entry in the 
	 * {@link #onDropPartition(PhysicalPartition, DatabusV3Registration)}
	 * 
	 * @param physicalPartition : PhysicalPartition object representing a single partition in Espresso
	 *                            E.g., if partition is MyDB_2, getId() == 2, getName() == "MyDB"
	 * @param reg : DatabusV3Registration object for the above specified partition ( a child registration )
	 */
	public void onAddPartition(PhysicalPartition physicalPartition, DatabusV3Registration reg);

	/**
	 * This method is invoked after the client instance stops listening to an Espresso partition
	 *
	 * The registration returned is the same as what was presented in the {{@link #onAddPartition(PhysicalPartition, DatabusV3Registration)}
	 * for the physicalPartition. ( The client library guarantees that there would have been such an invocation
	 * earlier )
	 * 
	 * This method is invoked purely for informational purpose for the client
	 * 
	 * @param physicalPartition : PhysicalPartition object representing a single partition in Espresso
	 *                            E.g., if partition is MyDB_2, getId() == 2, getName() == "MyDB"
	 * @param reg : DatabusV3Registration object for the above specified partition ( a child registration )
	 */
	public void onDropPartition(PhysicalPartition physicalPartition, DatabusV3Registration reg);
}
