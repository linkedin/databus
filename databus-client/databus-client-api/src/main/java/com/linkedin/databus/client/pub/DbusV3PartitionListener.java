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
 *
 * databusHttpV3ClientImpl.registerCluster(clusterName, dbusConsumerFactory,source(s) ).withDbusv3PartitionListener(dbusPartitionListener))
 */
public interface DbusV3PartitionListener
{
  /**
   * This method is invoked before the client instance starts listening to a new physicalPartition
   * of Espresso
   *
   * The client application may change checkpoint in this method, by invoking
   * {{@link com.linkedin.databus.client.pub.DatabusV3Registration#storeCheckpoint(com.linkedin.databus.core.Checkpoint, com.linkedin.databus.core.data_model.PhysicalPartition)}}
   * on the registration object returned by {{@link com.linkedin.databus.client.pub.DatabusV3Client#registerCluster(String, DbusV3ClusterConsumerFactory, String...)}}
   *
   * @param physicalPartition : PhysicalPartition object representing a single partition in Espresso
   *                            E.g., if partition is MyDB_2, getId() == 2, getName() == "MyDB"
   * @param clusterRegistration: Handle to the instance of the registration (same as returned by {{@link com.linkedin.databus.client.pub.DatabusV3Client#registerCluster(String, DbusV3ClusterConsumerFactory, String...)}}.
   */
  public void onAddPartition(PhysicalPartition physicalPartition, DatabusV3Registration clusterRegistration);

  /**
   * This method is invoked after the client instance stops listening to an Espresso partition and is purely for informational purpose for the client
   *
   * @param physicalPartition : PhysicalPartition object representing a single partition in Espresso
   *                            E.g., if partition is MyDB_2, getId() == 2, getName() == "MyDB"
   * @param clusterRegistration: Handle to the instance of the registration (same as returned by {{@link com.linkedin.databus.client.pub.DatabusV3Client#registerCluster(String, DbusV3ClusterConsumerFactory, String...)}}.
   */
  public void onDropPartition(PhysicalPartition physicalPartition, DatabusV3Registration clusterRegistration);
}
