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


import java.util.Collection;

import com.linkedin.databus.core.data_model.PhysicalPartition;


public interface DbusV3ClusterConsumerFactory
{
   /**
    * A concrete implementation of this interface must be implemented by the application. It will be used by the Databus
    * client library to instantiate new consumers
    *
    * In the case of load balanced mode for clients, the client-library receives notifications from Helix
    * to subscribe/drop a partition. The databus client library
    *  (a) Invokes this callback to create new consumers for this partiiton.
    *  (b) Constructs a registration with the subscriptions provided during {@link DatabusV3Client#registerCluster(String, DbusClusterConsumerFactory, String...)}
    *      call and adds the consumers from (a) to it.
    *  (c) If provided, will invoke {@link DbusV3PartitionListener#onAddPartition(PhysicalPartition, DatabusV3Registration)
    *      to let the client set checkpoints and regId (if needed).
    *  (d) Starts the new registration.
    *
    *  The client application is expected to create a new instance of consumer and not reuse consumers that
    *  have been associated with other registrations or returned in previous calls.
    *  No synchronization will be provided to ensure thread-safety if consumers are reused.
    *
    *  @param clusterInfo  : DbusClientClusterInfo provided by the application during registration.
    *  @param physicalPartition: physicalPartition corresponding to the helix notification
    *
    *  @return Collection<DatabusV3Consumer> A collection of consumers that process the incoming event stream in parallel.
    *                                        That is, each of the consumers processes a subset of the event stream.
    */
  Collection<DatabusV3Consumer> createPartitionedConsumers(DbusClusterInfo clusterInfo, PhysicalPartition physicalPartition);

}
