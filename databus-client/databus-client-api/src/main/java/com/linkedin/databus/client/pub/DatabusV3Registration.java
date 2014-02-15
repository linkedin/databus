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
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStatsMBean;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * A "registration" represents an identifier for a set of consumer(s) interested in
 * subscription(s). When we say a "registration" here, we mean "an object of type DatabusV3Registration"
 *
 * The user of the client library can specify a "RegistrationId" for the registration.
 * If that RegistrationId is currently not in use, it is allotted for the returned DatabusV3Registration object.
 *
 * If the user does not specify the RegistrationId, the library currently assigns one. The registration gets the
 * same id upon a restart.
 *
 * The persisted checkpoint for this particular consumerGroup is identified
 * through the RegistrationId, which is a hash generated off the consumer subscriptions.
 *
 * {@link DatabusV3Client#registerDatabusListener(DatabusV3Consumer, RegistrationId, DbusKeyCompositeFilterConfig, DatabusSubscription)}/
 * {@link DatabusV3Client#registerCluster(String, DbusClusterConsumerFactory, String)}
 * DatabusClient interface methods implemented by DatabusHttpV3ClientImpl
 */

public interface DatabusV3Registration
{

  /**
   * Initiates traffic consumption. After a consumer is registered, and until this call is invoked no
   * events will be seen.
   *
   * A connection is made to an appropriate relay / bootstrap server depending on the subscription and delivered
   * to the consumer
   *
   * @throws IllegalStateException If the registration is started multiple times
   *
   * Please note that this method does not declare explicitly that it throws an IllegalStateException. Reasons are explained below:
   * (1) It was not listed explicitly in releases prior to 2.13.2
   * (2) IllegalStateException is a runtime exception, and hence is not required to be declared
   * (3) We would like to maintain the same API erasure, to ensure no problems for client applications during upgrade.
   * (4) It may be explicitly declared going forward.
   */
  public StartResult start();

  /**
   * Stops traffic consumption. After completion of this call, all threads pulling events and
   * invoking this callback will be stopped. It is guaranteed that callbacks will not be invoked anymore.
   *
   * The registration object can still be inspected by client REST API.
   *
   * @throws IllegalStateException If the registration is not running.
   */
  public void shutdown()
  throws IllegalStateException;

  /**
   * Pauses traffic consumption asynchronously (i.e., an asynchronous call is made to the component which pulls
   * the events) If there are outstanding events in the client buffer, they will be dispatched to the consumer
   * until the buffer becomes empty.
   *
   * The registration can be paused for an arbitrary length of time, and may be shut down after being paused
   * (if desired).
   *
   * @throws IllegalStateException If the registration is not running
   */
  public void pause()
  throws IllegalStateException;

  /**
   * Suspend traffic consumption.  This is similar to the behavior of {{@link #pause()} and is asynchronous in
   * nature. Client applications are expected to use this API when an error/exception condition causes the client
   * application to suspend the consumption of events.
   *
   * The registration can be suspended for an arbitrary length of time, and may be shut down after being suspended
   * (if desired).
   *
   * @param ex Throwable for the exceptional/error condition.
   * @throws IllegalStateException If the registration is not running.
   */
  public void suspendOnError(Throwable ex)
  throws IllegalStateException;

  /**
   * Resume traffic consumption. This is an asynchronous call to the component which pulls events from the relay.
   * This can be called when the consumption is paused/suspended.
   *
   * @throws IllegalStateException If the registration is not paused or suspended.
   */
  public void resume()
  throws IllegalStateException;

  /**
   * Returns the internal registration state of the object
   *
   * An object of DatabusV3Registration obeys a state-machine with states in {@link RegistrationState}
   */
  public RegistrationState getState();

  /**
   * Stop the data processing of the consumer(s) on the registration. First, it shuts down the connections
   * between the puller thread in the client to the relay/bootstrap server. Then it, shuts down the event processing
   * loop on the consumer.
   *
   * This is a synchronous operation, and once the deregister is received, events are no longer received.
   *
   * TODO : DDSDBUS-2699
   * @note WARNING : Prior to 2.13.2, a shutdown would be invoked on the If this is the only registration for which the pull threads,
   * then a shutdown is invoked on the underlying serverContainer. That behavior is changed now, the application has to explicitly
   * invoke a shutdown() on the parent registration if a shutdown is required
   *
   * @return DeregisterResult
   * @throws IllegalStateException If the registration is already in DEREGISTERED state
   *
   * Please note that this method does not declare explicitly that it throws an IllegalStateException. Reasons are explained below:
   * (1) It was not listed explicitly in releases prior to 2.13.2
   * (2) IllegalStateException is a runtime exception, and hence is not required to be declared
   * (3) We would like to maintain the same API erasure, to ensure no problems for client applications during upgrade.
   * (4) It may be explicitly declared going forward.
   */
  public DeregisterResult deregister();

  /**
   * Obtains a cloned Collection of all subscriptions associated with this registration.
   * Changing this subscription will not have any effect on the registration.
   */
  public List<DatabusSubscription> getSubscriptions();

  /**
   * Returns an object that implements DatabusComponentStatus.
   * Helpful for obtaining diagnostics regarding the registration, and for invoking diagnostic operations
   * via JMX like pausing a consumer.
   */
  public DatabusComponentStatus getStatus();

  /**
   * Not exposing an API to expose getFilterConfig as Espresso is inherently partitioned
   * public DbusKeyCompositeFilterConfig getFilterConfig();
   */

  /**
   * Obtains a logger used by databus for logging messages associated with this registration
   */
  public Logger getLogger();


  /**
   *  @note WARNING: For Databus internal use only
   *  @deprecated
   */
  @Deprecated
  public DatabusV3Registration getParentRegistration();

  /**
   * API for building Registration with client-application defined regId.  The
   * ComponentStatus object will be recreated when regId changes.  Hence it is
   * the responsibility for the client application to use the new ComponentStatus
   * {@link #getStatus()} after calling this API.
   *
   * @param regId New Registration Id to be set.
   * @return this instance after adding regId
   * @throws DatabusClientException if the regId is already being used.
   * @throws IllegalStateException if the registration has already started.
   */
  public DatabusV3Registration withRegId(RegistrationId regId)
      throws DatabusClientException, IllegalStateException;

  /**
   * Unlike DatabusRegistration interface, not exposing an API to build a registration
   * with server-side filter as Espresso is inherently partitioned
   *
   * public DatabusV3Registration withServerSideFilter(DbusKeyCompositeFilterConfig filterConfig)
   * throws IllegalStateException;
   */

  /**
   * Returns the partition that this registration serves.
   *
   * @return Collection<PhysicalPartition> A collection of PhysicalPartition objects
   */
  public Collection<PhysicalPartition> getPhysicalPartitions();

  /**
   * Returns the last persisted checkpoint.  It is a copy of the actual checkpoint, so
   * changing this will not alter the actual checkpoint used by Databus Client.
   *
   * @param physicalPartition The partition for which the checkpoint has to be fetched. If only one partition is
   *                          associated with this registration, a null or the actual PhysicalPartition should be passed.
   * @throws DatabusClientException if it violates the following
   * For client load balanced scenario: The physicalPartition must be one of the partitions hosted in this client instance.
   * For any other scenario: The physicalPartition must be one of the partitions that the client has subscribed to.
   */
  public Checkpoint getLastPersistedCheckpoint(PhysicalPartition physicalPartition)
  throws DatabusClientException;

  /**
   * Allow the client application to set a checkpoint for a particular physical partition.
   *
   * @param ckpt The checkpoint to be set
   * @param physicalPartition The particular partition for which the checkpoint is set.
   * @throws IllegalStateException If the registration is not in REGISTERED state.
   * @throws DatabusClientException if it violates the following
   * For client load balanced scenario: The physicalPartition must be one of the partitions hosted in this client instance.
   * For any other scenario: The physicalPartition must be one of the partitions that the client has subscribed to.
   */
  public boolean storeCheckpoint(PhysicalPartition physicalPartition, Checkpoint ckpt)
  throws DatabusClientException, IllegalStateException;

  //TODO deprecate this and use storeCheckPoint and getCheckPoint with Partition information instead
  public CheckpointPersistenceProvider getCheckpointPersistenceProvider();

  /**
   * This method will be deprecated in a future release.
   * @deprecated Please use {@link #getCheckpointPersistenceProvider() instead}
   */
  @Deprecated
  public CheckpointPersistenceProvider getCheckpoint();

  /**
   * Obtains the inbound relay event statistics for the registration
   */
  public DbusEventsStatisticsCollectorMBean getRelayEventStats();

  /**
   * Obtains the inbound bootstrap event statistics
   */
  public DbusEventsStatisticsCollectorMBean getBootstrapEventStats();

  /**
   * Obtain statistics for the callbacks for relay events
   */
  public ConsumerCallbackStatsMBean getRelayCallbackStats();

  /**
   * Obtain statistics for the callbacks for bootstrap events
   */
  public ConsumerCallbackStatsMBean getBootstrapCallbackStats();

  /**
   * Returns unified relay/bootstrap statistics for client callbacks
   */
  public UnifiedClientStatsMBean getUnifiedClientStats();

  /**
   * @note WARNING: For Databus internal use only
   * Applicable for Espresso internal replication only.
   *
   * Fetch the most recent sequence number across all relays.
   *
   * @param FetchMaxSCNRequest : Request params for fetchMaxSCN
   * @return RelayFindMaxSCNResult : Contains the result of the fetch call and some useful meta information
   *                                 like minScn / maxScn across all the relays
   */
  public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
      throws InterruptedException;


  /**
   * @note WARNING: For Databus internal use only
   * Applicable for Espresso internal replication only.
   *
   * Flush from available relays to be able to catch up with the highest SCN.
   *
   * Makes the Client point to the relays (specified in RelayFindMaxSCNResult) with the most recent sequence
   * number and waits (with timeout) for the consumer callback to reach this SCN. This is a
   * bounded blocking call. It will wait for timeout milliseconds for the consumer
   * callback to reach the maxScn before returning from this method
   *
   * @param fetchSCNResult : FetchMaxScn result object.
   * @param flushRequest : Request params for flush.
   */
  public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult, FlushRequest flushRequest)
      throws InterruptedException;

  /**
   * @note WARNING: For Databus internal use only
   * Applicable for Espresso internal replication only.
   *
   * Discovers the most recent sequence number across all relays for the given subscriptions and uses flush
   * on the relay with that max SCN. This is a variant of the API call:
   * RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult, FlushRequest flushRequest)
   *
   * @param FetchMaxSCNRequest : Request params for fetchMaxSCN.
   * @param flushRequest : Request params for flush.
   */
  public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest, FlushRequest flushRequest)
      throws InterruptedException;

  /**
   * Retrieves the underlying registration id.
   *
   * Every registration (performed through a registerXXX call {@link DatabusHttpV3ClientImpl})
   * is associated with a unique id.
   */
  public RegistrationId getRegistrationId();

  /**
   * @deprecated Please use {@link #getRegistrationId() instead}
   * @return
   */
  @Deprecated
  public RegistrationId getId();

  /**
   * The API methods below are not present in DatabusRegistration
   */

  /**
   *  Obtains the (Espresso) database name corresponding to this registration.
   *
   *  A single registration cannot span more than one Espresso database. This method returns
   *  the name of that unique database
   *
   *  @throws IllegalStateException If this method is invoked on a parent registration ( which may have more than one database )
   *
   */
  public String getDBName()
  throws IllegalStateException;

  /**
   * Sets a listener interface for the client to get notified a partition is added / removed
   * Two use-cases are described below:
   *
   * Use-case 1: Manipulating a checkpoint for a partition
   * - The client creates a concrete implementation for DbusPartitionListener
   * - During the registration, it indicates that it wants to get notified before receiving events from a partition
   * - It invokes the registerCluster API as follows:
   *   <pre>
   *   {@code
   *   registerCluster(clientClusterName, consumerFactory, source).withDbusV3PartitionListener(dbusV3PartitionListener);
   *   }
   *   </pre>
   * - After databus client gets notified by helix about partition assignment, the following callback is invoked
   *   for the application
   *   <pre>
   *   {@code
   *   dbusPartitionListener.onAddPartition(physicalPartition, databusV3Registration);
   *   }
   *   </pre>
   * - The checkpoint for the physicalPartition may be changed by invoking an api on child registration
   *   databusV3Registration as follows
   *   <pre>
   *   {@code
   *   databusV3Registration.storeCheckpoint(Checkpoint)
   *   }
   *   </pre>
   *
   * Use-case 2: Logging on the client
   * - The client is notified before a partition is added/dropped
   * - The physical partition and the child databus registration are given in the callback which may be logged
   *   for informational purposes
   *
   */
  public void withDbusV3PartitionListener(DbusV3PartitionListener dbus3PartitionListener)
  throws IllegalStateException;

}
