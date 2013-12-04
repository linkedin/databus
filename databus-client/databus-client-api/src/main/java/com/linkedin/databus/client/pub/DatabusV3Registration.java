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


import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;

/**
 * A "registration" represents an identifier for a set of consumer(s) interested in
 * subscription(s). When we say a "registration object" is returned, we mean an object of
 * DatabusV3Registration.
 *
 * The user of the client library can specify a "RegistrationId" for the registration.
 * If the "id" is available, it is used for the returned DatabusV3Registration object. The library
 * currently assigns the same ID upon a restart.
 *
 * The persisted checkpoint for this particular consumerGroup is identified
 * through the RegistrationId, which is a hash generated off the consumer subscriptions.
 */

public interface DatabusV3Registration {

  /**
   * Initiates traffic consumption. After a consumer is registered, and until this call is invoked no
   * events will be seen
   *
   * <pre>
   * The following functionality is provided :
   * 1. Initiate a connection to the respective relay
   * 2. Obtains streaming data from the relay
   * 3. Events are delivered to the consumer
   * </pre>
   */
  public StartResult start();

  /**
   * Returns the internal registration state of the object
   *
   * An object of DatabusV3Registration obeys a state-machine with states such as CREATED,
   * STARTED, DEREGISTERED. The transitions happen on invocations of API calls within this interface
   *
   * <pre>
   * For example, a new instance is created in "CREATED" state
   * From CREATED state, it can go to STARTED state, when a start() method is called
   *                   , it can go to DEREGISTER state, when deregister() is called
   * From STARTED state, it can only go to DEREGISTERED state, when deregister() is called
   * From DEREGISTER state, it can only go to STARTED state, when a start() method is called
   * </pre>
   */
  public RegistrationState getState();

  /**
   *  Obtains the (Espresso) database name corresponding to this registration.
   *
   *  A single registration cannot span more than one Espresso database. This method returns
   *  the name of that unique database
   */
  public String getDBName();

  /**
   * Retrieves the underlying registration id.
   *
   * Every registration (performed through a registerXXX call {@link DatabusHttpV3ClientImpl})
   * is associated with a unique id.
   */
  public RegistrationId getId();

  /**
   * Obtains the parent registration if any. Parent registrations are generally creating when consuming from multiple
   * partitions simultaneously.
   * @return the parent registration or null if there isn't any*/
  public DatabusV3Registration getParentRegistration();

  /**
   * Fetch the most recent sequence number across all relays.
   *
   * Applicable for Espresso internal replication only.
   *
   * @param FetchMaxSCNRequest : Request params for fetchMaxSCN
   * @return RelayFindMaxSCNResult : Contains the result of the fetch call and some useful meta information
   *                                 like minScn / maxScn across all the relays
   */
  public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
      throws InterruptedException;


  /**
   * Flush from available relays to be able to catch up with the highest SCN.
   *
   * Applicable for Espresso internal replication only.
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
   * Discovers the most recent sequence number across all relays for the given subscriptions and uses flush
   * on the relay with that max SCN. This is a variant of the API call:
   * RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult, FlushRequest flushRequest)
   *
   * Applicable for Espresso internal replication only
   *
   * @param FetchMaxSCNRequest : Request params for fetchMaxSCN.
   * @param flushRequest : Request params for flush.
   */
  public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest, FlushRequest flushRequest)
      throws InterruptedException;


  /**
   * <pre>
   * Stop the data processing of the consumer(s) on the registration. Specifically, it does the following.
   * 1. Shuts down the connections from the relay puller to the relay servers.
   * 2. Shuts down the dispatcher state-machine ( and thereby the consumer-side event processing loop ).
   *
   * If this is the only registration for which the pull threads, then a shutdown is invoked on the
   * underlying serverContainer.
   * </pre>
   *
   * @return DeregisterResult
   */
  public DeregisterResult deregister();

  /**
   * Add subscriptions to a given registration object. Adding an already existent subscription,
   * will be a no-op.
   *
   * Currently unimplemented.
   *
   * This does not create a new the DatabusEspressoRegistration object, but only modifies the existing
   * object. Hence the registration id remains the same.
   *
   * The checkpoint for a given registration is created based on the RegistrationId. This has a 8-byte hash
   * generated off of subscriptions. Changing a registration, will require changing the checkpoint directory
   * location.
   *
   * @subs The list of subscriptions to add
   */
  public void addSubscriptions(DatabusSubscription ... subs)
      throws DatabusClientException;

  /**
   * Remove subscriptions from a given registration object. Removing a non-existent subscription,
   * will be a no-op.
   *
   * Currently unimplemented.
   *
   * This does not create a new the DatabusEspressoRegistration object ( only modifies the current one ).
   * Hence the registration id remains the same.
   *
   * The checkpoint for a given registration is created based on the RegistrationId. This has a 8-byte hash
   * generated off of subscriptions. Changing a registration, will require changing the checkpoint directory
   * location.
   *
   * @subs The list of subscriptions to remove.
   */

  public void removeSubscriptions(DatabusSubscription ... subs)
      throws DatabusClientException;

  /**
   * Obtains a list of all subscriptions associated with this registration.
   */
  public List<DatabusSubscription> getSubscriptions();

  /**
   * Adds the specified consumers associated with this registration.
   * The added consumers will have the same subscription(s) and filter parameters as the other consumers
   * associated with this registration
   *
   * Currently unimplemented
   *
   * TBD : Any requirements, on if we should make the consumer start from a specified sequence number ?
   */
  public void addDatabusConsumers(DatabusV3Consumer[] consumers);

  /**
   * Removes the specified consumers associated with this registration
   * Closes the connections associated with each of the consumers
   *
   * Currently unimplemented
   *
   * TBD : Any guarantees, on if we should make the consumer reach a specified sequence number ?
   */
  public void removeDatabusConsumers(DatabusV3Consumer[] consumers);

  /**
   * Returns an object that implements DatabusComponentStatus.
   * Helpful for obtaining diagnostics regarding the registration, and for invoking dignostic operations
   * via JMX like pausing a consumer.
   */
  public DatabusComponentStatus getStatus();

  /**
   * Exposes functionality to read / write a checkpoint with a checkpoint provider object.
   *
   * A typical example of how checkpoint is manipulated on the client library to start from newScn
   * {@code
     CheckpointPersistenceProvider ckp = reg.getCheckpoint();
     Checkpoint cp = Checkpoint.createOnlineConsumption(newScn);
     ckp.storeCheckpointV3(getSubscriptions(), cp, getId());
     }
   */
  public CheckpointPersistenceProvider getCheckpointPersistenceProvider();

  /**
   * This method will be deprecated in a future release.
   * @deprecated Please use {@link #getCheckpointPersistenceProvider() instead}
   */
  @Deprecated
  public CheckpointPersistenceProvider getCheckpoint();

  /**
   * Obtains a logger used by databus for logging messages associated with this registration
   */
  public Logger getLogger();

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
}
