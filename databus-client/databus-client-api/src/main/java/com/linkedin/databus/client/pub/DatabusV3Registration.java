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

public interface DatabusV3Registration {

	/**
	 *
 	 * Start the traffic ( if available ) for this registration.
	 */
	public StartResult start();

	/**
	 *
	 * Get state of registration
	 */
	public RegistrationState getState();

	/**
	 *  Obtains the database name corresponding to this registration
	 */
	public String getDBName();

	/**
	 * Every registration (performed through a registerXXX call @see DatabusEspressoClient ) is associated with a unique id.
	 *
	 * An Id is specified for every registration, during the registerXXX call).
	 * If Id is not specified, the client library will generate one. This can be retrieved by invoking the method below
	 */
	public RegistrationId getId();

	/**
	 * set parent registration id for this registration if it is part of MP registration
	 * @param rid
	 */
	public void setParentRegId(RegistrationId rid);

	/**
   * get parent registration id for this registration if it is part of MP registration
   */
  public RegistrationId getParentRegId();

	/**
	 * Fetch the most recent sequence number across all relays
	 * @param FetchMaxSCNRequest : Request params for fetchMaxSCN
	 * @return RelayFindMaxSCNResult
	 */
	public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
					throws InterruptedException;


	/**
	 *
	 * Makes the Client point to the relays (specified in RelayFindMaxSCNResult) with the most recent sequence number
	 * and waits (with timeout) for the consumer callback to reach this SCN. This is a
	 * bounded blocking call. It will wait for timeout milliseconds for the consumer
	 * callback to reach the maxScn before returning from this method
	 *
	 * @param fetchSCNResult : FetchMaxScn result object.
	 * @param flushRequest : Request params for flush.
	 */
	public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult, FlushRequest flushRequest)
					throws InterruptedException;

	/**
	 *
	 * Discovers the most recent sequence number across all relays for the given subscriptions and uses flush
	 * on the relay with that max SCN.
	 * @param FetchMaxSCNRequest : Request params for fetchMaxSCN.
	 * @param flushRequest : Request params for flush.
	 */
	public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest, FlushRequest flushRequest)
					throws InterruptedException;


	/**
	 *
 	 * De-register from the client library. If this is the only registration for which the pull threads
 	 * are serving then the pipeline will be shutdown.
	 */
	public DeregisterResult deregister();

	/**
	 *
 	 * Add subscriptions to a given registration object
	 * Adding an already existent subscription, will be a no-op.
	 *
	 * This does not create a new the DatabusEspressoRegistration object ( only modifies the current one ).
	 * Hence the id of the registration remains the same
	 *
	 * The checkpoint for a given registration is created based on the RegistrationId. This has a 8-byte hash
	 * generated off of subscriptions. Changing a registration, will require changing the checkpoint directory
	 * location
	 */
	public void addSubscriptions(DatabusSubscription ... subs)
	throws DatabusClientException;

	/**
	 *
 	 * Remove subscriptions from a given registration object
	 * Removing a non-existent subscription, will be a no-op.
	 *
	 * This does not create a new the DatabusEspressoRegistration object ( only modifies the current one ).
	 * Hence the id of the registration remains the same
	 *
 	 * The checkpoint for a given registration is created based on the RegistrationId. This has a 8-byte hash
	 * generated off of subscriptions. Changing a registration, will require changing the checkpoint directory
	 * location
	 */

	public void removeSubscriptions(DatabusSubscription ... subs)
	throws DatabusClientException;

	/**
	 * Obtains a list of all subscriptions associated with this registration
	 */
	public List<DatabusSubscription> getSubscriptions();

	/**
	 *
	 * Adds the specified consumers associated with this registration
	 * The added consumers will have the same subscription(s) and filter parameters as the other consumers
	 * associated with this registration
	 *
	 * TBD : Any requirements, on if we should make the consumer start from a specified sequence number ?
	 */
	public void addDatabusConsumers(DatabusV3Consumer[] consumers);

	/**
	 *
	 * Removes the specified consumers associated with this registration
	 * Closes the connections associated with each of the consumers
	 *
	 * TBD : Any guarantees, on if we should make the consumer reach a specified sequence number ?
	 */
	public void removeDatabusConsumers(DatabusV3Consumer[] consumers);

	/**
	 * Returns an object that implements DatabusComponentStatus
	 * Helpful for obtaining diagnostics regarding the registration
	 * The mapping between a registration and a
	 */
	public DatabusComponentStatus getStatus();

	/**
	 * Returns a checkpoint provider object
	 */
	public CheckpointPersistenceProvider getCheckpoint();

	/** Obtains a logger used by databus for logging messages associated with this registration */
	public Logger getLogger();

	/** Obtains the inbound relay event statistics for the registration */
	public DbusEventsStatisticsCollectorMBean getRelayEventStats();

    /** Obtains the inbound bootstrap event statistics */
    public DbusEventsStatisticsCollectorMBean getBootstrapEventStats();

    /** Obtain statistics for the callbacks for relay events*/
    public ConsumerCallbackStatsMBean getRelayCallbackStats();

    /** Obtain statistics for the callbacks for bootstrap events*/
    public ConsumerCallbackStatsMBean getBootstrapCallbackStats();
}
