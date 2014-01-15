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

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStatsMBean;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;


/**
 * Base interface for Registration, which is the databus-client handle for the client application
 * to handle the consumer callbacks that were registered in a single register() call.
 */
public interface DatabusRegistration
{
  public enum RegistrationState
  {
    /**
     * Initialization state. Dbus Client library has not set up registration for its consumers yet.
     */
    INIT,
    /**
     * Consumers have been registered but consumption has not yet started.
     */
    REGISTERED,
    /**
     * Consumption has started.
     */
    STARTED,
    /**
     * Consumption is paused.
     */
    PAUSED,
    /**
     * Consumption is resumed.
     */
    RESUMED,
    /**
     * Consumption is suspended because of error.
     */
    SUSPENDED_ON_ERROR,
    /**
     * Consumption is shut down.
     */
    SHUTDOWN,
    /**
     * Client library is unregistered and removed from client's internal data structures.
     */
    DEREGISTERED;

    /**
     * @return true if consumption has not yet started
     */
    public boolean isPreStartState()
    {
      switch(this)
      {
        case INIT :
        case REGISTERED :
          return true;
        default:
          return false;
      }
    }

    /**
     * @return true if consumption has completed
     */
    public boolean isPostRunState()
    {
      switch(this)
      {
        case SHUTDOWN:
        case DEREGISTERED:
          return true;
        default:
          return false;
      }
    }

    /**
     * @return true if consumption is actively running
     */
    public boolean isRunning()
    {
      switch (this)
      {
        case STARTED:
        case PAUSED:
        case SUSPENDED_ON_ERROR:
        case RESUMED:
          return true;
        default:
          return false;
      }
    }

    /**
     * @return true if registration is actively maintained in the client library
     */
    public boolean isActiveRegistration()
    {
      switch (this)
      {
        case REGISTERED:
        case STARTED:
        case PAUSED:
        case RESUMED:
        case SUSPENDED_ON_ERROR:
        case SHUTDOWN:
          return true;
        default:
          return false;
      }
    }
  };

  /**
   * This API should be called when the application is ready to start consuming events.
   * The methods defined in the ConsumerCallbacks registered will be invoked (in a separate
   * thread) as the events are pulled.  These callbacks will continue to be called until
   * the application calls pause() or shutdown() methods.
   *
   * @throws IllegalStateException if the registration is not in REGISTERED state.
   * @throws DatabusClientException
   *    if there are no subscriptions or callbacks registered, or
   *    if this registration cannot service the sources/subscriptions together.
   *
   * @return false if the client is already started, else true.
   */
  public boolean start() throws IllegalStateException, DatabusClientException;

  /**
   * This API should be called to shut down consumption for this registration.  At the
   * completion of this call, all threads pulling events and invoking the callbacks will
   * be stopped and no more callback for this registration called.  A registration that
   * is shut down can still be inspected by client REST API.
   *
   * @throws IllegalStateException if the registration is not running.
   */
  public void shutdown() throws IllegalStateException;

  /**
   * This API should be called to pause consumption for this registration.  This is an
   * asynchronous pause made to the component which pulls the events.  If there are
   * outstanding events in the client buffer, they will be dispatched to the consumer
   * callbacks until the buffer becomes empty.  The consumption can be paused for arbitrary
   * length of time.  The registration can be shut down when the registration is paused.
   *
   * @throws IllegalStateException if the registration is not running
   */
  public void pause() throws IllegalStateException;

  /**
   * API used to suspend consumption for this registration.  This is similar to the pause
   * behavior and is asynchronous in nature.  Client applications are expected to use this
   * API when an error/exception condition causes the client application to suspend the
   * consumption of events.  The consumption can be suspended for arbitrary length of time.
   * The registration can be shut down when the registration is suspended.
   *
   * @param ex Throwable for the exceptional/error condition.
   * @throws IllegalStateException if the registration is not running.
   */
  public void suspendOnError(Throwable ex) throws IllegalStateException;

  /**
   * API used to resume consumption for this registration.  This is also an asynchronous
   * call which resumes the component which pulls events from the relay.  This can be
   * called when the consumption is paused/suspended.
   *
   * @throws IllegalStateException if the registration is not running.
   */
  public void resume() throws IllegalStateException;

  /**
   * Returns the current state of registration.
   */
  public RegistrationState getState();

  /**
   * De-registers this registration from the client library.  If running, this API shuts
   * down the registration before de-registering.  At the completion of this call all the
   * state maintained in the client library for this registration will be cleaned up.
   *
   * @return false if this was already deregistered or cannot be found, else true
   * @throws IllegalStateException if the registration is not in REGISTERED state
   */
  public boolean deregister() throws IllegalStateException;

  /**
   * Obtains a cloned Collection of all subscriptions associated with this registration.
   * Changing this subscription will not have any effect on the registration.
   */
  public Collection<DatabusSubscription> getSubscriptions();

  /**
   * Returns an object that implements DatabusComponentStatus. This is helpful for
   * obtaining diagnostics regarding the registration and also to pause/resume
   * consumption for this registration through JMX.
   */
  public DatabusComponentStatus getStatus();

  /** Filter Config */
  public DbusKeyCompositeFilterConfig getFilterConfig();

  /** Obtains a logger used by databus for logging messages associated with this registration */
  public Logger getLogger();

  /** Parent Registration if this is part of MultiPartitionRegistration */
  public DatabusRegistration getParent();

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
  public DatabusRegistration withRegId(RegistrationId regId)
      throws DatabusClientException, IllegalStateException;

  /**
   * API for building Registration with server-side filtering.
   *
   * @param filterConfig
   * @return this instance after adding server-side filter
   * @throws IllegalStateException if the registration has already started.
   */
  public DatabusRegistration withServerSideFilter(DbusKeyCompositeFilterConfig filterConfig)
      throws IllegalStateException;

  /**
   * For non-partitioned consumers, returns null.  For source-partitioned or
   * load-balanced registration, returns the partition that this registration
   * serves.
   *
   * @return partitionId
   */
  public Collection<DbusPartitionInfo> getPartitions();

  /**
   * Last Persisted checkpoint.  This is a copy of the actual checkpoint, so
   * changing this will not alter the Databus client's checkpoint.
   */
  public Checkpoint getLastPersistedCheckpoint();

  /**
   * This API allows the client application to dictate the SCN from which it wants
   * the Databus client library to start.  This API can be invoked only when the
   * registration has not been started.
   *
   * @throws IllegalStateException if the Registration state is not in either INIT
   *                               or REGISTERED state
   */
  public boolean storeCheckpoint(Checkpoint ckpt) throws IllegalStateException;

  /** Obtains the inbound relay event statistics for the registration */
  public DbusEventsStatisticsCollectorMBean getRelayEventStats();

  /** Obtains the inbound bootstrap event statistics */
  public DbusEventsStatisticsCollectorMBean getBootstrapEventStats();

  /** Obtains statistics for the callbacks for relay events */
  public ConsumerCallbackStatsMBean getRelayCallbackStats();

  /** Obtains statistics for the callbacks for bootstrap events */
  public ConsumerCallbackStatsMBean getBootstrapCallbackStats();

  /** Returns unified relay/bootstrap statistics for client callbacks */
  public UnifiedClientStatsMBean getUnifiedClientStats();

  /**
   * Fetch the most recent sequence number across all relays.
   * <B>Note:  Not currently supported.</B>
   *
   * @param FetchMaxSCNRequest  Request params for fetchMaxSCN
   * @return RelayFindMaxSCNResult
   */
  public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
      throws InterruptedException;

  /**
   * Makes the Client point to the relays (specified in RelayFindMaxSCNResult) with
   * the most recent sequence number and waits (with timeout) for the consumer callback
   * to reach this SCN.  This is a bounded blocking call.  It will wait for timeout
   * milliseconds for the consumer callback to reach the maxScn before returning from
   * this method.
   * <B>Note:  Not currently supported.</B>
   *
   * @param fetchSCNResult  FetchMaxScn result object.
   * @param flushRequest  Request params for flush.
   */
  public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult, FlushRequest flushRequest)
      throws InterruptedException;

  /**
   * Discovers the most recent sequence number across all relays for the given
   * subscriptions and uses flush on the relay with that max SCN.
   * <B>Note:  Not currently supported.</B>
   *
   * @param FetchMaxSCNRequest  Request params for fetchMaxSCN.
   * @param flushRequest  Request params for flush.
   */
  public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest, FlushRequest flushRequest)
      throws InterruptedException;

  /**
   * @return  registration ID for this registration
   */
  public RegistrationId getRegistrationId();

}
