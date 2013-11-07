package com.linkedin.databus.client;
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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusSourcesConnection.StaticConfig;
import com.linkedin.databus.client.consumer.MultiConsumerCallback;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.data_model.DatabusSubscription;

public class BootstrapDispatcher extends GenericDispatcher<DatabusCombinedConsumer>
{
  public static final String MODULE = BootstrapDispatcher.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final RelayPullThread _relayPuller;
  private Checkpoint _lastCkpt;
  private DbusClientMode _bootstrapMode;

  public BootstrapDispatcher(String name,
                             StaticConfig connConfig,
                             List<DatabusSubscription> subs,
                             CheckpointPersistenceProvider checkpointPersistor,
                             DbusEventBuffer dataEventsBuffer,
                             MultiConsumerCallback asyncCallback,
                             RelayPullThread relayPuller,
                             MBeanServer mbeanServer,
                             DatabusHttpClientImpl serverHandle,
                             RegistrationId registrationId)
  {
    super(name, connConfig, subs, checkpointPersistor, dataEventsBuffer, asyncCallback,mbeanServer,serverHandle, registrationId, connConfig.getBstDispatcherRetries());
    _relayPuller = relayPuller;
  }

  @Override
  protected void doStartDispatchEvents()
  {
    _bootstrapMode = DbusClientMode.BOOTSTRAP_SNAPSHOT;
    _lastCkpt = null;
    super.doStartDispatchEvents();
  }

  @Override
  protected boolean processSysEvent(DispatcherState curState, DbusEvent event)
  {
    boolean success = true;
    boolean debugEnabled = getLog().isDebugEnabled();

    Checkpoint ckptInEvent = null;
    int eventSrcId = event.getSourceId();

    if (event.isCheckpointMessage())
    {
      ByteBuffer eventValue = event.value();
      byte[] eventBytes = new byte[eventValue.limit()];
      eventValue.get(eventBytes);

      if (eventValue.limit() > 0)
      {
        try
        {
          String cpString = new String(eventBytes, "UTF-8");
          ckptInEvent = new Checkpoint(cpString);
          _lastCkpt = ckptInEvent;
          getLog().info("bootstrap checkpoint received: " + ckptInEvent);

          _bootstrapMode = _lastCkpt.getConsumptionMode();

          curState.setEventsSeen(true);

          if (_bootstrapMode == DbusClientMode.ONLINE_CONSUMPTION)
          {
            getLog().info("bootstrap done");
            Checkpoint restartCkpt = _lastCkpt.clone();
            _relayPuller.enqueueMessage(
                BootstrapResultMessage.createBootstrapCompleteMessage(restartCkpt));
          }
        }
        catch (RuntimeException e )
        {
          getLog().error("checkpoint deserialization failed", e);
          success = false;
        }
        catch (IOException e )
        {
          getLog().error("checkpoint deserialization failed", e);
          success = false;
        }
      }
      else
      {
        getLog().error("Missing checkpoint in control message");
        success = false;
      }
    }
    else
    {
      if (debugEnabled) getLog().debug(getName() + ": control srcid:" + eventSrcId);
      success = super.processSysEvent(curState, event);
    }

    return success;
  }

  @Override
  protected Checkpoint createCheckpoint(DispatcherState curState, DbusEvent event)
  {
    if (null != curState.getCurrentSource())
    {
      if (null == _lastCkpt)
      {
        throw new DatabusRuntimeException("unable to create a checkpoint");
        //NOTE: we cannot create a checkpoint here as we don't know the sinceSCN, bootstrap stage, etc.
      }

      _lastCkpt.onEvent(event);

    }

    return _lastCkpt;
  }

}
