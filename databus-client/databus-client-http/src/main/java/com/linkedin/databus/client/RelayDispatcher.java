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


import java.util.List;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;
import com.linkedin.databus.client.DatabusSourcesConnection.StaticConfig;
import com.linkedin.databus.client.consumer.MultiConsumerCallback;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventUtils;
import com.linkedin.databus.core.DbusPrettyLogUtils;
import com.linkedin.databus.core.SCNRegressMessage;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus.core.data_model.DatabusSubscription;

public class RelayDispatcher extends GenericDispatcher<DatabusCombinedConsumer>
{
  private final BootstrapPullThread _bootstrapPuller;

  public RelayDispatcher(String name,
                         StaticConfig connConfig,
                         List<DatabusSubscription> subsList,
                         CheckpointPersistenceProvider checkpointPersistor,
                         DbusEventBuffer dataEventsBuffer,
                         MultiConsumerCallback asyncCallback,
                         BootstrapPullThread bootstrapPuller,
                         MBeanServer mbeanServer,
                         DatabusHttpClientImpl serverHandle,
                         RegistrationId registrationId,
                         Logger log)
  {
    super(name, connConfig, subsList, checkpointPersistor, dataEventsBuffer, asyncCallback, mbeanServer,
          serverHandle, registrationId, connConfig.getDispatcherRetries(), log);
    _bootstrapPuller = bootstrapPuller;
  }

  @Override
  protected Checkpoint createCheckpoint(DispatcherState curState, DbusEvent event)
  {
      return createOnlineConsumptionCheckpoint(_lastWindowScn, _lastEowTsNsecs, curState, event);
  }

  @Override
  protected boolean processSysEvent(DispatcherState curState, DbusEvent event)
  {
    boolean success = true;

    if (event.isCheckpointMessage())
    {
      Checkpoint ckpt = null;
      try
      {
        ckpt = DbusEventUtils.getCheckpointFromEvent(event);
        DbusClientMode bootstrapMode = ckpt.getConsumptionMode();

        if (bootstrapMode != DbusClientMode.ONLINE_CONSUMPTION)
        {
          if(_bootstrapPuller == null) {
            _log.error("Checkpoint specifies that the consumer is bootstrapping, but bootstrapPuller is not present (Is bootstrap disabled ?)");
            return false;
          }
          ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
          if (curState.getStateId() != DispatcherState.StateId.EXPECT_EVENT_WINDOW)
          {
            _log.warn("The current state of the dispatcher is NOT as expected (" + DispatcherState.StateId.EXPECT_EVENT_WINDOW.name() + "). State prior to this: " + curState.getStateId().name());
            //Fixing bug that caused TestRelayBootstrapSwitch to fail; no apparent need to rollback
            //curState.switchToRollback();
            //doRollback(curState);
          }
          curState.getEventsIterator().getEventBuffer().clear();
          curState.resetIterators();
          curState.switchToExpectEventWindow();

          _bootstrapPuller.enqueueMessage(LifecycleMessage.createStartMessage());
          _log.info("Switching to bootstrap mode");
        }
        else
        {
          success = super.processSysEvent(curState, event);
        }
      }
      catch (Exception e )
      {
        DbusPrettyLogUtils.logExceptionAtError("Internal error processing a system event", e, _log);
        success = false;
      }
    }
    else if (event.isSCNRegressMessage())
    {
    	SCNRegressMessage message = DbusEventUtils.getSCNRegressFromEvent(event);
    	_log.info("Switching relays, some of the events maybe replayed. The Checkpoint to which the client with regress: " + message);
    	curState.setSCNRegress(true);
      curState.switchToExpectEventWindow();
      //enqueueMessage(curState);
    }
    else
    {
      success = super.processSysEvent(curState, event);
    }

    return success;
  }

}
