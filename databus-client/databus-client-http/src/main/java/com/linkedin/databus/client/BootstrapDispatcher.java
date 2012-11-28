package com.linkedin.databus.client;

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
                             MultiConsumerCallback<DatabusCombinedConsumer> asyncCallback,
                             RelayPullThread relayPuller,
                             MBeanServer mbeanServer,
                             DatabusHttpClientImpl serverHandle,
                             RegistrationId registrationId)
  {
    super(name, connConfig, subs, checkpointPersistor, dataEventsBuffer, asyncCallback,mbeanServer,serverHandle, registrationId);
    _relayPuller = relayPuller;
  }

  @Override
  protected void doStartDispatchEvents(DispatcherState curState)
  {
    _bootstrapMode = DbusClientMode.BOOTSTRAP_SNAPSHOT;
    _lastCkpt = null;
    super.doStartDispatchEvents(curState);
  }

  @Override
  protected boolean processSysEvent(DispatcherState curState, DbusEvent event)
  {
    boolean success = true;
    boolean debugEnabled = LOG.isDebugEnabled();

    Checkpoint ckptInEvent = null;
    short eventSrcId = event.srcId();

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
          _bootstrapMode = _lastCkpt.getConsumptionMode();
          LOG.info(getName() + ": boostrap mode: " + _bootstrapMode.toString());

          curState.setEventsSeen(true);

          if (_bootstrapMode == DbusClientMode.ONLINE_CONSUMPTION)
          {
            LOG.info(getName() + ": bootstrap done");
            _relayPuller.enqueueMessage(
                BootstrapResultMessage.createBootstrapCompleteMessage(_lastCkpt));
          }
        }
        catch (RuntimeException e )
        {
          LOG.error(getName() + ": checkpoint deserialization failed", e);
          success = false;
        }
        catch (IOException e )
        {
          LOG.error(getName() + ": checkpoint deserialization failed", e);
          success = false;
        }
      }
      else
      {
        LOG.error("Missing checkpoint in control message");
        success = false;
      }
    }
    else
    {
      if (debugEnabled) LOG.debug(getName() + ": control srcid:" + eventSrcId);
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
        _lastCkpt = new Checkpoint();
        _lastCkpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
        _lastCkpt.setSnapshotSource(curState.getCurrentSource().getName());
        _lastCkpt.startSnapShotSource();
        _lastCkpt.setSnapshotOffset(0);
      }

      _lastCkpt.onEvent(event);

    }

    return _lastCkpt;
  }

}
