package com.linkedin.databus2.v1_adapter;

import java.io.IOException;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.Databus;
import com.linkedin.databus.core.DatabusConnectionPushImpl;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;

public class V1SourceRelay extends HttpRelay
{
  private final PhysicalSource _physSource;
  private final PhysicalPartition _physPart;
  private final V1EventCallback _v1callback;
  private final V1SourceEventFactoryFactory _v1eventFactoryFactory;
  private final DatabusConnectionPushImpl<Object, Object> _connPush;

  public V1SourceRelay(StaticConfig config, PhysicalSourceStaticConfig[] pConfigs,
                       Databus<Object, Object> databus)
         throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
    PhysicalSourceStaticConfig physConfig = getPhysicalSources().get(0);
    _physSource = new PhysicalSource(physConfig.getUri());
    _physPart = new PhysicalPartition(physConfig.getId(), physConfig.getName());
    DbusEventBuffer buf = getEventBuffer().getOneBuffer(_physPart);
    try
    {
      _v1callback = new V1EventCallback(buf,
                                        getInboundEventStatisticsCollector());
      _v1eventFactoryFactory =
          new V1SourceEventFactoryFactory(physConfig,
                                          getSchemaRegistryService(),
                                          buf,
                                          getInboundEventStatisticsCollector());
      _connPush = null;
      //_connPush = new DatabusConnectionPushImpl<Object, Object>(_oraEventsMonitorFactory, maxSCNReader, sources, _v1callback, timeout, isFirstUpdateSynchronous, name, errRecoveryTimespan, maxNumConsecutiveExceptions, postExceptionDelaySeedTimespan, maxStartupErrRecoveryTimespan, waitForShutdownTimespan, false);
    }
    catch (NoSuchSchemaException e)
    {
      throw new DatabusException(e);
    }
    catch (EventCreationException e)
    {
      throw new DatabusException(e);
    }
    catch (UnsupportedKeyException e)
    {
      throw new DatabusException(e);
    }
  }

}
