package com.linkedin.databus2.producers;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/** Manages {@link RelayEventGenerator} instances. */
public class MockEventProducerServiceProvider implements EventProducerServiceProvider
{
  public static final String SCHEME = "mock";

  @Override
  public String getUriScheme()
  {
    return SCHEME;
  }

  @Override
  public EventProducer createProducer(PhysicalSourceStaticConfig config,
                                      SchemaRegistryService schemaRegistryService,
                                      DbusEventBufferAppendable eventBuffer,
                                      DbusEventsStatisticsCollector statsCollector,
                                      MaxSCNReaderWriter checkpointWriter) throws InvalidConfigException
  {
    return new RelayEventGenerator(config, schemaRegistryService, eventBuffer, statsCollector, checkpointWriter);
  }

  @Override
  public ConfigBuilder<? extends PhysicalSourceStaticConfig> createConfigBuilder(String propPrefix)
  {
    return new PhysicalSourceConfig();
  }

}
