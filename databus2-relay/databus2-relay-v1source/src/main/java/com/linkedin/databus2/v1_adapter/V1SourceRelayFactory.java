package com.linkedin.databus2.v1_adapter;

import java.io.IOException;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.PhysicalSourceConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.AbstractRelayFactory;
import com.linkedin.databus2.relay.RelayFactory;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.StandardSchemaRegistryFactory;

public class V1SourceRelayFactory extends AbstractRelayFactory implements RelayFactory
{
  private final HttpRelay.Config _relayConfigBuilder;
  private final StandardSchemaRegistryFactory _schemaRegistryFactory;
  private final PhysicalSourceStaticConfig _physConfig;

  public V1SourceRelayFactory(HttpRelay.Config relayConfigBuilder,
                              PhysicalSourceStaticConfig physConfig)
  {
    super();
    _relayConfigBuilder = relayConfigBuilder;
    _physConfig = physConfig;
    _schemaRegistryFactory = new StandardSchemaRegistryFactory(relayConfigBuilder.getSchemaRegistry());
  }

  public static V1SourceRelayFactory create(HttpRelay.Config relayConfigBuilder,
                                            String physicalSourcesConfigFile)
                                            throws InvalidConfigException
  {
    PhysicalSourceConfigBuilder psourceConfBuilder =
        new PhysicalSourceConfigBuilder(new String[]{physicalSourcesConfigFile});
    PhysicalSourceStaticConfig physConf = psourceConfBuilder.build()[0];
    V1SourceRelayFactory factory = new V1SourceRelayFactory(relayConfigBuilder, physConf);

    return factory;
  }

  @Override
  public HttpRelay createRelay() throws DatabusException
  {
    try
    {
      SchemaRegistryService schemaRegistry = _schemaRegistryFactory.createSchemaRegistry();
      HttpRelay.StaticConfig relayStaticConfig = _relayConfigBuilder.build();
      getSourcesIdNameRegistry().updateFromIdNamePairs(relayStaticConfig.getSourceIds());
      HttpRelay relay = new HttpRelay(relayStaticConfig,
                                      new PhysicalSourceStaticConfig[]{_physConfig},
                                      getSourcesIdNameRegistry(),
                                      schemaRegistry);

      return relay;
    }
    catch (IOException ioe)
    {
      throw new DatabusException("OracleRelay instantiation error: " + ioe.getMessage(), ioe);
    }
  }

}
