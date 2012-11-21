package com.linkedin.databus2.producers.db;

import java.io.IOException;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.util.PhysicalSourceConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.AbstractRelayFactory;
import com.linkedin.databus2.relay.RelayFactory;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.StandardSchemaRegistryFactory;

/** Creates a relay setup for reading updates from an Oracle database*/
public class OracleRelayFactory extends AbstractRelayFactory implements RelayFactory
{
  private final HttpRelay.Config _relayConfigBuilder;
  private final String[] _physicalSourcesConfigFiles;
  private final StandardSchemaRegistryFactory _schemaRegistryFactory;

  public OracleRelayFactory(HttpRelay.Config relayConfigBuilder, String[] physicalSourcesConfigFiles)
  {
    super();
    _relayConfigBuilder = relayConfigBuilder;
    _physicalSourcesConfigFiles = physicalSourcesConfigFiles;
    _schemaRegistryFactory = new StandardSchemaRegistryFactory(relayConfigBuilder.getSchemaRegistry());
  }

  public HttpRelay.Config getRelayConfigBuilder()
  {
    return _relayConfigBuilder;
  }

  @Override
  public HttpRelay createRelay() throws DatabusException
  {
    try
    {
      SchemaRegistryService schemaRegistry = _schemaRegistryFactory.createSchemaRegistry();
      HttpRelay.StaticConfig relayStaticConfig = _relayConfigBuilder.build();
      getSourcesIdNameRegistry().updateFromIdNamePairs(relayStaticConfig.getSourceIds());
      PhysicalSourceConfigBuilder psourceConfBuilder =
          new PhysicalSourceConfigBuilder(_physicalSourcesConfigFiles);
      return new HttpRelay(relayStaticConfig, psourceConfBuilder.build(), getSourcesIdNameRegistry(),
                           schemaRegistry);
    }
    catch (IOException ioe)
    {
      throw new DatabusException("OracleRelay instantiation error: " + ioe.getMessage(), ioe);
    }
  }

}
