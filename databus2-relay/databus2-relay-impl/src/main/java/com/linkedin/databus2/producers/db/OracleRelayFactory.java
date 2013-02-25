package com.linkedin.databus2.producers.db;
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
