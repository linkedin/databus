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
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

/*
 * The test verifies if the client will be able to connect to the multi-tenant relay
 */
public class TestMultitenantRelay
{

  DatabusHttpClientImpl.Config clientConfBuilder;
  @BeforeClass
  public void setUp() throws InvalidConfigException
  {
    Properties clientProps = new Properties();
    clientProps.setProperty("client.runtime.relay(1).name", "relay");
    clientProps.setProperty("client.runtime.relay(1).port", "10001");
    clientProps.setProperty("client.runtime.relay(1).sources", "source1");
    clientProps.setProperty("client.runtime.relay(2).name", "relay");
    clientProps.setProperty("client.runtime.relay(2).port", "10001");
    clientProps.setProperty("client.runtime.relay(2).sources", "source2");
    clientProps.setProperty("client.runtime.relay(3).name", "relay2");
    clientProps.setProperty("client.runtime.relay(3).port", "10002");
    clientProps.setProperty("client.runtime.relay(3).sources", "source3");

    clientConfBuilder = new DatabusHttpClientImpl.Config();
    ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("client.", clientConfBuilder);
    configLoader.loadConfig(clientProps);
  }

  @Test
  public void relayMergeCheck() throws IOException, DatabusException
  {
    Assert.assertEquals(clientConfBuilder.getRuntime().getRelays().size(),3);
    DatabusHttpClientImpl.StaticConfig clientConf = clientConfBuilder.build();
    Assert.assertEquals(clientConf.getRuntime().getRelays().size(), 3);
    DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConf);
    Assert.assertEquals(client.getRelayGroups().size(),3);
  }

}
