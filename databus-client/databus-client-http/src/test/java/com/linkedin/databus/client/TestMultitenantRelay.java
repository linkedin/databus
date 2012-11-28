package com.linkedin.databus.client;

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
