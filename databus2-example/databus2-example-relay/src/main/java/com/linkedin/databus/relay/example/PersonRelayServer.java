package com.linkedin.databus.relay.example;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class PersonRelayServer extends HttpRelay {
  public static final String MODULE = PersonRelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  static final String FULLY_QUALIFIED_PROFILE_EVENT_NAME = "com.linkedin.events.example.person.Person";
  static final int MEMBER_PROFILE_SRC_ID = 40;

  public PersonRelayServer() throws IOException, InvalidConfigException, DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public PersonRelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    this(config.build(), pConfigs);
  }

  public PersonRelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Properties startupProps = HttpRelay.processCommandLineArgs(args);
    Config config = new Config();

    config.getContainer().setIdFromName("PersonRelayServer.localhost");

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.relay.", config);

    config.setSourceName(String.valueOf(MEMBER_PROFILE_SRC_ID), FULLY_QUALIFIED_PROFILE_EVENT_NAME);

    HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);
    LOG.info("source = " + staticConfig.getSourceIds());

    PersonRelayServer serverContainer = new PersonRelayServer(staticConfig, null);

    serverContainer.startAndBlock();
  }
}
