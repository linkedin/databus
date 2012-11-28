package com.linkedin.databus3.espresso.li;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus3.espresso.EspressoRelay;
import com.linkedin.databus3.espresso.EspressoRelayFactory;

public class EspressoRelayStartable implements Startable, Shutdownable
{
  public static final Logger LOG = Logger.getLogger(EspressoRelayStartable.class.getName());

  EspressoRelay _relayInstance;
  EspressoRelayFactory _relayFactory;
  private final EspressoRelay.StaticConfigBuilder _config;

  public EspressoRelayStartable(Properties properties, Properties override)
         throws IOException, DatabusException
  {
    Properties props = new Properties();
    for(Map.Entry<Object, Object> entry : properties.entrySet())
    {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }
    for(Map.Entry<Object, Object> entry : override.entrySet())
    {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }

    LOG.info("properties: " + properties + "; override: " + override + "; merged props: " + props);

    // Load the configuration
    _config = new EspressoRelay.StaticConfigBuilder();
    ConfigLoader<EspressoRelay.StaticConfig> staticConfigLoader =
        new ConfigLoader<EspressoRelay.StaticConfig>("databus.relay.", _config);
    staticConfigLoader.loadConfig(props);

    LOG.info("creating espresso relay");
    _relayFactory = new EspressoRelayFactory(_config, null);
    _relayInstance = (EspressoRelay)_relayFactory.createRelay();
    LOG.info("initializing espresso relay");
    LOG.info("espresso relay initialized");
  }

  @Override
  public void shutdown()
  {
    LOG.info("relay shutdown requested");
    _relayInstance.shutdown();
    LOG.info("relay is shutdown.");
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    LOG.info("Waiting for relay shutdown ...");
    _relayInstance.awaitShutdown();
    LOG.info("Relay is shutdown!");
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    LOG.info("Waiting for relay shutdown ...");
    try
    {
      _relayInstance.awaitShutdown(timeout);
    }
    catch (TimeoutException te)
    {
      throw new TimeOutException(te.getMessage());
    }
    LOG.info("Relay is shutdown!");
  }

  @Override
  public void start() throws Exception
  {
    LOG.info("starting relay");
    _relayInstance.start();
    LOG.info("starting relay: done");
  }

  public EspressoRelay getRelay()
  {
    return _relayInstance;
  }

}
