package com.linkedin.databus.bootstrap.server;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.util.ConfigLoader;

public class BootstrapServerStartable implements Startable, Shutdownable
{
  public final String MODULE = BootstrapServerStartable.class.getName();
  public final Logger LOG = Logger.getLogger(MODULE);

  private final BootstrapServerStaticConfig _staticConfig;
  private final BootstrapHttpServer _bootstrapServer;

  public BootstrapServerStartable(Properties properties) throws Exception
  {
	BootstrapServerConfig configBuilder = new BootstrapServerConfig();
    ConfigLoader<BootstrapServerStaticConfig> configLoader =
      new ConfigLoader<BootstrapServerStaticConfig>("databus.bootstrap.server.", configBuilder);
    configLoader.loadConfig(properties);
    _staticConfig = configBuilder.build();

    _bootstrapServer = new BootstrapHttpServer(_staticConfig);
  }

  @Override
  public void shutdown()
  {
    LOG.info("Shutdown requested");
    _bootstrapServer.shutdown();
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    LOG.info("Waiting for shutdown");
    _bootstrapServer.awaitShutdown();
    LOG.info("bootstrap server is shutdown!");
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    //TODO add timeout
    LOG.info("Waiting for shutdown with timeout");
    _bootstrapServer.awaitShutdown();
    LOG.info("bootstrap server is shutdown!");
  }

  @Override
  public void start() throws Exception
  {
    LOG.info("Starting bootstrap server");
    _bootstrapServer.start();
  }

  public BootstrapHttpServer getBootstrapHttpServer()
  {
    return _bootstrapServer;
  }

}
