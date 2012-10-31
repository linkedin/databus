package com.linkedin.databus.bootstrap.producer;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.util.ConfigLoader;

public class BootstrapProducerStartable implements Startable, Shutdownable
{
  public final String MODULE = BootstrapProducerStartable.class.getName();
  public final Logger LOG = Logger.getLogger(MODULE);

  private final BootstrapProducerStaticConfig _staticConfig;
  private final DatabusBootstrapProducer _bootstrapProducer;

  public BootstrapProducerStartable(Properties defaultProps, Properties overrideProps) throws Exception
  {
    LOG.info("default config properties: " + defaultProps + "; \noverride properties:" + overrideProps);

    Properties props = new Properties();
    for(Map.Entry<Object, Object> entry : defaultProps.entrySet())
    {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }
    for(Map.Entry<Object, Object> entry : overrideProps.entrySet())
    {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }

    BootstrapProducerConfig configBuilder = initConfigBuilder(props);
    _staticConfig = configBuilder.build();
    LOG.info("BootstrapProducerReadOnlyConfig =" + _staticConfig);
    _bootstrapProducer = new DatabusBootstrapProducer(_staticConfig);
  }

  public BootstrapProducerStartable(Properties properties) throws Exception
  {
    LOG.info("config properties" + properties);

    BootstrapProducerConfig configBuilder = initConfigBuilder(properties);
    _staticConfig = configBuilder.build();
    LOG.info("BootstrapReadOnlyConfig =" + _staticConfig);
    _bootstrapProducer = new DatabusBootstrapProducer(_staticConfig);
  }

  public BootstrapProducerConfig initConfigBuilder(Properties properties) throws Exception
  {

    BootstrapProducerConfig staticConfigBuilder = new BootstrapProducerConfig();
    ConfigLoader<BootstrapProducerStaticConfig> staticConfigLoader =
        new ConfigLoader<BootstrapProducerStaticConfig>("databus.bootstrap.producer.",
            staticConfigBuilder);
    staticConfigLoader.loadConfig(properties);

    return staticConfigBuilder;
  }


  @Override
  public void shutdown()
  {
    LOG.info("Shutdown request");
    _bootstrapProducer.shutdown();
    LOG.info("Bootstrap producer is shutdown.");
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    LOG.info("Waiting for shutdown");
    _bootstrapProducer.awaitShutdown();
    LOG.info("Bootstrap producer is shutdown.");
  }

  @Override
  public void waitForShutdown(long arg0) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    // TODO Auto-generated method stub
    _bootstrapProducer.awaitShutdown();
  }

  @Override
  public void start() throws Exception
  {
    LOG.info("Starting bootstrap producer");
    _bootstrapProducer.start();
  }

  public DatabusBootstrapProducer getBootstrapProducer()
  {
    return _bootstrapProducer;
  }

}
