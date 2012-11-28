/**
 * 
 */
package com.linkedin.databus.client.generic;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

/**
 * @author "Balaji Varadarajan<bvaradarajan@linkedin.com>"
 *
 */
public class DatabusV1V2ClientStarter<V1 extends Startable & Shutdownable, V2 extends Startable & Shutdownable> implements Shutdownable, Startable
{
  public static final String MODULE = DatabusV1V2ClientStarter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  @SuppressWarnings("rawtypes")
  private final V1 _v1;
  private final V2 _v2;
  private final boolean _v1Enabled;

  public DatabusV1V2ClientStarter(Config<V1,V2> config)
  {
    _v1 = config.getV1();
    _v2 = config.getV2();
    _v1Enabled = config.isV1Enabled();
  }

  @Override
  public void start() throws Exception
  {
    if (_v1Enabled)
    {
      LOG.info("Starting databus v1 client ...");
      _v1.start();
      LOG.info("databus v1 client started.");
    }
    else
    {
      LOG.info("Starting databus v2 client ... ");
      _v2.start();
      LOG.info("databus v2 client started ...");
    }
  }

  @SuppressWarnings("rawtypes")
  public static class Config<V1 extends Startable & Shutdownable, V2 extends Startable & Shutdownable>
  {
    private V1 _v1;
    private V2 _v2;
    private boolean _v1Enabled = true;
    public V1 getV1()
    {
      return _v1;
    }
    public void setV1(V1 v1)
    {
      _v1 = v1;
    }
    public V2 getV2()
    {
      return _v2;
    }
    public void setV2(V2 v2)
    {
      _v2 = v2;
    }
    public boolean isV1Enabled()
    {
      return _v1Enabled;
    }
    public void setV1Enabled(boolean v1Enabled)
    {
      _v1Enabled = v1Enabled;
    }
  }

  
  @Override
  public void shutdown()
  {
    if (_v1Enabled)
    {
      LOG.info("shutdown requested for databus v1 client ...");
      _v1.shutdown();
      LOG.info("shutdown request for databus v1 client processed.");
    }
    else
    {
      LOG.info("shutdown requested for databus v2 client ...");
      _v2.shutdown();
      LOG.info("shutdown request for databus v2 client processed.");
    }
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    if (_v1Enabled)
    {
      LOG.info("waiting for shutdown for databus v1 client ... ");
      _v1.waitForShutdown();
      LOG.info("databus v1 client shutdown.");
    }
    else
    {
      LOG.info("waiting for shutdown for databus v2 client ...");
      _v2.waitForShutdown();
      LOG.info("databus v2 client shutdown ...");
    }
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    if (_v1Enabled)
    {
      LOG.info("waiting for shutdown for databus v1 client with timeout: " + timeout);
      _v1.waitForShutdown(timeout);
      LOG.info("databus v1 client shutdown.");
    }
    else
    {
      LOG.info("waiting for shutdown for databus v2 client with timeout: " + timeout);
      _v2.waitForShutdown(timeout);
      LOG.info("databus v2 client shutdown.");
    }
  }

}
