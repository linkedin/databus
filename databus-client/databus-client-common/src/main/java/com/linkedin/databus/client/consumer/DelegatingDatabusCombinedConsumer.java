package com.linkedin.databus.client.consumer;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;

/**
 * A {@link DatabusStreamConsumer} that delegates all class to another consumer. This class is
 * meant to overriden with specific implementations of some of the callbacks while it will take
 * care of the rest of the callbacks.
 * */
public abstract class DelegatingDatabusCombinedConsumer extends AbstractDatabusCombinedConsumer
{
  protected final DatabusStreamConsumer _streamDelegate;
  protected final DatabusBootstrapConsumer _bootstrapDelegate;
  protected Logger _log;

  public DelegatingDatabusCombinedConsumer(DatabusStreamConsumer streamDelegate,
                                           DatabusBootstrapConsumer bootstrapDelegate)
  {
    this(streamDelegate, bootstrapDelegate, null);
  }

  public DelegatingDatabusCombinedConsumer(DatabusStreamConsumer streamDelegate,
                                           DatabusBootstrapConsumer bootstrapDelegate,
                                           Logger log)
  {
    super();
    _streamDelegate = streamDelegate;
    _bootstrapDelegate = bootstrapDelegate;
    _log = null != log ? log : Logger.getLogger(this.getClass());
  }

  public DelegatingDatabusCombinedConsumer(DatabusCombinedConsumer delegate)
  {
    this(delegate, delegate, null);
  }

  public DelegatingDatabusCombinedConsumer(DatabusCombinedConsumer delegate, Logger log)
  {
    this(delegate, delegate, log);
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onStartConsumption() : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStartConsumption error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onStopConsumption() : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStopConsumption error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onStartDataEventSequence(startScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStartDataEventSequence error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onEndDataEventSequence(endScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onEndDataEventSequence error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onRollback(rollbackScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onRollback error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onStartSource(source, sourceSchema) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStartSource error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onEndSource(source, sourceSchema) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onEndSource error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onDataEvent(e, eventDecoder) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onDataEvent error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onCheckpoint(checkpointScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onCheckpoint error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    try
    {
      return null != _streamDelegate ? _streamDelegate.onError(err) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onError error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onStartBootstrap() : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStartBootstrap error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onStopBootstrap() : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStopBootstrap error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onStartBootstrapSequence(startScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStartBootstrapSequence error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onEndBootstrapSequence(endScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onEndBootstrapSequence error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onStartBootstrapSource(name, sourceSchema) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onStartBootstrapSource error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onEndBootstrapSource(name, sourceSchema) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onEndBootstrapSource error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onBootstrapEvent(e, eventDecoder) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onBootstrapEvent error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onBootstrapRollback(batchCheckpointScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onBootstrapRollback error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onBootstrapCheckpoint(batchCheckpointScn) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onBootstrapCheckpoint error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    try
    {
      return null != _bootstrapDelegate ? _bootstrapDelegate.onBootstrapError(err) : ConsumerCallbackResult.SUCCESS;
    }
    catch (RuntimeException re)
    {
      _log.error("onBootstrapError error: " + re.getMessage(), re);
      return ConsumerCallbackResult.ERROR;
    }
  }

  public DatabusStreamConsumer getStreamDelegate()
  {
    return _streamDelegate;
  }

  public DatabusBootstrapConsumer getBootstrapDelegate()
  {
    return _bootstrapDelegate;
  }

  public Logger getLog()
  {
    return _log;
  }

  public void setLog(Logger log)
  {
    _log = log;
  }
  
  @Override
  public boolean canBootstrap() 
  {
    return true;
  }

}
