package com.linkedin.databus.client.consumer;

import org.apache.avro.Schema;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;

/**
 * Provides default implementations for all {@link DatabusStreamConsumer} methods. Real
 * implementations can override only the methods they care about
 * @author cbotev
 *
 */
public abstract class AbstractDatabusStreamConsumer implements DatabusStreamConsumer
{
  private final ConsumerCallbackResult _defaultAnswer;

  protected AbstractDatabusStreamConsumer(ConsumerCallbackResult defaultAnswer)
  {
    _defaultAnswer = defaultAnswer;
  }

  protected AbstractDatabusStreamConsumer()
  {
    this(ConsumerCallbackResult.SUCCESS);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return _defaultAnswer;
  }
}
