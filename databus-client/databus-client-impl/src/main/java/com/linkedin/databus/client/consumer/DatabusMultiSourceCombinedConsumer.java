/**
 * 
 */
package com.linkedin.databus.client.consumer;

import java.util.Map;
import java.util.Map.Entry;

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
 * @author "Balaji Varadarajan<bvaradarajan@linkedin.com>"
 *
 * A multi-source Databus V2 delegator Consumer implementing both stream and bootstrap callbacks. 
 * Source specific consumers which implement the business logic are registered to this class.
 * 
 */
public class DatabusMultiSourceCombinedConsumer implements DatabusCombinedConsumer
{
  public static final String MODULE = DatabusMultiSourceCombinedConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  
  private final Map<String, DatabusStreamConsumer> _streamConsumerMap;
  private final Map<String, DatabusBootstrapConsumer> _bootstrapConsumerMap;
  
  private DatabusStreamConsumer _currentStreamConsumer;
  private DatabusBootstrapConsumer _currentBootstrapConsumer;
  
  public DatabusMultiSourceCombinedConsumer(Map<String, DatabusStreamConsumer> streamConsumerMap,
                                            Map<String, DatabusBootstrapConsumer> bootstrapConsumerMap)
  {
    _streamConsumerMap = streamConsumerMap; 
    _bootstrapConsumerMap = bootstrapConsumerMap;
  }
  
  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onStartConsumption()
   */
  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
     ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
     for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
     {
       ConsumerCallbackResult r = entry.getValue().onStartConsumption();
       
       if ( r != ConsumerCallbackResult.SUCCESS)
       {  
         result = r;
       }
     }
     return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onStopConsumption()
   */
  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onStopConsumption();
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onStartDataEventSequence(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onStartDataEventSequence(startScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onEndDataEventSequence(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onEndDataEventSequence(endScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onRollback(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onRollback(rollbackScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onStartSource(java.lang.String, org.apache.avro.Schema)
   */
  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    DatabusStreamConsumer consumer = _streamConsumerMap.get(source);
    _currentStreamConsumer = consumer;
    if ( null == consumer)
    {
      LOG.fatal("onStartSource: Unable to find the source in the StreamConsumerMap. Received Source :" + source + ", sourceSchema :" + sourceSchema);
      return ConsumerCallbackResult.ERROR_FATAL;
    }
    return consumer.onStartSource(source, sourceSchema);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onEndSource(java.lang.String, org.apache.avro.Schema)
   */
  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  { 
    return _currentStreamConsumer.onEndSource(source, sourceSchema);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onDataEvent(com.linkedin.databus.core.DbusEvent, com.linkedin.databus.client.pub.DbusEventDecoder)
   */
  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return _currentStreamConsumer.onDataEvent(e, eventDecoder);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onCheckpoint(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onCheckpoint(checkpointScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusStreamConsumer#onError(java.lang.Throwable)
   */
  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusStreamConsumer> entry : _streamConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onError(err);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onStartBootstrap()
   */
  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onStartBootstrap();
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onStopBootstrap()
   */
  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onStopBootstrap();
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onStartBootstrapSequence(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onStartBootstrapSequence(startScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onEndBootstrapSequence(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onEndBootstrapSequence(endScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onStartBootstrapSource(java.lang.String, org.apache.avro.Schema)
   */
  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
  {
    DatabusBootstrapConsumer consumer = _bootstrapConsumerMap.get(name);
    _currentBootstrapConsumer = consumer;
    if ( null == consumer)
    {
      LOG.fatal("onStartBootstrapSource: Unable to find the source in the StreamConsumerMap. Received Source :" + name + ", sourceSchema :" + sourceSchema);
      return ConsumerCallbackResult.ERROR_FATAL;
    }
    return consumer.onStartBootstrapSource(name, sourceSchema);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onEndBootstrapSource(java.lang.String, org.apache.avro.Schema)
   */
  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    return _currentBootstrapConsumer.onEndBootstrapSource(name, sourceSchema);

  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onBootstrapEvent(com.linkedin.databus.core.DbusEvent, com.linkedin.databus.client.pub.DbusEventDecoder)
   */
  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                 DbusEventDecoder eventDecoder)
  {
    return _currentBootstrapConsumer.onBootstrapEvent(e,eventDecoder);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onBootstrapRollback(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onBootstrapRollback(batchCheckpointScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onBootstrapCheckpoint(com.linkedin.databus.client.pub.SCN)
   */
  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onBootstrapCheckpoint(batchCheckpointScn);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.pub.DatabusBootstrapConsumer#onBootstrapError(java.lang.Throwable)
   */
  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS; 
    for (Entry<String, DatabusBootstrapConsumer> entry : _bootstrapConsumerMap.entrySet())
    {
      ConsumerCallbackResult r = entry.getValue().onBootstrapError(err);
      
      if ( r != ConsumerCallbackResult.SUCCESS)
      {  
        result = r;
      }
    }
    return result;
  }
}
