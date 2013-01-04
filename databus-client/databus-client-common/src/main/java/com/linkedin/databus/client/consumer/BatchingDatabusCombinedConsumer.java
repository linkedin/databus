package com.linkedin.databus.client.consumer;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * Buffers incoming onEvent calls in batches and calls the delegates once the batches reach a
 * certain size.
 *
 * @author cbotev
 */
public abstract class BatchingDatabusCombinedConsumer<T extends SpecificRecord>
       extends AbstractDatabusCombinedConsumer
{
  public static final String MODULE = BatchingDatabusCombinedConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final StaticConfig _staticConfig;
  private final Class<T> _payloadClass;
  private final ArrayList<T> _streamEvents;
  private final ArrayList<T> _bootstrapEvents;

  public BatchingDatabusCombinedConsumer(Config config,
                                         Class<T> payloadClass) throws InvalidConfigException
  {
    this(config.build(), payloadClass);
  }

  public BatchingDatabusCombinedConsumer(StaticConfig config,
                                         Class<T> payloadClass)
  {
    super();
    _staticConfig = config;
    _payloadClass = payloadClass;
    _streamEvents = new ArrayList<T>(config.getStreamBatchSize());
    _bootstrapEvents = new ArrayList<T>(config.getBootstrapBatchSize());
  }


  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    ConsumerCallbackResult result = flushStreamEvents();
    return result;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    switch (_staticConfig.getBatchingLevel())
    {
      case SOURCES:
      case WINDOWS: result = flushStreamEvents(); break;
      case FULL: result = ConsumerCallbackResult.SUCCESS; break;
    }
    return result;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    ConsumerCallbackResult result = flushStreamEvents();
    return result;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    switch (_staticConfig.getBatchingLevel())
    {
      case SOURCES:result = flushStreamEvents(); break;
      case WINDOWS:
      case FULL: result = ConsumerCallbackResult.SUCCESS; break;
    }
    return result;
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    ConsumerCallbackResult result = flushStreamEvents();
    return result;
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    ConsumerCallbackResult result = flushBootstrapEvents();
    return result;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    switch (_staticConfig.getBatchingLevel())
    {
      case SOURCES:
      case WINDOWS: result = flushBootstrapEvents(); break;
      case FULL: result = ConsumerCallbackResult.SUCCESS; break;
    }
    return result;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    switch (_staticConfig.getBatchingLevel())
    {
      case SOURCES:result = flushBootstrapEvents(); break;
      case WINDOWS:
      case FULL: result = ConsumerCallbackResult.SUCCESS; break;
    }
    return result;
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                 DbusEventDecoder eventDecoder)
  {
    T eventObj = eventDecoder.getTypedValue(e, (T)null, _payloadClass);
    ConsumerCallbackResult result = addBootstrapEvent(eventObj);
    return result;
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    ConsumerCallbackResult result = flushBootstrapEvents();
    return result;
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    ConsumerCallbackResult result = flushBootstrapEvents();
    return result;
  }


  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    T eventObj = eventDecoder.getTypedValue(e, (T)null, _payloadClass);
    ConsumerCallbackResult result = addDataEvent(eventObj);
    return result;
  }

  private ConsumerCallbackResult addDataEvent(T eventObj)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    _streamEvents.add(eventObj);
    if (_streamEvents.size() >= _staticConfig.getStreamBatchSize())
    {
      result = flushStreamEvents();
    }

    return result;
  }

  private ConsumerCallbackResult addBootstrapEvent(T eventObj)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    _bootstrapEvents.add(eventObj);
    if (_bootstrapEvents.size() >= _staticConfig.getBootstrapBatchSize())
    {
      result = flushBootstrapEvents();
    }

    return result;
  }

  private ConsumerCallbackResult flushStreamEvents()
  {
    ConsumerCallbackResult result;
    if (0 == _streamEvents.size()) result = ConsumerCallbackResult.SUCCESS;
    else
    {
      result = onDataEventsBatch(_streamEvents);
      _streamEvents.clear();
    }

    return result;
  }

  private ConsumerCallbackResult flushBootstrapEvents()
  {
    ConsumerCallbackResult result;
    if (0 == _bootstrapEvents.size()) result = ConsumerCallbackResult.SUCCESS;
    else
    {
      result = onBootstrapEventsBatch(_bootstrapEvents);
      _bootstrapEvents.clear();
    }

    return result;
  }

  protected abstract ConsumerCallbackResult onDataEventsBatch(List<T> eventsBatch);
  protected abstract ConsumerCallbackResult onBootstrapEventsBatch(List<T> eventsBatch);

  public StaticConfig getStaticConfig()
  {
    return _staticConfig;
  }

  public static class StaticConfig
  {
    /**
     * Batching level for batching combined consumer.
     *
     * <ul>
     *  <li>SOURCES - only event within the same source are batched</li>
     *  <li>WINDOWS - only event within the same window are batched</li>
     *  <li>FULL - all events are batched</li>
     * </ul>
     * */
    public static enum BatchingLevel
    {
      SOURCES,
      WINDOWS,
      FULL
    }

    private final int _streamBatchSize;
    private final int _bootstrapBatchSize;
    private final BatchingLevel _batchingLevel;

    public StaticConfig(BatchingLevel batchingLevel, int streamBatchSize, int bootstrapBatchSize)
    {
      super();
      _batchingLevel = batchingLevel;
      _streamBatchSize = streamBatchSize;
      _bootstrapBatchSize = bootstrapBatchSize;
    }

    /** The max number of batched stream data events */
    public int getStreamBatchSize()
    {
      return _streamBatchSize;
    }

    /** The max number of batched bootstrap data events */
    public int getBootstrapBatchSize()
    {
      return _bootstrapBatchSize;
    }

    /** The level at witch batching happens*/
    public BatchingLevel getBatchingLevel()
    {
      return _batchingLevel;
    }

    @Override
    public String toString()
    {
      StringBuilder resBuilder = new StringBuilder(100);
      Formatter fmt = new Formatter(resBuilder);
      try
      {
	      fmt.format("{\"batchingLevel\":\"%s\",\"streamBatchSize\":%d,\"bootstrapBatchSize\":%d}",
	                 _batchingLevel.toString(), _streamBatchSize, _bootstrapBatchSize);
	
	      fmt.flush();
	      return fmt.toString();
      }
      finally
      {
    	  fmt.close();
      }
    }
  }


  public static class Config implements ConfigBuilder<StaticConfig>
  {

    private int _streamBatchSize = 10;
    private int _bootstrapBatchSize = 20;
    private String _batchingLevel = StaticConfig.BatchingLevel.SOURCES.toString();

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      StaticConfig.BatchingLevel batchingLevel = null;
      try
      {
        batchingLevel = StaticConfig.BatchingLevel.valueOf(_batchingLevel);
      }
      catch (Exception e)
      {
        throw new InvalidConfigException("invalid batchingLevel:" + _batchingLevel);
      }

      if (_streamBatchSize <= 0) throw new InvalidConfigException("invalid streamBatchSize:" +
                                                                  _streamBatchSize);
      if (_bootstrapBatchSize <= 0) throw new InvalidConfigException("invalid bootstrapBatchSize:" +
                                                                     _bootstrapBatchSize);

      StaticConfig newConfig = new StaticConfig(batchingLevel, _streamBatchSize, _bootstrapBatchSize);
      LOG.info(BatchingDatabusCombinedConsumer.class.getSimpleName() + ".Config:" + newConfig);

      return newConfig;
    }

    public int getStreamBatchSize()
    {
      return _streamBatchSize;
    }

    public void setStreamBatchSize(int streamBatchSize)
    {
      _streamBatchSize = streamBatchSize;
    }

    public int getBootstrapBatchSize()
    {
      return _bootstrapBatchSize;
    }

    public void setBootstrapBatchSize(int bootstrapBatchSize)
    {
      _bootstrapBatchSize = bootstrapBatchSize;
    }

    public String getBatchingLevel()
    {
      return _batchingLevel;
    }

    public void setBatchingLevel(String batchingLevel)
    {
      _batchingLevel = batchingLevel;
    }

  }

}
