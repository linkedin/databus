package com.linkedin.databus.client.generic;

import com.linkedin.databus.client.ClientFileBasedEventTrackingCallback;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

public class DatabusFileLoggingConsumer extends AbstractDatabusCombinedConsumer
	implements DatabusConsumerPauseInterface
{

  public final static String MODULE = DatabusFileLoggingConsumer.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  private ClientFileBasedEventTrackingCallback _fileBasedCallback = null;
  private boolean _isPaused;
  private String _EventPattern = null;

  public static class StaticConfig
  {
    private final String _valueDumpFile;
    private boolean _append;
    
    /** The file where to store the JSON values. If null, no values are to be stored. */
    public String getValueDumpFile()
    {
      return _valueDumpFile;
    }

    public boolean isAppendOnly()
    {
    	return _append;
    }
    
    public StaticConfig(String valueDumpFile, boolean append)
    {
      _valueDumpFile = valueDumpFile;
      _append = append;
    }

  }

  public static class StaticConfigBuilder implements ConfigBuilder<StaticConfig>
  {
    private String _valueDumpFile;
    private boolean _appendOnly = false; // by default file logging is not append-only
    
    public String getValueDumpFile()
    {
      return _valueDumpFile;
    }

    public void setValueDumpFile(String valueDumpFile)
    {
      _valueDumpFile = valueDumpFile;
    }
    
    public boolean getAppendOnly() {
		return _appendOnly;
	}

	public void setAppendOnly(boolean appendOnly) {
		this._appendOnly = appendOnly;
	}

	@Override
    public StaticConfig build() throws InvalidConfigException
    {
      return new StaticConfig(_valueDumpFile, _appendOnly);
    }

  }

  public DatabusFileLoggingConsumer(StaticConfigBuilder configBuilder)
         throws IOException, InvalidConfigException
  {
    this(configBuilder.build());
  }

  public DatabusFileLoggingConsumer(StaticConfig config) throws IOException
  {
    this(config.getValueDumpFile(), config.isAppendOnly());
  }

  public DatabusFileLoggingConsumer(String outputFilename, boolean appendOnly) throws IOException
  {
    if (outputFilename != null)
    {
      LOG.info("DatabusFileLoggingConsumer instantiated with output file :" + outputFilename + ", appendOnly :" + appendOnly);	
      _fileBasedCallback = new ClientFileBasedEventTrackingCallback(outputFilename, appendOnly);
      _fileBasedCallback.init();
    }
  }

  public DatabusFileLoggingConsumer() throws IOException
  {
    this((String)null, false);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn) {
    waitIfPaused();
    LOG.info("startEvents:" + checkpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  protected void LogTypedValue(DbusEvent e, DbusEventDecoder eventDecoder) {
    LOG.info("Log Typed Value");
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder) {
    waitIfPaused();
    if (!e.isValid())
    {
      throw new RuntimeException("Got invalid event!!!");
    }
    if (_fileBasedCallback != null)
    {
      _fileBasedCallback.dumpEventValue(e, eventDecoder);
    }
    LogTypedValue(e, eventDecoder);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
    waitIfPaused();
    LOG.info("endEvents:" + endScn);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema) {
    waitIfPaused();
    LOG.info("endSource:" + source);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn) {
    waitIfPaused();
    LOG.info("rollback:"+ startScn);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn) {
    waitIfPaused();
    LOG.info("startDataEventSequence:" + startScn);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema) {
    waitIfPaused();
    LOG.info("startSource:" + source);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    waitIfPaused();
    // The file based logging already done in LoggingConsumer, this one just deserialize if needed
    //_fileBasedCallback.onEvent(e);
    if (!e.isValid())
    {
      throw new RuntimeException("Got invalid event!!!");
    }
    if (_fileBasedCallback != null)
    {
      _fileBasedCallback.dumpEventValue(e, eventDecoder);
    }
    LogTypedValue(e, eventDecoder);
    printBootstrapEventInfo(BootstrapStage.OnBootstrapEvent, e.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public synchronized void pause()
  {
    _isPaused = true;
    LOG.info("Consumer is set to pause!");
  }

  @Override
  public synchronized void resume()
  {
    _isPaused = false;
    notifyAll();
    LOG.info("Consumer is set to resume!");
  }

  @Override
  public synchronized void waitIfPaused()
  {
    while (_isPaused)
    {
      LOG.info("Consumer is paused!");
      try
      {
        wait();
      }
      catch (InterruptedException e)
      {
        // resume waiting, nothing to do
      }
    }
  }
  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.StartBootstrapSequence, startScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSequence, endScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.StartBootstrapSource, name);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSource, name);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    waitIfPaused();
    printBootstrapEventInfo(BootstrapStage.OnCheckpointEvent, batchCheckpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  enum StreamStage
  {
    StartDataEventSequence,
    EndDataEventSequence,
    OnStreamEvent,
    OnCheckpointEvent,
    StartStreamSource,
    EndStreamSource,
    InvalidStage
  }

  enum BootstrapStage
  {
    StartBootstrapSequence,
    EndBootstrapSequence,
    /*    StartSnapshotBatch,
    EndSnapshotBatch,
    StartCatchupBatch,
    EndCatchupBatch,
     */    OnBootstrapEvent,
     OnCheckpointEvent,
     StartBootstrapSource,
     EndBootstrapSource
  }

  protected void printBootstrapEventInfo(BootstrapStage stage, String info)
  {
    LOG.info(stage + ": " + info);
    //System.out.println(stage + ": " + info);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    waitIfPaused();
    LOG.info("startBootstrap");
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    waitIfPaused();
    LOG.info("stopBootstrap");
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    waitIfPaused();
    LOG.info("startBootstrapRollback:" + batchCheckpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    waitIfPaused();
    LOG.info("startConsumption");
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    waitIfPaused();
    LOG.info("stopConsumption");
    return ConsumerCallbackResult.SUCCESS;
  }

  public String getEventPattern()
  {
    return _EventPattern;
  }
  public Boolean isCheckingEventPattern()
  {
    return (_EventPattern != null) ;
  }

  public void setEventPattern(String checkEventPattern)
  {
    this._EventPattern = checkEventPattern;
  }
}
