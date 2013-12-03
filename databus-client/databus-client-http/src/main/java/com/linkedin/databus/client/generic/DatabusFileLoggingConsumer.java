package com.linkedin.databus.client.generic;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.ClientFileBasedEventTrackingCallback;
import com.linkedin.databus.client.ClientFileBasedMetadataTrackingCallback;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.FileBasedEventTrackingCallback;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class DatabusFileLoggingConsumer extends AbstractDatabusCombinedConsumer
	implements DatabusConsumerPauseInterface
{

  public final static String MODULE = DatabusFileLoggingConsumer.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  private ClientFileBasedEventTrackingCallback _fileBasedDecodedValueCallback = null;
  private ClientFileBasedMetadataTrackingCallback _fileBasedMetadataCallback = null;
  private FileBasedEventTrackingCallback _fileBasedRawEventCallback = null;
  private boolean _isPaused;
  private String _EventPattern = null;

  public static class StaticConfig
  {
    private final String _valueDumpFile;
    private final String _metadataDumpFile;
    private final String _eventDumpFile;
    private boolean _append;

    /** The file in which to store the payload values in JSON format. If null, no values are to be stored. */
    public String getValueDumpFile()
    {
      return _valueDumpFile;
    }

    /** The file in which to store the decoded metadata info from v2 events. If null, no metadata are to be stored. */
    public String getMetadataDumpFile()
    {
      return _metadataDumpFile;
    }

    /** The file in which to store the raw (undecoded) event in JSON format. If null, no raw events will be stored. */
    public String getEventDumpFile()
    {
      return _eventDumpFile;
    }

    public boolean isAppendOnly()
    {
      return _append;
    }

//NOT USED?
//  public StaticConfig(String valueDumpFile, boolean append)
//  {
//    this(valueDumpFile, null, append);
//  }

    public StaticConfig(String valueDumpFile, String metadataDumpFile, String eventDumpFile, boolean append)
    {
      _valueDumpFile = valueDumpFile;
      _metadataDumpFile = metadataDumpFile;
      _eventDumpFile = eventDumpFile;
      _append = append;
    }
  }


  public static class StaticConfigBuilder implements ConfigBuilder<StaticConfig>
  {
    private String _valueDumpFile;
    private String _metadataDumpFile;
    private String _eventDumpFile;
    private boolean _appendOnly = false; // by default file logging is not append-only

    public String getValueDumpFile()
    {
      return _valueDumpFile;
    }

    public String getMetadataDumpFile()
    {
      return _metadataDumpFile;
    }

    public String getEventDumpFile()
    {
      return _eventDumpFile;
    }

    public void setValueDumpFile(String valueDumpFile)
    {
      _valueDumpFile = valueDumpFile;
    }

    public void setMetadataDumpFile(String metadataDumpFile)
    {
      _metadataDumpFile = metadataDumpFile;
    }

    public void setEventDumpFile(String eventDumpFile)
    {
      _eventDumpFile = eventDumpFile;
    }

    public boolean getAppendOnly()
    {
      return _appendOnly;
    }

    public void setAppendOnly(boolean appendOnly)
    {
      this._appendOnly = appendOnly;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      return new StaticConfig(_valueDumpFile, _metadataDumpFile, _eventDumpFile, _appendOnly);
    }
  }


//NOT USED?
//public DatabusFileLoggingConsumer() throws IOException
//{
//  this((String)null, false);
//}

//NOT USED?
//public DatabusFileLoggingConsumer(StaticConfigBuilder configBuilder)
//       throws IOException, InvalidConfigException
//{
//  this(configBuilder.build());
//}

  public DatabusFileLoggingConsumer(StaticConfig config) throws IOException
  {
    this(config.getValueDumpFile(), config.getMetadataDumpFile(), config.getEventDumpFile(), config.isAppendOnly());
  }

  public DatabusFileLoggingConsumer(String valueDumpFile, boolean appendOnly) throws IOException
  {
    this(valueDumpFile, null, null, appendOnly);
  }

  public DatabusFileLoggingConsumer(String valueDumpFile,
                                    String metadataDumpFile,
                                    String eventDumpFile,
                                    boolean appendOnly)
  throws IOException
  {
    LOG.info("DatabusFileLoggingConsumer instantiated with payload-value dump file: " + valueDumpFile +
             ", metadata dump file: " + metadataDumpFile +
             ", raw-event dump file: " + eventDumpFile +
             ", appendOnly: " + appendOnly);

    if (valueDumpFile != null)
    {
      _fileBasedDecodedValueCallback = new ClientFileBasedEventTrackingCallback(valueDumpFile, appendOnly);
      _fileBasedDecodedValueCallback.init();
    }

    if (metadataDumpFile != null)
    {
      _fileBasedMetadataCallback = new ClientFileBasedMetadataTrackingCallback(metadataDumpFile, appendOnly);
      _fileBasedMetadataCallback.init();
    }

    if (eventDumpFile != null)
    {
      _fileBasedRawEventCallback = new FileBasedEventTrackingCallback(eventDumpFile, appendOnly);
      _fileBasedRawEventCallback.init();
    }
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    waitIfPaused();
    LOG.info("startEvents:" + checkpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  protected void LogTypedValue(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    LOG.info("Log Typed Value");
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    waitIfPaused();
    if (!e.isValid())
    {
      throw new RuntimeException("Got invalid event!!!");
    }

    if (_fileBasedDecodedValueCallback != null)
    {
      _fileBasedDecodedValueCallback.dumpEventValue(e, eventDecoder);
    }

    if( _fileBasedMetadataCallback != null && eventDecoder instanceof DbusEventAvroDecoder )
    {
    	_fileBasedMetadataCallback.dumpEventMetadata(e, (DbusEventAvroDecoder) eventDecoder);
    }

    if (_fileBasedRawEventCallback != null)
    {
      _fileBasedRawEventCallback.onEvent(e);
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

    if (!e.isValid())
    {
      throw new RuntimeException("Got invalid event!!!");
    }

    if (_fileBasedDecodedValueCallback != null)
    {
      _fileBasedDecodedValueCallback.dumpEventValue(e, eventDecoder);
    }

    if (_fileBasedRawEventCallback != null)
    {
      _fileBasedRawEventCallback.onEvent(e);
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
    // There are integration tests that rely on this message (they look for "EndBootstrapSequence:" in the logs)
    LOG.info(stage + ": " + info);
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
