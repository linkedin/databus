/*
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
 */
package com.linkedin.databus.client.bootstrap;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig.ProviderType;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.generic.ConsumerPauseRequestProcessor;
import com.linkedin.databus.client.generic.DatabusConsumerPauseInterface;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.FileBasedEventTrackingCallback;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;

/**
 *
 */
public class IntegratedDummyDatabusConsumer extends DatabusBootstrapDummyConsumer
    implements DatabusStreamConsumer, DatabusConsumerPauseInterface
{
  public final static String MODULE = IntegratedDummyDatabusConsumer.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  private static final int FIFTEEN_HUNDRED_KILOBYTES_IN_BYTES = 1500000;
  private static final int TEN_MEGABYTES_IN_BYTES = 10000000;

  private DatabusHttpClientImpl _dbusClient;
  private FileBasedEventTrackingCallback _fileBasedCallback;
  private long _maxEventBufferSize;
  private int _maxReadBufferSize;
  private long _maxBootstrapWindownScn;
  private long _maxRelayWindowScn;
  private boolean _isPaused;
  private boolean _useConsumerTimeout;
  private int _numUserSpecifiedErrorResults;

  public IntegratedDummyDatabusConsumer(String outputFilename, long maxEventBufferSize, int maxReadBufferSize,
                                        boolean useConsumerTimeout)
  {
    // use the default log4j configuration
    //System.out.println("level set to debug");
    _fileBasedCallback = new FileBasedEventTrackingCallback(outputFilename, false);
    _maxEventBufferSize = maxEventBufferSize;
    _maxReadBufferSize = maxReadBufferSize;
    _useConsumerTimeout = useConsumerTimeout;
  }

  public IntegratedDummyDatabusConsumer(String outputFilename, long maxEventBufferSize, int maxReadBufferSize)
  {
    this(outputFilename, maxEventBufferSize, maxReadBufferSize, true);
  }

  public IntegratedDummyDatabusConsumer(String outputFilename)
  {
    this(outputFilename, TEN_MEGABYTES_IN_BYTES, FIFTEEN_HUNDRED_KILOBYTES_IN_BYTES);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    waitIfPaused();
    // this is for the final comparison only because events from bootstrap
    // have different scn. But the window scn shall be of the same.
    _fileBasedCallback.onEvent(e);
    printBootstrapEventInfo(BootstrapStage.OnBootstrapEvent, e.toString());
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    waitIfPaused();
    _maxBootstrapWindownScn = ((SingleSourceSCN)endScn).getSequence();
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSequence, endScn.toString());
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    waitIfPaused();
    printStreamEventInfo(StreamStage.OnCheckpointEvent, checkpointScn.toString());
    return getUserSpecifiedCallbackResult();
  }
  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    waitIfPaused();
    _fileBasedCallback.onEvent(e);
    printStreamEventInfo(StreamStage.OnStreamEvent, e.toString());
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    waitIfPaused();
    _maxRelayWindowScn = ((SingleSourceSCN)endScn).getSequence();
    printStreamEventInfo(StreamStage.EndDataEventSequence, endScn.toString());
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    printStreamEventInfo(StreamStage.EndStreamSource,
                         " source=" + source +
                         " schema=" + ((null == sourceSchema) ? "null" : sourceSchema.getFullName()));
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    printStreamEventInfo(StreamStage.InvalidStage, "rollback not implemented");
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    printStreamEventInfo(StreamStage.StartDataEventSequence, startScn.toString());
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    printStreamEventInfo(StreamStage.StartStreamSource,
                         " source=" + source +
                         " schema=" + ((null == sourceSchema) ? "null" : sourceSchema.getFullName()));
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return getUserSpecifiedCallbackResult();
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return getUserSpecifiedCallbackResult();
  }

  public void initConn(List<String> sources) throws IOException, InvalidConfigException,
                                                    DatabusClientException, DatabusException
  {
    StringBuilder sourcesString = new StringBuilder();
    boolean firstSrc = true;
    for (String source: sources)
    {
      if (! firstSrc) sourcesString.append(",");
      firstSrc = false;
      sourcesString.append(source);
    }

    _fileBasedCallback.init();
    ArrayList<DatabusBootstrapConsumer> bootstrapCallbacks = new ArrayList<DatabusBootstrapConsumer>();
    bootstrapCallbacks.add(this);

    ArrayList<DatabusStreamConsumer> streamCallbacks = new ArrayList<DatabusStreamConsumer>();
    streamCallbacks.add(this);

    DatabusHttpClientImpl.Config clientConfigBuilder = new DatabusHttpClientImpl.Config();
    clientConfigBuilder.getContainer().getJmx().setJmxServicePort(5555);
    clientConfigBuilder.getContainer().setId(545454);
    clientConfigBuilder.getContainer().setHttpPort(8082);
    clientConfigBuilder.getCheckpointPersistence().setType(ProviderType.FILE_SYSTEM.toString());
    clientConfigBuilder.getCheckpointPersistence().getFileSystem().setRootDirectory("./integratedconsumer-checkpoints");
    clientConfigBuilder.getCheckpointPersistence().setClearBeforeUse(true);
    clientConfigBuilder.getRuntime().getBootstrap().setEnabled(true);

    DatabusSourcesConnection.Config srcDefaultConfig= new DatabusSourcesConnection.Config();
    srcDefaultConfig.setFreeBufferThreshold((int) (_maxEventBufferSize*0.05));
    srcDefaultConfig.setCheckpointThresholdPct(80);
    srcDefaultConfig.setConsumerTimeBudgetMs(_useConsumerTimeout? 60000 : 0); // 60 sec before retries (unless disabled)
    srcDefaultConfig.getDispatcherRetries().setMaxRetryNum(3);                // max of 3 retries
    clientConfigBuilder.setConnectionDefaults(srcDefaultConfig);

    DbusEventBuffer.Config eventBufferConfig = clientConfigBuilder.getConnectionDefaults().getEventBuffer();
    eventBufferConfig.setMaxSize(_maxEventBufferSize);
    eventBufferConfig.setAverageEventSize(_maxReadBufferSize);


    // TODO: the following shall be used once we can set eventbuffer for bootstrap through the config builder (DDSDBUS-82)
    // For now, bootstrap buffer will use the same config as relay buffer.
    //clientConfigBuilder.getConnectionDefaults().
    DatabusHttpClientImpl.StaticConfig clientConfig = clientConfigBuilder.build();


    ServerInfoBuilder relayBuilder = clientConfig.getRuntime().getRelay("1");
    relayBuilder.setName("DefaultRelay");
    relayBuilder.setHost("localhost");
    relayBuilder.setPort(9000);
    relayBuilder.setSources(sourcesString.toString());

    ServerInfoBuilder bootstrapBuilder = clientConfig.getRuntime().getBootstrap().getService("2");
    bootstrapBuilder.setName("DefaultBootstrapServices");
    bootstrapBuilder.setHost("localhost");
    bootstrapBuilder.setPort(6060);
    bootstrapBuilder.setSources(sourcesString.toString());

    _dbusClient = new DatabusHttpClientImpl(clientConfig);
    _dbusClient.registerDatabusStreamListener(this, sources, null);
    _dbusClient.registerDatabusBootstrapListener(this, sources, null);
    // add pause processor
    try
    {
      _dbusClient.getProcessorRegistry().register(ConsumerPauseRequestProcessor.COMMAND_NAME,
                                                  new ConsumerPauseRequestProcessor(null, this));
    }
    catch (ProcessorRegistrationConflictException e)
    {
      LOG.error("Failed to register " + ConsumerPauseRequestProcessor.COMMAND_NAME);
    }
  }

  protected void printStreamEventInfo(StreamStage stage, String info)
  {
    LOG.debug(stage + ": " + info);
  }

  public synchronized void start() throws Exception
  {
    _isPaused = false;
    _dbusClient.start();
    LOG.info(MODULE + " started!");
  }

  public synchronized void shutdown()
  {
    _isPaused = false;
    _dbusClient.shutdown();
  }

  public long getMaxBootstrapWindowScn()
  {
    return _maxBootstrapWindownScn;
  }

  public long getMaxRelayWindowScn()
  {
    return _maxRelayWindowScn;
  }

  public StatsCollectors<UnifiedClientStats> getUnifiedClientStatsCollectors()
  {
    return _dbusClient.getUnifiedClientStatsCollectors();
  }

  public List<DatabusSourcesConnection> getRelayConnections()
  {
    return _dbusClient.getRelayConnections();
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

  public synchronized void setNumUserSpecifiedErrorResults(int numErrorResults)
  {
    LOG.info("Will return " + numErrorResults + " ConsumerCallbackResult.ERROR as requested.");
    _numUserSpecifiedErrorResults = numErrorResults;
  }

  protected synchronized ConsumerCallbackResult getUserSpecifiedCallbackResult()
  {
    if (_numUserSpecifiedErrorResults > 0)
    {
      --_numUserSpecifiedErrorResults;
      if (_numUserSpecifiedErrorResults > 0)
      {
        LOG.info("Returning ConsumerCallbackResult.ERROR as requested; still have " +
                 _numUserSpecifiedErrorResults + " more to go.");
      }
      else
      {
        LOG.info("Returning ConsumerCallbackResult.ERROR as requested (last one).");
      }
      return ConsumerCallbackResult.ERROR;
    }
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

  public static void main(String args[]) throws Exception
  {
    //BasicConfigurator.configure();
    //Logger.getRootLogger().setLevel(Level.INFO);

    IntegratedDummyDatabusConsumer consumer = new IntegratedDummyDatabusConsumer("IntegratedDummyDatabusConsumerMain");
    ArrayList<String> sources = new ArrayList<String>();
    sources.add("source1");
    consumer.initConn(sources);
    consumer.start();
    consumer.shutdown();
  }
}
