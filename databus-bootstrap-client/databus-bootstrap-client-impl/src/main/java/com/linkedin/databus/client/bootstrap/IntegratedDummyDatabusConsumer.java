/**
 *
 */
package com.linkedin.databus.client.bootstrap;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig.ProviderType;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.generic.ConsumerPauseRequestProcessor;
import com.linkedin.databus.client.generic.DatabusConsumerPauseInterface;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.FileBasedEventTrackingCallback;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

/**
 * @author lgao
 *
 */
public class IntegratedDummyDatabusConsumer
extends DatabusBootstrapDummyConsumer
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

  public IntegratedDummyDatabusConsumer(String outputFilename, long maxEventBufferSize, int maxReadBufferSize, boolean useConsumerTimeout)
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
    // this is for the final comparason only because events from bootstrap
    // have different scn. But the window scn shall be of the same.
    _fileBasedCallback.onEvent(e);
    printBootstrapEventInfo(BootstrapStage.OnBootstrapEvent, e.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    waitIfPaused();
    _maxBootstrapWindownScn = ((SingleSourceSCN)endScn).getSequence();
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSequence, endScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    waitIfPaused();
    printStreamEventInfo(StreamStage.OnCheckpointEvent, checkpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }
  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    waitIfPaused();
    _fileBasedCallback.onEvent(e);
    printStreamEventInfo(StreamStage.OnStreamEvent, e.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    waitIfPaused();
    _maxRelayWindowScn = ((SingleSourceSCN)endScn).getSequence();
    printStreamEventInfo(StreamStage.EndDataEventSequence, endScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    printStreamEventInfo(StreamStage.EndStreamSource,
                         " source=" + source +
                         " schema=" + ((null == sourceSchema) ? "null" : sourceSchema.getFullName()));
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    printStreamEventInfo(StreamStage.InvalidStage, "rollback not implemented");
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    printStreamEventInfo(StreamStage.StartDataEventSequence, startScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    printStreamEventInfo(StreamStage.StartStreamSource,
                         " source=" + source +
                         " schema=" + ((null == sourceSchema) ? "null" : sourceSchema.getFullName()));
    return ConsumerCallbackResult.SUCCESS;
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
    clientConfigBuilder.getConnectionDefaults().setCheckpointThresholdPct(50.0);
    clientConfigBuilder.getRuntime().getBootstrap().setEnabled(true);


    DatabusSourcesConnection.Config srcDefaultConfig= new DatabusSourcesConnection.Config();
    srcDefaultConfig.setFreeBufferThreshold((int) (_maxEventBufferSize*0.05));
    srcDefaultConfig.setCheckpointThresholdPct(80);
    clientConfigBuilder.setConnectionDefaults(srcDefaultConfig);

    if (!_useConsumerTimeout)
    {
      clientConfigBuilder.getConnectionDefaults().setConsumerTimeBudgetMs(0);
    }
    DbusEventBuffer.Config eventBufferConfig = clientConfigBuilder.getConnectionDefaults().getEventBuffer();
    eventBufferConfig.setMaxSize(_maxEventBufferSize);
    eventBufferConfig.setReadBufferSize(_maxReadBufferSize);


    // TODO: the following shall be used once we can set eventbuffer for bootstrap throught the config builder (DDSDBUS-82)
    // For now, bootstrap buffer will use the same config. as relay buffer
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

    /*DbusEventBuffer eventBuffer = new DbusEventBuffer(TEN_MEGABYTES_IN_BYTES,
                                                      1000,
                                                      FIFTEEN_HUNDRED_KILOBYTES_IN_BYTES,
                                                      true, QueuePolicy.BLOCK_ON_WRITE);

    CheckpointPersistenceProvider cpPersistenceProvider =
      clientConfig.getCheckpointPersistence().getOrCreateCheckpointPersistenceProvider();

    DatabusHttpClientImpl.RuntimeConfig clientRuntimeConfig =
      _dbusClient.getClientConfigManager().getReadOnlyConfig();

    // creat pull and dispatch threads
    ContainerStatisticsCollector containerStatsCollector =
        (ContainerStatisticsCollector)clientConfig.getContainer().getOrCreateContainerStatsCollector();

    DbusEventsStatisticsCollector dbusEventsStatsCollector =
        new DbusEventsStatisticsCollector(clientConfig.getContainer().getId(),
                                          "outboundEvents",
                                clientConfig.getRuntime().getOutboundEventsStatsCollector().isEnabled(),
                                true,
                                clientConfig.getContainer().getOrCreateMBeanServer());

    ExecutorService ioThreadPool = Executors.newCachedThreadPool();
    DispatcherThread dispatcherThread = new RelayDispatcherThread(streamCallbacks);
    ClientPullThread clientPullThread = new RelayPullThread(clientRuntimeConfig.getRelays(),
                                                            sources,
                                                            eventBuffer,
                                                            ioThreadPool,
                                                            dispatcherThread,
                                                            bootstrapCallbacks,
                                                            clientConfig,
                                                            clientRuntimeConfig,
                                                            cpPersistenceProvider,
                                                            containerStatsCollector,
                                                            dbusEventsStatsCollector);

    // create the http connection to bootstrap service and start getting events
    _dbusClient = new DatabusHttpRelayConnection(clientRuntimeConfig.getBootstrap().getServices(),
                                               sources,
                                               eventBuffer,
                                               dispatcherThread,
                                               clientPullThread,
                                               ioThreadPool,
                                               clientConfig,
                                               containerStatsCollector,
                                               dbusEventsStatsCollector);*/
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

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return ConsumerCallbackResult.SUCCESS;
  }
}
