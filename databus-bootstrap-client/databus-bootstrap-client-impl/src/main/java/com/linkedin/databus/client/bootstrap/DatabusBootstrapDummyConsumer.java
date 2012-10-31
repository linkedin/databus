package com.linkedin.databus.client.bootstrap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.AbstractDatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.util.InvalidConfigException;

public class DatabusBootstrapDummyConsumer extends AbstractDatabusBootstrapConsumer
{
  public final static String MODULE = DatabusBootstrapDummyConsumer.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  static DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                                         int maxReadBufferSize, AllocationPolicy allocationPolicy, QueuePolicy policy)
  throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxEventBufferSize);
    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setReadBufferSize(maxReadBufferSize);
    config.setAllocationPolicy(allocationPolicy.name());
    config.setQueuePolicy(policy.toString());
    return config.build();
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    printBootstrapEventInfo(BootstrapStage.StartBootstrapSequence, startScn.toString());
	return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSequence, endScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
  {
    printBootstrapEventInfo(BootstrapStage.StartBootstrapSource, name);
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    printBootstrapEventInfo(BootstrapStage.EndBootstrapSource, name);
    return ConsumerCallbackResult.SUCCESS;
  }



  /*  @Override
  public void startSnapshotBatch(Checkpoint startBatchCheckpoint)
  {
    printCheckpointInfo(BootstrapStage.StartSnapshotBatch, startBatchCheckpoint);
  }

  @Override
  public void endSnapshotBatch(Checkpoint endBatchCheckpoint)
  {
    printCheckpointInfo(BootstrapStage.EndSnapshotBatch, endBatchCheckpoint);
  }

  @Override
  public void startCatchupBatch(Checkpoint startBatchCheckpoint)
  {
    printCheckpointInfo(BootstrapStage.StartCatchupBatch, startBatchCheckpoint);
  }

  @Override
  public void endCatchupBatch(Checkpoint endBatchCheckpoint)
  {
    printCheckpointInfo(BootstrapStage.EndCatchupBatch, endBatchCheckpoint);
  }
   */

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    printBootstrapEventInfo(BootstrapStage.OnBootstrapEvent, e.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    printBootstrapEventInfo(BootstrapStage.OnCheckpointEvent, batchCheckpointScn.toString());
    return ConsumerCallbackResult.SUCCESS;
  }

  /*
  @Override
  public ConsumerCallbackResult onCheckpointEvent(Checkpoint batchCheckpoint)
  {
    printCheckpointInfo(BootstrapStage.OnCheckpointEvent, batchCheckpoint);
    return ConsumerCallbackResult.SUCCESS;
  }
   */
  protected void printBootstrapEventInfo(BootstrapStage stage, String info)
  {
    LOG.info(stage + ": " + info);
    //System.out.println(stage + ": " + info);
  }

  public static void main(String args[]) throws Exception
  {
    /* Commenting out because one cannot use DatabusSourceConnection only for bootstrap
    Properties startupProps = BootstrapConfig.loadConfigProperties(args);

    DatabusHttpClientImpl.Config clientConfigBuilder = new DatabusHttpClientImpl.Config();
    ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
      new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("databus.client.", clientConfigBuilder);

    DatabusHttpClientImpl.StaticConfig clientConfig = configLoader.loadConfig(startupProps);;
    String sourceName = "source1";

    ServerInfoBuilder bootstrapBuilder = clientConfig.getRuntime().getBootstrap().getService("2");
    bootstrapBuilder.setName("DefaultBootstrapServices");
    bootstrapBuilder.setHost("localhost");
    bootstrapBuilder.setPort(6060);
    bootstrapBuilder.setSources(sourceName);

    DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

    DatabusHttpClientImpl.RuntimeConfig clientRuntimeConfig =
      client.getClientConfigManager().getReadOnlyConfig();

    CheckpointPersistenceProvider cpPersistenceProvider =
      clientConfig.getCheckpointPersistence().getOrCreateCheckpointPersistenceProvider();

    DbusEventBuffer eventBuffer = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 1000, 1500000, AllocationPolicy.DIRECT_MEMORY, QueuePolicy.BLOCK_ON_WRITE));
    Long sourceId = Long.valueOf(1L);
    Long schemaId = Long.valueOf(123456789L);

    // initialize source list
    List<String> sources = new ArrayList<String>();
    sources.add(sourceName);

    // initialize source ID, Name pair list
    IdNamePair idNamePair = new IdNamePair(sourceId, sourceName);
    List<IdNamePair> idNameList = new ArrayList<IdNamePair>();
    idNameList.add(idNamePair);

    // initialize source ID, Schema list
    RegisterResponseEntry respEntry = new RegisterResponseEntry(schemaId,
                                                                sourceName);
    Map<Long, RegisterResponseEntry> sourcesSchemas = new HashMap<Long, RegisterResponseEntry>();
    sourcesSchemas.put(sourceId, respEntry);

    // set up bootstrap server info list
    Set<ServerInfo> bootstrapServices = clientRuntimeConfig.getBootstrap().getServices();

    // create bootstrap consumer callbacks
    DatabusBootstrapDummyConsumer dummyBootstrapConsumer = new DatabusBootstrapDummyConsumer();
    ArrayList<DatabusBootstrapConsumer> consumerCallbacks = new ArrayList<DatabusBootstrapConsumer>();
    consumerCallbacks.add(dummyBootstrapConsumer);

    // creat pull and dispatch threads
    ExecutorService ioThreadPool = Executors.newCachedThreadPool();

    ContainerStatisticsCollector containerStatsCollector =
      clientConfig.getContainer().getOrCreateContainerStatsCollector();

    DbusEventsStatisticsCollector dbusEventsStatsCollector =
      new DbusEventsStatisticsCollector(clientConfig.getContainer().getId(),
                                        "outboundEvents",
                                        clientConfig.getRuntime().getContainer().getOutboundEventsStatsCollector().isEnabled(),
                                        true,
                                        clientConfig.getContainer().getOrCreateMBeanServer());
    startState.switchToSourcesSuccess(idNameList);
    startState.switchToRegisterSuccess(sourcesSchemas);
    // this has to be called before we can start the thread
    ((BootstrapPullThread)clientPullThread).setRelayState(startState);

    // create the http connection to bootstrap service and start getting events
    DatabusSourcesConnection dbusConn = new DatabusSourcesConnection(bootstrapServices,
                                                                         sources,
                                                                         eventBuffer,
                                                                         dispatcherThread,
                                                                         clientPullThread,
                                                                         ioThreadPool,
                                                                         clientConfig,
                                                                         dbusEventsStatsCollector);

    dbusConn.start();
    dbusConn.await();
    dbusConn.stop();*/
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
}
