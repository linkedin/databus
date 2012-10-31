package com.linkedin.databus3.espresso.client.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.generic.DatabusFileLoggingConsumer;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus3.espresso.client.DatabusHttpV3ClientImpl;
import com.linkedin.databus3.espresso.client.data_model.EspressoSubscriptionUriCodec;

public class EspressoTestDatabusClient implements Startable, Shutdownable
{
  public static final Logger LOG = Logger.getLogger(EspressoTestDatabusClient.class.getName());

  private DatabusHttpV3ClientImpl [] _clients;
  private DatabusHttpV3ClientImpl  _v3client;
  private DatabusHttpV3ClientImpl.StaticConfig _clientConf;
  private DatabusFileLoggingConsumer.StaticConfig _consumerConf;

  public EspressoTestDatabusClient(Properties defaultProps)
         throws IOException, DatabusException, DatabusClientException
  {
    this(defaultProps, null);
  }

  public EspressoTestDatabusClient(Properties defaultProps, Properties overrideProps)
  throws IOException, DatabusException, DatabusClientException
  {
    // Merge the default properties and override properties into a single new properties object
    Properties props = new Properties();
    if (null != defaultProps)
    {
      for(Map.Entry<Object, Object> entry : defaultProps.entrySet())
      {
        props.setProperty((String)entry.getKey(), (String)entry.getValue());
      }
    }

    if (null != overrideProps)
    {
      for(Map.Entry<Object, Object> entry : overrideProps.entrySet())
      {
        props.setProperty((String)entry.getKey(), (String)entry.getValue());
      }
    }

    LOG.info("properties: " + defaultProps + "; override: " + overrideProps +
             "; merged props: " + props);

    /**
     * Provide an optional property to create 5.3 style clients.
     * Once 5.3 is stable, 5.2 style test client support will be removed
     */
    String is53TestClient = props.getProperty("databus.espresso.client.clusterManager.version");
    if (null != is53TestClient && is53TestClient.equals("multiPartition"))
    	createMultiPartitionConsumerTestClient(props);
    else if (null != is53TestClient && is53TestClient.equals("5.3"))
        create53StyleTestClient(props);
    else
    	create52StyleTestClient(props);
  }

  /**
   * Creates a 5.2 style client, wherein
   * (1) A different client would be created per subscription
   * @param props
   * @throws IOException
   * @throws DatabusException
   * @throws DatabusClientException
   */
  private void create52StyleTestClient(Properties props)
		  throws IOException, DatabusException, DatabusClientException
		  {
	  DatabusHttpV3ClientImpl.StaticConfigBuilder configBuilder =
			  new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  ConfigLoader<DatabusHttpV3ClientImpl.StaticConfig> configLoader =
			  new ConfigLoader<DatabusHttpV3ClientImpl.StaticConfig>("databus.espresso.client.",
					  configBuilder);
	  // consumer specific config
	  DatabusFileLoggingConsumer.StaticConfigBuilder consumerConfBuilder =
			  new DatabusFileLoggingConsumer.StaticConfigBuilder();
	  ConfigLoader<DatabusFileLoggingConsumer.StaticConfig> consumerConfigLoader =
			  new ConfigLoader<DatabusFileLoggingConsumer.StaticConfig>("databus.espresso.testconsumer.",
					  consumerConfBuilder);

	  configLoader.loadConfig(props);
	  consumerConfigLoader.loadConfig(props);

	  _clientConf = configBuilder.build();
	  _consumerConf = consumerConfBuilder.build();

	  // Settings that must be different for each client
	  // we use settings in the prop file as base values
	  int basePort = _clientConf.getContainer().getHttpPort();
	  String dumpRootDirectory = _consumerConf.getValueDumpFile();
	  String traceRootDirectory = _clientConf.getConnectionDefaults().getEventBuffer().getTrace().getFilename();

	  LOG.info("base_port: " + basePort);
	  LOG.info("dump_file: " + dumpRootDirectory);
	  LOG.info("trace_file: " + traceRootDirectory);

	  // Subscription for the
	  String dbNamesProp;
	  if (! _clientConf.getClusterManager().getEnabled())
		  dbNamesProp = _clientConf.getRuntime().getRelay("1").getSources();
	  else
		  dbNamesProp = _clientConf.getSubscriptions();

	  if(dbNamesProp == null)
		  throw new IllegalArgumentException("sources/subs are not setup in the prop file");

	  String [] allDBs = dbNamesProp.split(",");
	  _clients = new DatabusHttpV3ClientImpl[allDBs.length];
	  int i=0;
	  for(String dbNameString : allDBs) {
		  dbNameString = dbNameString.trim();
		  String [] dbNameParts = dbNameString.split(":");
		  int partId = 0; // default value
		  if(dbNameParts.length != 2) {
			  LOG.warn("sources/subs names are not setup in the prop file. Using ppart id=0");
			  dbNameString = dbNameString + ":0";
		  } else {
			  partId = Integer.parseInt(dbNameParts[1]);
		  }
		  String logicalDbName = dbNameParts[0];
		  String [] logicalDbNameParts = logicalDbName.split("\\.");
		  if(logicalDbNameParts.length != 2)
			  throw new IllegalArgumentException("sources/subs are in wrong format in the prop file");
		  String dbName = logicalDbNameParts[0];

		  configBuilder.getContainer().setHttpPort(basePort + i);
		  // need i in all of the following directory settings for the case we have multiple
		  // consumers for the same partition
		  //if(cpRootDirectory != null)
		  //  configBuilder.getCheckpointPersistence().getFileSystem().setRootDirectory(
		  //                      cpRootDirectory + "/" + dbNameString + "_" + i);
		  if(dumpRootDirectory != null)
			  consumerConfBuilder.setValueDumpFile(dumpRootDirectory + "." + dbName + "_" + partId);
		  if(traceRootDirectory != null)
			  configBuilder.getConnectionDefaults().getEventBuffer().getTrace().setFilename(
					  traceRootDirectory + "." + dbName + "_" + partId);

		  if (! _clientConf.getClusterManager().getEnabled())
			  configBuilder.getRuntime().getRelay("1").setSources(dbNameString);

		  DatabusHttpV3ClientImpl.StaticConfig clientConf = configBuilder.build();
		  DatabusFileLoggingConsumer.StaticConfig consumerConf = consumerConfBuilder.build();

		  _clients[i] = new DatabusHttpV3ClientImpl(clientConf);

		  //LoggingConsumer _consumer = _client.getLoggingListener();

		  DatabusSubscription ds = createDatabusSubscription(partId, dbName);
		  LOG.info("subscribing for pid=" + partId + ";db=" + dbName + ";ds="+ds);

		  EspressoTestStreamConsumer consumer = new EspressoTestStreamConsumer("econsumer1", consumerConf);
		  _clients[i].registerDatabusListener(consumer, null, null, ds);
		  LOG.info("creating client: " + _clients[i] + "consumer=" + consumer + ";ds=" + ds);
		  i++;
	  }
  }

  /**
   * Creates a 5.3 style client, wherein
   * (1) A different client would be created per subscription
   * @param props
   * @throws IOException
   * @throws DatabusException
   * @throws DatabusClientException
   */
  private void create53StyleTestClient(Properties props)
  throws IOException, DatabusException, DatabusClientException
  {
	  // client config
	  DatabusHttpV3ClientImpl.StaticConfigBuilder configBuilder =  new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  ConfigLoader<DatabusHttpV3ClientImpl.StaticConfig> configLoader = new ConfigLoader<DatabusHttpV3ClientImpl.StaticConfig>("databus.espresso.client.", configBuilder);
	  configLoader.loadConfig(props);
	  _clientConf = configBuilder.build();

	  // Validate subscriptions obtained from _clientConf
	  String subsProp;
	  if (! _clientConf.getClusterManager().getEnabled())
	  {
		  subsProp = _clientConf.getRuntime().getRelay("1").getSources();
		  LOG.info("Set subsProp as " + subsProp);
	  }
	  else
	  {
		  subsProp = _clientConf.getSubscriptions();
		  LOG.info("Set subsProp as " + subsProp);
	  }
	  if(subsProp == null)
		  throw new IllegalArgumentException("sources/subs are not setup in the prop file");

	  // client configuration parameters are valid. Build the client
	  _v3client = new DatabusHttpV3ClientImpl(_clientConf);

	  // Build data structures for being able to build 'n' consumers
	  String [] allSubscriptions = subsProp.split(",");
	  EspressoTestStreamConsumer[] v3consumers = new EspressoTestStreamConsumer[allSubscriptions.length];
	  int i = 0;
	  for(String dbNameString : allSubscriptions) {
		  String[] dbNameParts;
		  String logicalDbName;
		  String[] logicalDbNameParts;
		  String dbName = "";
		  int partId = 0; // default value
		  DatabusSubscription ds=null;
		  try
		  {
			  dbNameString = dbNameString.trim();
			  dbNameParts = dbNameString.split(":");
			  if(dbNameParts.length != 2) {
				  throw new IllegalArgumentException("Should have two parts in the format EspressoDB.*:1, but is of the form" + dbNameParts);
			  } else {
				  partId = Integer.parseInt(dbNameParts[1]);
			  }
			  logicalDbName = dbNameParts[0];
			  logicalDbNameParts = logicalDbName.split("\\.");
			  if(logicalDbNameParts.length != 2)
				  throw new IllegalArgumentException("sources/subs are in wrong format in the prop file " + logicalDbNameParts);
			  dbName = logicalDbNameParts[0];
			  ds =  createDatabusSubscription(partId, dbName);
		  } catch (NumberFormatException ne)
		  {
			  EspressoSubscriptionUriCodec es = EspressoSubscriptionUriCodec.getInstance();
			  URI u = null;
			  try { u = new URI(dbNameString);} catch (URISyntaxException ue) {}
			  dbName = es.decode(u).getPhysicalPartition().getName();
			  partId = es.decode(u).getPhysicalPartition().getId();
			  try {
				  ds = DatabusSubscription.createFromUri(dbNameString);
			  } catch (URISyntaxException e){LOG.error("Error creating subscription from URI", e);}
		  }

		  // consumer specific config
		  DatabusFileLoggingConsumer.StaticConfigBuilder consumerConfBuilder = new DatabusFileLoggingConsumer.StaticConfigBuilder();
		  ConfigLoader<DatabusFileLoggingConsumer.StaticConfig> consumerConfigLoader =
				  new ConfigLoader<DatabusFileLoggingConsumer.StaticConfig>("databus.espresso.testconsumer.", consumerConfBuilder);
		  consumerConfigLoader.loadConfig(props);

		  // Value dump file
		  String valueDumpFile = consumerConfBuilder.getValueDumpFile();
		  if(valueDumpFile != null)
		  {
			  consumerConfBuilder.setValueDumpFile(valueDumpFile + "." + dbName + "_" + partId);
		  	  LOG.info("dump_file: " + valueDumpFile);
		  }

		  LOG.info("subscribing for pid=" + partId + ";db=" + dbName + ";ds="+ds);

		  // Create consumer
		  DatabusFileLoggingConsumer.StaticConfig consumerConf = consumerConfBuilder.build();
		  v3consumers[i] = new EspressoTestStreamConsumer("econsumer1", consumerConf);

		  // Create registration for consumer - subscription
		  DatabusV3Registration regObj = _v3client.registerDatabusListener(v3consumers[i], null, null, ds);
		  regObj.start();
		  LOG.info("On client: " + _v3client + "consumer=" + v3consumers[i] + ";ds=" + ds + ";regsitrationObject=" + regObj.getId().getId());
		  i++;
  		}
	}

  /**
   *
   * @param props
   * @throws IOException
   * @throws DatabusException
   * @throws DatabusClientException
   */
  private void createMultiPartitionConsumerTestClient(Properties props)
  throws IOException, DatabusException, DatabusClientException
  {
	  // client config
	  
	  DatabusHttpV3ClientImpl.StaticConfigBuilder configBuilder =  new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  ConfigLoader<DatabusHttpV3ClientImpl.StaticConfig> configLoader = new ConfigLoader<DatabusHttpV3ClientImpl.StaticConfig>("databus.espresso.client.", configBuilder);
	  configLoader.loadConfig(props);
	  _clientConf = configBuilder.build();

	  // Validate subscriptions obtained from _clientConf
	  String subsProp;
	  boolean ccIgnoreCheckSCNHoles = false;
	  
	  if (! _clientConf.getClusterManager().getEnabled())
	  {
		  throw new DatabusException("For this test, we expect helix integration");
	  }
	  else
	  {
		  subsProp = _clientConf.getSubscriptions();
		  LOG.info("Set subsProp as " + subsProp);
	  }
	  ccIgnoreCheckSCNHoles = _clientConf.getIgnoreSCNHolesCheck();
	  
	  if ( ccIgnoreCheckSCNHoles == true) {
		  LOG.warn("Setting SCN Hole Checking to False in the Consistancy Checker");
	  }
	  
	  if(subsProp == null)
		  throw new IllegalArgumentException("sources/subs are not setup in the prop file");

	  // Build data structures for being able to build 'n' consumers
	  String [] allSubscriptions = subsProp.split(",");
	  if (allSubscriptions.length < 1)
	  {
		  throw new IllegalArgumentException("Incorrect number of subscriptions");
	  }
	  URI u = null;
	  try { u = new URI(allSubscriptions[0]);} catch (URISyntaxException ue) {}
	  String dbName = EspressoSubscriptionUriCodec.getInstance().decode(u).getPhysicalPartition().getName();

	  List<DatabusSubscription> dsl = null;
	  try
	  {
		  dsl = DatabusSubscription.createFromUriList(Arrays.asList(allSubscriptions));
	  } catch (URISyntaxException ue)
	  {
		  throw new IllegalArgumentException(ue);
	  }

	  // client configuration parameters are valid. Build the client
	  _v3client = new DatabusHttpV3ClientImpl(_clientConf);

	  // consumer specific config
	  DatabusFileLoggingConsumer.StaticConfigBuilder consumerConfBuilder = new DatabusFileLoggingConsumer.StaticConfigBuilder();
	  ConfigLoader<DatabusFileLoggingConsumer.StaticConfig> consumerConfigLoader =
			  new ConfigLoader<DatabusFileLoggingConsumer.StaticConfig>("databus.espresso.testconsumer.", consumerConfBuilder);
	  consumerConfigLoader.loadConfig(props);

	  // Value dump file for all consumer output from a given set of subscriptions
	  // Since they all have to belong to an EspressoDB, they are just prefixed with dbName
	  // Note that this will not work if there are two multipartition subscriptions for same DB ..
	  // will have to cross that when we get to testing it
	  String valueDumpFile = consumerConfBuilder.getValueDumpFile();
	  if(valueDumpFile != null)
	  {
		  consumerConfBuilder.setValueDumpFile(valueDumpFile + "." + dbName );
	  	  LOG.info("dump_file: " + valueDumpFile);
	  }

	  DatabusFileLoggingConsumer.StaticConfig consumerConf = consumerConfBuilder.build();
	  EspressoTestStreamConsumer dec = new EspressoTestStreamConsumer("multiPartitionConsumerDelegate", consumerConf);

	  DatabusSubscription.registerUriCodec(EspressoSubscriptionUriCodec.getInstance());

	  RegistrationId rid = new RegistrationId("multiPartitionConsumer");

	  // Create registration for consumer - subscription
	  DatabusSubscription[] subList = dsl.toArray(new DatabusSubscription[dsl.size()]);

	  long maxPartitionTimeMs = 500;
	  String cluster =  _clientConf.getClusterManager().getRelayClusterName();
	  String zk = _clientConf.getClusterManager().getRelayZkConnectString();

	  EspressoTestValidatingConsumer multiConsumer = new EspressoTestValidatingConsumer(dec, LOG, maxPartitionTimeMs, false, zk,cluster,dbName,ccIgnoreCheckSCNHoles, subList);

	  DatabusV3Registration regObj = _v3client.registerDatabusListener(multiConsumer, rid, null, subList);

/*
	  long scn = 0;
	  int partitionId = 0;
	  setCheckpoint(scn, regObj, Arrays.asList(subList), partitionId);
*/
	  regObj.start();
}

  public static DatabusSubscription createDatabusSubscription(int partId, String dbName) {
  	/**
  	 * Build the DatabusSubscription object based on information from properties / ClusterManager
  	 */
  	PhysicalSource ps = PhysicalSource.ANY_PHISYCAL_SOURCE;
  	LogicalSourceId.Builder lidB = new LogicalSourceId.Builder();
  	lidB.setId((short)partId);
  	LogicalSourceId lid = lidB.build();
  	PhysicalPartition pp = new PhysicalPartition(partId, dbName);
  	DatabusSubscription ds = new DatabusSubscription(ps, pp, lid);
  	return ds;
  }


  @Override
  public void shutdown()
  {
    LOG.info("shutdown request");
    for(DatabusHttpV3ClientImpl client : _clients) {
      client.shutdown();
      LOG.info("shutdown request: out for client=" + client);
    }
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    LOG.info("waiting for shutdown");
    if ((null != _clients) && (_clients.length > 0))
    {
    	for(DatabusHttpV3ClientImpl client : _clients) {
    		client.awaitShutdown();
    		LOG.info("waiting for shutdown: out for client=" + client);
    	}
    }
    else
    {
    	assert(null != _v3client);
        _v3client.awaitShutdown();
    }
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    LOG.info("waiting for shutdown");
    try
    {
    	if ((null != _clients) && (_clients.length > 0))
    	{
    		for(DatabusHttpV3ClientImpl client : _clients) {
    			client.awaitShutdown(timeout);
    			LOG.info("waiting for shutdown: out for client=" + client);
    		}
    	}
    	else
    	{
        	assert(null != _v3client);
    		_v3client.awaitShutdown();
    	}
    }
    catch (TimeoutException e)
    {
      throw new org.jboss.netty.handler.timeout.TimeoutException(e);
    }

  }

  @Override
  public void start() throws Exception
  {
    LOG.info("starting");
    if ((null != _clients) && (_clients.length > 0))
    {
    	for(DatabusHttpV3ClientImpl client : _clients) {
    	    client.registerShutdownHook();
    		client.start();
    		LOG.info("starting: out for client=" + client);
    	}
    }
    else
    {
    	assert(null != _v3client);
    	_v3client.start();
    }
  }

  public DatabusHttpClientImpl getClientImpl(int idx)
  {
	    if ((null != _clients) && (_clients.length > 0))
	    {
	    	return _clients[idx];
	    }
	    else
	    {
	    	assert( null != _v3client);
	    	return _v3client;
	    }
  }

  public static void main(String[] args) throws Exception
  {
    EspressoTestDatabusClient.Cli cli = new EspressoTestDatabusClient.Cli();
    cli.processCommandLineArgs(args);

    if (cli.getValueDumpFile() != null)
    {
      cli.getConfigProps().setProperty("databus.espresso.testconsumer.valueDumpFile",
                                       cli.getValueDumpFile());
    }

    EspressoTestDatabusClient client = new EspressoTestDatabusClient(cli.getConfigProps(), null);
    client.start();
    client.waitForShutdown();
  }

  @SuppressWarnings("resource")
  private void setCheckpoint(long scn,
		  							   DatabusV3Registration reg,
		  							   List<DatabusSubscription> ds,
		  							   int partitionId)
  throws IOException
  {
	  CheckpointPersistenceProvider ckp = reg.getCheckpoint();
	  Checkpoint cp = new Checkpoint();
	  cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
	  if (scn < 0)
	  {
		  throw new IOException("Error checkpointing SCN: " + scn + " for partition " + partitionId);
	  }
	  cp.setWindowScn(scn);
	  cp.setWindowOffset(-1);
	  RegistrationId regId = reg.getId();
	  ckp.storeCheckpointV3(ds, cp, regId);
	  LOG.info("set checkpoint at scn " + cp.getWindowScn());
	  return;
	}


  public static class Cli extends DatabusHttpV3ClientImpl.Cli
  {
    public static final String VALUE_DUMP_FILE_OPT_NAME = "value_file";
    public static final char VALUE_DUMP_FILE_OPT_CHAR = 'V';

    private String _valueDumpFile;

    public Cli()
    {
      super("java " + EspressoTestDatabusClient.class.getName() + " [options]");
    }

    public String getValueDumpFile()
    {
      return _valueDumpFile;
    }

    @SuppressWarnings("static-access")
    @Override
    protected void constructCommandLineOptions()
    {
      super.constructCommandLineOptions();

      Option valueDumpFileOption = OptionBuilder.withLongOpt(VALUE_DUMP_FILE_OPT_NAME)
                                                .withDescription("File to dump deserilized values")
                                                .hasArg()
                                                .withArgName("file")
                                                .create(VALUE_DUMP_FILE_OPT_CHAR);

      _cliOptions.addOption(valueDumpFileOption);
    }

    @Override
    public void processCommandLineArgs(String[] cliArgs) throws IOException,
        DatabusException
    {
      super.processCommandLineArgs(cliArgs);
      if (_cmd.hasOption(VALUE_DUMP_FILE_OPT_CHAR))
      {
        _valueDumpFile = _cmd.getOptionValue(VALUE_DUMP_FILE_OPT_CHAR);
      }

    }

  }


  class EspressoTestStreamConsumer  extends DatabusFileLoggingConsumer
                                    implements DatabusV3Consumer
  {
    private final String _name;

    public EspressoTestStreamConsumer(String name, DatabusFileLoggingConsumer.StaticConfig config)
           throws IOException
    {
      super(config);
      _name = name;
    }

    @Override
    public ConsumerCallbackResult onCheckpoint(SCN checkpointScn) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: On Checkpoint: " + checkpointScn);
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: got_event" + e + " eventDecoder = " + eventDecoder.getPayloadSchema(e).getId());
      return super.onDataEvent(e, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onEndDataEventSequence: " + endScn);
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onEndSource:"  + source);
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onRollback(SCN startScn) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onRollback: " + startScn);
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartDataEventSequence(SCN startScn) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onStartDataEventSequence: " + startScn);
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartSource(String srcSub, Schema sourceSchema) {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onStartSource: "  + srcSub);
      return ConsumerCallbackResult.SUCCESS;
    }

	public String getName() {
		return _name;
	}

    @Override
    public ConsumerCallbackResult onStartConsumption() {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onStartConsumption" );
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStopConsumption()
    {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onStopConsumption");
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onError(Throwable err)
    {
      if (LOG.isDebugEnabled())
        LOG.debug("TEST_CONSUMER: onError: " + err);
      return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public boolean canBootstrap()
    {
    	return false;
    }

    @Override
    public ConsumerCallbackResult onStartBootstrap()
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStopBootstrap()
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartBootstrapSource(String sourceName, Schema sourceSchema)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onBootstrapError(Throwable err)
    {
    	return ConsumerCallbackResult.SUCCESS;
    }

  }
 }
