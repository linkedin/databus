/*
 * $Id: DatabusRelayMain.java 261967 2011-04-19 02:54:35Z cbotev $
 */
package com.linkedin.databus2.relay;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.ControlSourceEventsRequestProcessor;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.RelayEventProducer;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * Main class for a Databus Relay.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 261967 $
 */
public class DatabusRelayMain extends HttpRelay {
	public static final Logger LOG = Logger.getLogger(DatabusRelayMain.class
			.getName());
	public static final String DB_RELAY_CONFIG_FILE_OPT_NAME = "db_relay_config";

	protected static String[] _dbRelayConfigFiles = new String[] { "integration-test/config/sources-member2.json" };

	MultiServerSequenceNumberHandler _maxScnReaderWriters;
	protected Map<PhysicalPartition, EventProducer> _producers;
	Map<PhysicalPartition, MonitoringEventProducer> _monitoringProducers;
	ControlSourceEventsRequestProcessor _csEventRequestProcessor;

  private boolean _dbPullerStart = false;

	public DatabusRelayMain() throws IOException, InvalidConfigException,
			DatabusException {
		this(new HttpRelay.Config(), null);
	}

	public DatabusRelayMain(HttpRelay.Config config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		this(config.build(), pConfigs);
	}

	public DatabusRelayMain(HttpRelay.StaticConfig config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		super(config, pConfigs);
		SequenceNumberHandlerFactory handlerFactory = _relayStaticConfig
				.getDataSources().getSequenceNumbersHandler().createFactory();
		_maxScnReaderWriters = new MultiServerSequenceNumberHandler(
				handlerFactory);
		_producers = new HashMap<PhysicalPartition, EventProducer>(
				_pConfigs.size());
		_monitoringProducers = new HashMap<PhysicalPartition, MonitoringEventProducer>(_pConfigs.size());
		_dbPullerStart = false;
	}

	public void setDbPullerStart(boolean s) {
		_dbPullerStart = s;
	}

	public boolean getDbPullerStart() {
		return _dbPullerStart;
	}

	/** overrides HTTP relay method */
	@Override
	public void removeOneProducer(PhysicalSourceStaticConfig pConfig) {
		PhysicalPartition pPartition = pConfig.getPhysicalPartition();

		List<EventProducer> plist = new ArrayList<EventProducer>();

		if (_producers != null && _producers.containsKey(pPartition))
			plist.add(_producers.remove(pPartition));

		// if(_maxScnReaderWriters != null &&
		// _maxScnReaderWriters.getHandler(pPartition)
		// _maxScnReaderWriters.remove();

		if (_monitoringProducers != null
				&& _monitoringProducers.containsKey(pPartition))
			plist.add(_monitoringProducers.remove(pPartition));

		if (plist.size() > 0 && _csEventRequestProcessor != null)
			_csEventRequestProcessor.removeEventProducers(plist);
	}

	/** overrides HTTP relay method */
	@Override
	public void addOneProducer(PhysicalSourceStaticConfig pConfig)
			throws DatabusException, EventCreationException,
			UnsupportedKeyException, SQLException, InvalidConfigException {

		// Register a command to allow start/stop/status of the relay
		List<EventProducer> plist = new ArrayList<EventProducer>();

		PhysicalPartition pPartition = pConfig.getPhysicalPartition();
		MaxSCNReaderWriter maxScnReaderWriters = _maxScnReaderWriters
				.getOrCreateHandler(pPartition);
		LOG.info("Starting server container with maxScnReaderWriter:"
				+ maxScnReaderWriters);

		// Get the event buffer
		DbusEventBufferAppendable dbusEventBuffer = getEventBuffer()
				.getDbusEventBufferAppendable(pPartition);

		// Get the schema registry service
		SchemaRegistryService schemaRegistryService = getSchemaRegistryService();

		// Get a stats collector per physical source
		addPhysicalPartitionCollectors(pPartition);
		String statsCollectorName = pPartition.toSimpleString();
		/*
		 * _inBoundStatsCollectors.addStatsCollector(statsCollectorName, new
		 * DbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
		 * statsCollectorName+".inbound", true, false, getMbeanServer()));
		 *
		 * _outBoundStatsCollectors.addStatsCollector(statsCollectorName, new
		 * DbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
		 * statsCollectorName+".outbound", true, false, getMbeanServer()));
		 */

		// Create the event producer
		String uri = pConfig.getUri();
		EventProducer producer = null;
		if (uri.startsWith("jdbc:")) {
			// if a buffer for this partiton exists - we are overwri
			producer = new OracleEventProducerFactory().buildEventProducer(
					pConfig, schemaRegistryService, dbusEventBuffer,
					getMbeanServer(), _inBoundStatsCollectors
							.getStatsCollector(statsCollectorName),
					maxScnReaderWriters);
		} else if (uri.startsWith("mock")) {
			// Get all relevant pConfig attributes
			producer = new RelayEventGenerator(pConfig, schemaRegistryService,
					dbusEventBuffer,
					_inBoundStatsCollectors
							.getStatsCollector(statsCollectorName),
					maxScnReaderWriters);
		} else {
			// Get all relevant pConfig attributes and initialize the nettyThreadPool objects
			RelayEventProducer.DatabusClientNettyThreadPools nettyThreadPools =
						new RelayEventProducer.DatabusClientNettyThreadPools(0,getNetworkTimeoutTimer(),getBossExecutorService(),
																				getIoExecutorService(), getHttpChannelGroup());
			producer = new RelayEventProducer(pConfig, dbusEventBuffer,
					_inBoundStatsCollectors
							.getStatsCollector(statsCollectorName),
					maxScnReaderWriters,nettyThreadPools);
		}

		// if a buffer for this partiton exists - we are overwriting it.
		_producers.put(pPartition, producer);

		plist.add(producer);
		// append 'monitoring event producer'
		if (producer instanceof OracleEventProducer) {
			MonitoringEventProducer monitoringProducer = new MonitoringEventProducer(
					"dbMonitor." + pPartition.toSimpleString(),
					pConfig.getName(), pConfig.getUri(),
					((OracleEventProducer) producer).getSources(),
					getMbeanServer());
			_monitoringProducers.put(pPartition, monitoringProducer);
			plist.add(monitoringProducer);
		}

		if (_csEventRequestProcessor == null)
			_csEventRequestProcessor = new ControlSourceEventsRequestProcessor(
					null, this, plist);
		else
			_csEventRequestProcessor.addEventProducers(plist);

		RequestProcessorRegistry processorRegistry = getProcessorRegistry();
		processorRegistry.reregister(
				ControlSourceEventsRequestProcessor.COMMAND_NAME,
				_csEventRequestProcessor);
	}

	public void initProducers() throws InvalidConfigException,
			DatabusException, EventCreationException, UnsupportedKeyException,
			SQLException, ProcessorRegistrationConflictException {
		LOG.info("initializing producers");

		for (PhysicalSourceStaticConfig pConfig : _pConfigs) {
			addOneProducer(pConfig);
		}
		this.setDbPullerStart(_relayStaticConfig.getStartDbPuller());
		LOG.info("done initializing producers");
	}

	/** get maxScnReaderWriters given a physical source **/
	public MaxSCNReaderWriter getMaxSCNReaderWriter(PhysicalSourceStaticConfig pConfig)
	{
		try
		{
			MaxSCNReaderWriter maxScnReaderWriters = _maxScnReaderWriters
				.getOrCreateHandler(pConfig.getPhysicalPartition());
			return maxScnReaderWriters;
		}
		catch (DatabusException e)
		{
			LOG.warn("Cannot get maxScnReaderWriter for " + pConfig.getPhysicalPartition() +  " error=" + e);
		}
		return null;
	}

	private static Options constructCommandLineOptions() {
		Options options = new Options();
		options.addOption(DB_RELAY_CONFIG_FILE_OPT_NAME, true,
				"Db Relay Config File");
		return options;
	}

	public static String[] processLocalArgs(String[] cliArgs)
			throws IOException, ParseException {
		CommandLineParser cliParser = new GnuParser();
		Options cliOptions = constructCommandLineOptions();

		CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);
		// Options here has to be up front
		if (cmd.hasOption(DB_RELAY_CONFIG_FILE_OPT_NAME)) {
			String opt = cmd.getOptionValue(DB_RELAY_CONFIG_FILE_OPT_NAME);
			LOG.info("DbConfig command line=" + opt);
			_dbRelayConfigFiles = opt.split(",");
			LOG.info("DB Relay Config File = "
					+ Arrays.toString(_dbRelayConfigFiles));
		}

		// return what left over args
		return cmd.getArgs();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String[] leftOverArgs = processLocalArgs(args);

		// Process the startup properties and load configuration
		Properties startupProps = ServerContainer
				.processCommandLineArgs(leftOverArgs);
		Config config = new Config();
		ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>(
				"databus.relay.", config);

		// read physical config files
		ObjectMapper mapper = new ObjectMapper();
		PhysicalSourceConfig[] physicalSourceConfigs = new PhysicalSourceConfig[_dbRelayConfigFiles.length];
		PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[physicalSourceConfigs.length];

		int i = 0;
		for (String file : _dbRelayConfigFiles) {
			LOG.info("processing file: " + file);
			File sourcesJson = new File(file);
			PhysicalSourceConfig pConfig = mapper.readValue(sourcesJson,
					PhysicalSourceConfig.class);
			pConfig.checkForNulls();
			physicalSourceConfigs[i] = pConfig;
			pStaticConfigs[i] = pConfig.build();

			// Register all sources with the static config
			for (LogicalSourceConfig lsc : pConfig.getSources()) {
				config.setSourceName("" + lsc.getId(), lsc.getName());
			}
			i++;
		}

		HttpRelay.StaticConfig staticConfig = staticConfigLoader
				.loadConfig(startupProps);

		// Create and initialize the server instance
		DatabusRelayMain serverContainer = new DatabusRelayMain(staticConfig,
				pStaticConfigs);

		serverContainer.initProducers();
		serverContainer.registerShutdownHook();
		serverContainer.startAndBlock();
	}

	@Override
	protected void doStart() {
		super.doStart();

		for (Entry<PhysicalPartition, EventProducer> entry : _producers
				.entrySet()) {
			EventProducer producer = entry.getValue();
			// now start the default DB puller thread; depending on
			// configuration setting / cmd line
			if (this.getDbPullerStart()) {
				if (producer != null) {
					LOG.info("starting db puller: " + producer.getName());
					producer.start(-1L);
					LOG.info("db puller started: " + producer.getName());
				}
			}
		}
	}

	@Override
	public void pause() {
		for (Entry<PhysicalPartition, EventProducer> entry : _producers
				.entrySet()) {
			EventProducer producer = entry.getValue();

			if (null != producer) {
				if (producer.isRunning()) {
					producer.pause();
					LOG.info("EventProducer :" + producer.getName()
							+ "  pause sent");
				} else if (producer.isPaused()) {
					LOG.info("EventProducer :" + producer.getName()
							+ "  already paused");
				}
			}
		}
	}

	@Override
	public void resume() {
		for (Entry<PhysicalPartition, EventProducer> entry : _producers
				.entrySet()) {
			EventProducer producer = entry.getValue();
			if (null != producer) {
				if (producer.isPaused()) {
					producer.unpause();
					LOG.info("EventProducer :" + producer.getName()
							+ "  resume sent");
				} else if (producer.isRunning()) {
					LOG.info("EventProducer :" + producer.getName()
							+ "  already running");
				}
			}
		}
	}
	

	@Override
	protected void doShutdown() {
		LOG.warn("Shutting down Relay!");
		for (Entry<PhysicalPartition, EventProducer> entry : _producers
				.entrySet()) {
			PhysicalPartition pPartition = entry.getKey();
			EventProducer producer = entry.getValue();

			if (null != producer
					&& (producer.isRunning() || producer.isPaused())) {
				producer.shutdown();
				try {
					producer.waitForShutdown();
				} catch (InterruptedException ie) {
				}
				LOG.info("EventProducer is shutdown!");
			}

			MonitoringEventProducer monitoringProducer = _monitoringProducers
					.get(pPartition);
			if (monitoringProducer != null) {
				if (monitoringProducer.isRunning()) {
					monitoringProducer.shutdown();
				}

				while (monitoringProducer.isRunning()
						|| monitoringProducer.isPaused()) {
					try {
						monitoringProducer.waitForShutdown();
					} catch (InterruptedException ie) {
					}
				}
				monitoringProducer.unregisterMBeans();
			}
		}
		super.doShutdown();
	}

	public EventProducer[] getProducers() {
		EventProducer[] result = new EventProducer[_producers.size()];
		_producers.values().toArray(result);
		return result;
	}

	public MonitoringEventProducer[] getMonitoringProducers() {
		MonitoringEventProducer[] result = new MonitoringEventProducer[_monitoringProducers
				.size()];
		_monitoringProducers.values().toArray(result);
		return result;
	}

	@Override
	public void awaitShutdown() {
		super.awaitShutdown();
	}

}
