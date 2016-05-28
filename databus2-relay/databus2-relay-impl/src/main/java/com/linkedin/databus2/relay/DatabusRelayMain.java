/*
 * $Id: DatabusRelayMain.java 261967 2011-04-19 02:54:35Z cbotev $
 */
package com.linkedin.databus2.relay;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.ControlSourceEventsRequestProcessor;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.EventProducerServiceProvider;
import com.linkedin.databus2.producers.RelayEventProducer;
import com.linkedin.databus2.producers.RelayEventProducersRegistry;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.SourceType;
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


    private final RelayEventProducersRegistry _producersRegistry = RelayEventProducersRegistry.getInstance();
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
    if(uri == null)
      throw new DatabusException("Uri is required to start the relay");
    uri = uri.trim();
		EventProducer producer = null;
		if (uri.startsWith("jdbc:")) {
		  SourceType sourceType = pConfig.getReplBitSetter().getSourceType();
          if (SourceType.TOKEN.equals(sourceType))
            throw new DatabusException("Token Source-type for Replication bit setter config cannot be set for trigger-based Databus relay !!");

			// if a buffer for this partiton exists - we are overwri
			producer = new OracleEventProducerFactory().buildEventProducer(
					pConfig, schemaRegistryService, dbusEventBuffer,
					getMbeanServer(), _inBoundStatsCollectors
							.getStatsCollector(statsCollectorName),
					maxScnReaderWriters);
		} else if (uri.startsWith("mock")) {
		  // Get all relevant pConfig attributes
		  //TODO add real instantiation
		  EventProducerServiceProvider mockProvider = _producersRegistry.getEventProducerServiceProvider("mock");
		  if (null == mockProvider)
		  {
		    throw new DatabusRuntimeException("relay event producer not available: " + "mock");
		  }
		  producer = mockProvider.createProducer(pConfig, schemaRegistryService,
		                                         dbusEventBuffer,
		                                         _inBoundStatsCollectors
		                                                 .getStatsCollector(statsCollectorName),
		                                         maxScnReaderWriters);
		} else if (uri.startsWith("gg:")){
      producer = new GoldenGateEventProducer(pConfig,
                                             schemaRegistryService,
                                             dbusEventBuffer,
                                             _inBoundStatsCollectors
                                                 .getStatsCollector(statsCollectorName),
                                             maxScnReaderWriters);

    } else if (uri.startsWith("mysql:")){
       LOG.info("Adding OpenReplicatorEventProducer for uri :" + uri);
       final String serviceName = "or";
       EventProducerServiceProvider orProvider = _producersRegistry.getEventProducerServiceProvider(serviceName);
       if (null == orProvider)
       {
         throw new DatabusRuntimeException("relay event producer not available: " + serviceName);
       }
       producer = orProvider.createProducer(pConfig, schemaRegistryService,
                                            dbusEventBuffer,
                                            _inBoundStatsCollectors.getStatsCollector(statsCollectorName),
                                            maxScnReaderWriters);
    } else if (uri.startsWith("kafka:")){
      EventProducerServiceProvider orProvider = _producersRegistry.getEventProducerServiceProvider("kafka");
      if (null == orProvider)
      {
        throw new DatabusRuntimeException("relay event producer not available: " + "kafka");
      }
      producer = orProvider.createProducer(pConfig, schemaRegistryService,
                                           dbusEventBuffer,
                                           _inBoundStatsCollectors.getStatsCollector(statsCollectorName),
                                           maxScnReaderWriters);
    } else
     {
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
					((OracleEventProducer) producer).getMonitoredSourceInfos(),
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

  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli();
    cli.processCommandLineArgs(args);
    cli.parseRelayConfig();
    // Process the startup properties and load configuration
    PhysicalSourceStaticConfig[] pStaticConfigs = cli.getPhysicalSourceStaticConfigs();
    HttpRelay.StaticConfig staticConfig = cli.getRelayConfigBuilder().build();

    // Create and initialize the server instance
    DatabusRelayMain serverContainer = new DatabusRelayMain(staticConfig, pStaticConfigs);

    serverContainer.initProducers();
    serverContainer.registerShutdownHook();
    serverContainer.startAndBlock();
  }

	@Override
	protected void doStart() {
		super.doStart();

		LOG.info("Starting. Producers are :" + _producers);

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
