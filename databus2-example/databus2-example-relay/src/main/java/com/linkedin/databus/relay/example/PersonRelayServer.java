package com.linkedin.databus.relay.example;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class PersonRelayServer extends DatabusRelayMain {
  public static final String MODULE = PersonRelayServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  static final String FULLY_QUALIFIED_PERSON_EVENT_NAME = "com.linkedin.events.example.person.Person";
  static final int PERSON_SRC_ID = 40;

  MultiServerSequenceNumberHandler _maxScnReaderWriters;
  protected Map<PhysicalPartition, EventProducer> _producers;

  public PersonRelayServer() throws IOException, InvalidConfigException, DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public PersonRelayServer(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    this(config.build(), pConfigs);
  }

  public PersonRelayServer(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);

  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
	  
		_dbRelayConfigFiles = new String[] { "config/sources-person.json" };

		 String [] leftOverArgs = processLocalArgs(args);

		 // Process the startup properties and load configuration
		 Properties startupProps = ServerContainer.processCommandLineArgs(leftOverArgs);
		 Config config = new Config();
		 ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>("databus.relay.", config);

		 // read physical config files
		 ObjectMapper mapper = new ObjectMapper();
		 PhysicalSourceConfig [] physicalSourceConfigs = new PhysicalSourceConfig[_dbRelayConfigFiles.length];
		 PhysicalSourceStaticConfig [] pStaticConfigs =
				 new PhysicalSourceStaticConfig[physicalSourceConfigs.length];

		 int i = 0;
		 for(String file : _dbRelayConfigFiles) {
			 LOG.info("processing file: " + file);
			 File sourcesJson = new File(file);
			 PhysicalSourceConfig pConfig = mapper.readValue(sourcesJson, PhysicalSourceConfig.class);
			 pConfig.checkForNulls();
			 physicalSourceConfigs[i] = pConfig;
			 pStaticConfigs[i] = pConfig.build();

			 // Register all sources with the static config
			 for(LogicalSourceConfig lsc : pConfig.getSources()) {
				 config.setSourceName("" + lsc.getId(), lsc.getName());
			 }
			 i++;
		 }

		 HttpRelay.StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

		 // Create and initialize the server instance
		 DatabusRelayMain serverContainer = new PersonRelayServer(staticConfig, pStaticConfigs);

		 serverContainer.initProducers();
		 serverContainer.startAndBlock();	  
  }
  
}
