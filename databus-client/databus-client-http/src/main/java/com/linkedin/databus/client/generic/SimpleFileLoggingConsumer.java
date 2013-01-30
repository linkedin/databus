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


import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.registration.DatabusV2RegistrationImpl;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.ContainerOperationProcessor;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * Base class for setting up consumer, process arguments
 * @author dzhang
 *
 */
public class SimpleFileLoggingConsumer {

  public static final String MODULE = SimpleFileLoggingConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String RELAY_HOST_OPT_NAME = "relay_host";
  public static final String RELAY_PORT_OPT_NAME = "relay_port";
  public static final String EVENT_DUMP_FILE_OPT_NAME = "f";
  public static final String VALUE_DUMP_FILE_OPT_NAME = "value_file";
  public static final String HTTP_PORT_OPT_NAME = "http_port";
  public static final String JMX_SERVICE_PORT_OPT_NAME = "jmx_service_port";
  public static final String CHECKPOINT_DIR = "checkpoint_dir";
  public static final String BOOTSTRAP_HOST_OPT_NAME = "bootstrap_host";
  public static final String BOOTSTRAP_PORT_OPT_NAME = "bootstrap_port";
  public static final String EVENT_PATTERN_OPT_NAME = "event_pattern";
  public static final String FILTER_CONF_FILE_OPT_NAME = "filter_conf_file";
  public static final String SERVER_SIDE_FILTER_PREFIX = "serversidefilter.";

  private static String _eventDumpFile = null;
  private static String _valueDumpFile = null;
  private static String _relayHost = null;
  private static String _relayPort = null;
  private static String _bootstrapHost = null;
  private static String _bootstrapPort = null;
  private static String _httpPort = null;
  private static String _jmxServicePort = null;
  private static String _checkpointFileRootDir = null;
  private static String _eventPattern = null;
  private static boolean _enableBootStrap = false;
  private static String  _filterConfFile = null;

  protected static Options constructCommandLineOptions()
  {
    Options options = new Options();
    options.addOption(RELAY_HOST_OPT_NAME, true, "Relay Host Name");
    options.addOption(RELAY_PORT_OPT_NAME, true, "Relay Port");
    options.addOption(EVENT_DUMP_FILE_OPT_NAME, true, "File to dump event");
    options.addOption(VALUE_DUMP_FILE_OPT_NAME, true, "File to dump deserilized values");
    options.addOption(HTTP_PORT_OPT_NAME, true, "Consumer http port");
    options.addOption(JMX_SERVICE_PORT_OPT_NAME, true, "Consumer jmx service port");
    options.addOption(CHECKPOINT_DIR, true, "Checkpoint dir");
    options.addOption(BOOTSTRAP_HOST_OPT_NAME, true, "Bootstrap server Name");
    options.addOption(BOOTSTRAP_PORT_OPT_NAME, true, "Bootstrap server Port");
    options.addOption(EVENT_PATTERN_OPT_NAME, true, "Event Pattern Name to Check");
    options.addOption(FILTER_CONF_FILE_OPT_NAME, true, "Server Side Filter Config");
    return options;
  }

  protected static String[] processLocalArgs(String[] cliArgs) throws IOException, ParseException
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);
    // Options here has to be up front
    if (cmd.hasOption(RELAY_HOST_OPT_NAME))
    {
      _relayHost = cmd.getOptionValue(RELAY_HOST_OPT_NAME);
      LOG.info("Relay Host = " + _relayHost);
    }
    if (cmd.hasOption(RELAY_PORT_OPT_NAME))
    {
      _relayPort = cmd.getOptionValue(RELAY_PORT_OPT_NAME);
      LOG.info("Relay Port = " + _relayPort);
    }
    if (cmd.hasOption(EVENT_DUMP_FILE_OPT_NAME))
    {
      _eventDumpFile = cmd.getOptionValue(EVENT_DUMP_FILE_OPT_NAME);
      LOG.info("Saving event dump to file: " + _eventDumpFile);
    }
    if (cmd.hasOption(VALUE_DUMP_FILE_OPT_NAME))
    {
      _valueDumpFile = cmd.getOptionValue(VALUE_DUMP_FILE_OPT_NAME);
      LOG.info("Saving event value dump to file: " + _valueDumpFile);
    }
    if (cmd.hasOption(HTTP_PORT_OPT_NAME))
    {
      _httpPort = cmd.getOptionValue(HTTP_PORT_OPT_NAME);
      LOG.info("Consumer http port =  " + _httpPort);
    }
    if (cmd.hasOption(JMX_SERVICE_PORT_OPT_NAME))
    {
      _jmxServicePort = cmd.getOptionValue(JMX_SERVICE_PORT_OPT_NAME);
      LOG.info("Consumer JMX Service port =  " + _jmxServicePort);
    }
    if (cmd.hasOption(CHECKPOINT_DIR))
    {
      _checkpointFileRootDir = cmd.getOptionValue(CHECKPOINT_DIR);
      LOG.info("Checkpoint dir =  " + _checkpointFileRootDir);
    }
    if (cmd.hasOption(BOOTSTRAP_HOST_OPT_NAME))
    {
      _bootstrapHost = cmd.getOptionValue(BOOTSTRAP_HOST_OPT_NAME);
      LOG.info("Bootstrap Server = " + _bootstrapHost);
    }
    if (cmd.hasOption(BOOTSTRAP_PORT_OPT_NAME))
    {
      _bootstrapPort = cmd.getOptionValue(BOOTSTRAP_PORT_OPT_NAME);
      LOG.info("Bootstrap Server Port = " + _bootstrapPort);
    }
    if (cmd.hasOption(EVENT_PATTERN_OPT_NAME))
    {
      _eventPattern = cmd.getOptionValue(EVENT_PATTERN_OPT_NAME);
      LOG.info("Event pattern = " + _eventPattern);
    }
    if (_bootstrapHost != null || _bootstrapPort != null)
    {
      _enableBootStrap = true;
    }

    if ( cmd.hasOption(FILTER_CONF_FILE_OPT_NAME))
    {
    	_filterConfFile = cmd.getOptionValue(FILTER_CONF_FILE_OPT_NAME);
    	LOG.info("Server Side Filtering Config File =" + _filterConfFile);
    }

    // return what left over args
    return cmd.getArgs();
  }

  protected String[] addSources()
  {
    return null;
  }

  protected DatabusFileLoggingConsumer createTypedConsumer(String valueDumpFile) throws IOException
  {
    return new DatabusFileLoggingConsumer(valueDumpFile, false);
  }

  public void mainFunction(String args[]) throws Exception
  {
    String [] leftOverArgs = processLocalArgs(args);
    Properties startupProps = DatabusHttpClientImpl.processCommandLineArgs(leftOverArgs);
    DatabusHttpClientImpl.Config clientConfigBuilder = new DatabusHttpClientImpl.Config();

    clientConfigBuilder.getContainer().setIdFromName(MODULE + ".localhost");

    if (_enableBootStrap)
    {
    	clientConfigBuilder.getRuntime().getBootstrap().setEnabled(true);
    }
    ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("databus.client.", clientConfigBuilder);

    String[] sources = addSources();

    StringBuilder sourcesString = new StringBuilder();
    boolean firstSrc = true;
    for (String source: sources)
    {
      if (! firstSrc) sourcesString.append(",");
      firstSrc = false;
      sourcesString.append(source);
    }

    if (_httpPort != null)
    {
      startupProps.put("databus.client.container.httpPort", _httpPort);
    }
    if (_jmxServicePort != null)
    {
      startupProps.put("databus.client.container.jmx.jmxServicePort", _jmxServicePort);
    }
    if (_checkpointFileRootDir != null)
    {
      startupProps.put("databus.client.checkpointPersistence.fileSystem.rootDirectory", _checkpointFileRootDir);
    }
   
    DatabusHttpClientImpl.StaticConfig clientConfig = configLoader.loadConfig(startupProps);

    // set up relay
    ServerInfoBuilder relayBuilder = clientConfig.getRuntime().getRelay("1");
    relayBuilder.setName("DefaultRelay");
    if (_relayHost != null)
    {
      relayBuilder.setHost(_relayHost);
    }
    if (_relayPort != null)
    {
      relayBuilder.setPort(Integer.parseInt(_relayPort));
    }
    relayBuilder.setSources(sourcesString.toString());

    // set up bootstrap
    if (_enableBootStrap)
    {
      ServerInfoBuilder bootstrapBuilder = clientConfig.getRuntime().getBootstrap().getService("2");
      bootstrapBuilder.setName("DefaultBootstrapServices");
      if (_bootstrapHost != null)
      {
        bootstrapBuilder.setHost(_bootstrapHost);
      }
      if (_bootstrapPort != null)
      {
        bootstrapBuilder.setPort(Integer.parseInt(_bootstrapPort));
      }
      bootstrapBuilder.setSources(sourcesString.toString());
    }

    // handler server side filtering, can either pass a config file or set from command line
    DbusKeyCompositeFilterConfig filterConfig = createServerSideFilterConfig(_filterConfFile, startupProps);

    // set up listeners
    DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);
    

    DatabusFileLoggingConsumer consumer = createTypedConsumer(_valueDumpFile);
    if (_eventPattern != null)
    {
      consumer.setEventPattern(_eventPattern);
    }
    DatabusRegistration reg = client.register(consumer, sources);
    
    if (null != filterConfig)
    	reg.withServerSideFilter(filterConfig);
    
    if (reg instanceof DatabusV2RegistrationImpl)
    {
    	DatabusV2RegistrationImpl r = (DatabusV2RegistrationImpl)reg;
    	r.getLoggingConsumer().enableEventFileTrace(_eventDumpFile);
    } else {
    	throw new RuntimeException("Unexpected type for registration Object !!");
    }
    
    // add pause processor
    try
    {
      client.getProcessorRegistry().register(ConsumerPauseRequestProcessor.COMMAND_NAME,
                                                  new ConsumerPauseRequestProcessor(null, consumer));
      client.getProcessorRegistry().register(ContainerOperationProcessor.COMMAND_NAME,
                                 new ContainerOperationProcessor(null, client));
    }
    catch (ProcessorRegistrationConflictException e)
    {
      LOG.error("Failed to register " + ConsumerPauseRequestProcessor.COMMAND_NAME);
    }
    
    DatabusClientShutdownThread shutdownThread = new DatabusClientShutdownThread(client);
    Runtime.getRuntime().addShutdownHook(shutdownThread);
    
    client.startAndBlock();  //this should automatically start the registration
  }

  private DbusKeyCompositeFilterConfig createServerSideFilterConfig(String filterConfFile,
                                                                    Properties startupProps) throws IOException, InvalidConfigException
  {
    Boolean cmdLineHasFilterConfig = hasServerSideFilterConfig(startupProps);
    if ( filterConfFile != null || cmdLineHasFilterConfig)
    {
      Properties props;
      // cmdline override the config file
      if (cmdLineHasFilterConfig)
      {
        props = startupProps;
      }
      else
      {
        LOG.info("filterConfFile = " + filterConfFile);
        props = new Properties();

        FileInputStream filterConfStream = new FileInputStream(filterConfFile);
        try
        {
          props.load(filterConfStream);
        }
        finally
        {
          filterConfStream.close();
        }
      }

      DbusKeyCompositeFilterConfig.Config conf = new DbusKeyCompositeFilterConfig.Config();
      ConfigLoader<DbusKeyCompositeFilterConfig.StaticConfig> filterConfigLoader =
        new ConfigLoader<DbusKeyCompositeFilterConfig.StaticConfig>(SERVER_SIDE_FILTER_PREFIX, conf);
      DbusKeyCompositeFilterConfig.StaticConfig sConf = filterConfigLoader.loadConfig(props);

      return new DbusKeyCompositeFilterConfig(sConf);
    }
    else
    {
      return null;
    }
  }

  private Boolean hasServerSideFilterConfig(Properties startupProps)
  {
    Enumeration<Object> propKeys = startupProps.keys();
    while (propKeys.hasMoreElements())
    {
      String key = (String) propKeys.nextElement();
      if (key.contains(SERVER_SIDE_FILTER_PREFIX))
      {
        return true;
      }
    }
    return false;
  }
  
  static class DatabusClientShutdownThread extends Thread
  {
    public static final String MODULE = DatabusClientShutdownThread.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    private final ServerContainer _serverContainer;

    public DatabusClientShutdownThread(ServerContainer serverContainer)
    {
      super("ServerContainer Shutdown Thread");
      _serverContainer = serverContainer;
    }

    @Override
    public void run()
    {
      LOG.info("Starting shutdown procedure for server container...");
      try
      {
        if (null != _serverContainer && _serverContainer.isRunningStatus())
        {
          _serverContainer.shutdownUninteruptibly();
        }
        LOG.info("Server Container shutdown.");
      }
      catch (Exception e)
      {
        LOG.error("Error shutting down Server Container", e);
      }
    }
  } 
}
