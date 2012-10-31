/**
 *
 */
package com.linkedin.databus.client.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.client.DatabusClientRunnable;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * @author "Balaji Varadarajan<bvaradarajan@linkedin.com>"
 *
 */
public class DatabusGenericClient implements Shutdownable, Startable, DatabusClientRunnable
{
  public static final String MODULE = DatabusGenericClient.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final StaticConfig _clientConfig;
  private final DatabusHttpClientImpl _httpClient;
  private final String _appName;

  public DatabusGenericClient(String appName,
                              Properties consumerProps)
    throws Exception
  {
    LOG.info("DatabusGenericClient : appName =" + appName);
    LOG.info("DatabusGenericClient : consumerProps =" + consumerProps);

    _appName = appName;
    Config configBuilder = new Config();

    ConfigLoader<StaticConfig> configLoader
    = new ConfigLoader<StaticConfig>("databus2.", configBuilder);

    configLoader.loadConfig(consumerProps);

    _clientConfig = configBuilder.build();

    _httpClient = new DatabusHttpClientImpl(_clientConfig.getClient());
  }


  public void registerConsumers(String[][] sources,
		   					   boolean enableStreamConsumers,
                               List<DatabusStreamConsumer> streamConsumers,
                               boolean enableBootstrapConsumers,
                               List<DatabusBootstrapConsumer> bootstrapConsumers,
                               Properties filterProps)
    throws Exception
  {
    LoggingConsumer logConsumer = _httpClient.getLoggingListener();

    DbusKeyCompositeFilterConfig filterConf = createFilterConfig(filterProps);

    DatabusStreamConsumer[] sConsumers = null;
    if ( enableStreamConsumers && (null != streamConsumers))
    {
      sConsumers = new DatabusStreamConsumer[streamConsumers.size()];
      int i=0;
      for (DatabusStreamConsumer s: streamConsumers)
      {
    	  sConsumers[i++] = s;
      }
    }

    DatabusBootstrapConsumer[] bConsumers = null;
    if ( enableBootstrapConsumers && (null != bootstrapConsumers ))
    {
      bConsumers = new DatabusBootstrapConsumer[bootstrapConsumers.size()];
      int i=0;
      for (DatabusBootstrapConsumer b: bootstrapConsumers)
      {
    	  bConsumers[i++] = b;
      }
    }


    for (String[] sourcesList: _clientConfig.getSources())
    {
      if (null != sourcesList)
      {
        if ( null != sConsumers)
        {
            _httpClient.registerDatabusStreamListener(logConsumer, filterConf, sourcesList);
        	_httpClient.registerDatabusStreamListener(sConsumers, filterConf, sourcesList);
        }

        if ( null != bConsumers)
        {
            _httpClient.registerDatabusBootstrapListener(logConsumer, filterConf, sourcesList);
        	_httpClient.registerDatabusBootstrapListener(bConsumers,filterConf, sourcesList);
        }
      }
    }
  }

  private DbusKeyCompositeFilterConfig createFilterConfig(Properties filterProps)
    throws Exception
  {
    DbusKeyCompositeFilterConfig filterConf = null;
    if ( null != filterProps )
    {
        DbusKeyCompositeFilterConfig.Config staticConfigBuilder = new DbusKeyCompositeFilterConfig.Config();
        ConfigLoader<DbusKeyCompositeFilterConfig.StaticConfig> staticConfigLoader =
                new ConfigLoader<DbusKeyCompositeFilterConfig.StaticConfig>("serversidefilter.", staticConfigBuilder);

        DbusKeyCompositeFilterConfig.StaticConfig staticConfig = staticConfigLoader.loadConfig(filterProps);

        filterConf = new DbusKeyCompositeFilterConfig(staticConfig);
    }
    return filterConf;
  }

  public void registerConsumer(String[][] sources,
		  					   boolean enableStreamConsumer,
                               DatabusStreamConsumer streamConsumer,
                               boolean enableBootstrapConsumer,
                               DatabusBootstrapConsumer bootstrapConsumer,
                               Properties filterProps)
    throws Exception
  {
    LoggingConsumer logConsumer = _httpClient.getLoggingListener();

    DbusKeyCompositeFilterConfig filterConf = createFilterConfig(filterProps);

    for (String[] sourcesList: sources)
    {
      if (null != sourcesList)
      {
        if ( enableStreamConsumer && (null != streamConsumer))
        {
        	_httpClient.registerDatabusStreamListener(logConsumer, filterConf, sourcesList);
        	_httpClient.registerDatabusStreamListener(streamConsumer, filterConf, sourcesList);
        }

        if ( enableBootstrapConsumer && (null != bootstrapConsumer))
        {
            _httpClient.registerDatabusBootstrapListener(logConsumer, filterConf, sourcesList);
        	_httpClient.registerDatabusBootstrapListener(bootstrapConsumer, filterConf, sourcesList);
        }
      }
    }
  }

  /*
   * Register a list of Stream and Bootstrap Consumers (parallel) for sources listed
   */
  public DatabusGenericClient(
          String appName,
          Properties filterProps,
          Properties consumerProps,
          Properties overriddenConsumerProps,
          List<DatabusStreamConsumer> streamConsumers,
          List<DatabusBootstrapConsumer> bootstrapConsumers)
  		throws Exception
  {
	  this(appName, filterProps, consumerProps, overriddenConsumerProps, true, streamConsumers, true, bootstrapConsumers);
  }

  /*
   * Register a list of Stream and Bootstrap Consumers (parallel) for sources listed
   */
  public DatabusGenericClient(
                              String appName,
                              Properties filterProps,
                              Properties consumerProps,
                              Properties overriddenConsumerProps,
                              boolean     enableStreamConsumers,
                              List<DatabusStreamConsumer> streamConsumers,
                              boolean     enableBootstrapConsumers,
                              List<DatabusBootstrapConsumer> bootstrapConsumers)
    throws Exception
  {
    LOG.info("DatabusGenericClient : appName =" + appName);
    LOG.info("DatabusGenericClient : filterProps =" + filterProps);
    LOG.info("DatabusGenericClient : consumerProps =" + consumerProps);
    LOG.info("DatabusGenericClient : overridenConsumerProps =" + overriddenConsumerProps);
    LOG.info("DatabusGenericClient : enableStreamConsumers =" + enableStreamConsumers );
    LOG.info("DatabusGenericClient : Stream Consumers =" + streamConsumers);
    LOG.info("DatabusGenericClient : enableBootstrapConsumers =" + enableBootstrapConsumers );
    LOG.info("DatabusGenericClient : Bootstrap Consumers =" + bootstrapConsumers);
    _appName = appName;

    // Merge the default properties and override properties into a single new properties object
    Properties props = new Properties();
    for(Map.Entry<Object, Object> entry : consumerProps.entrySet())
    {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }

    if ( null != overriddenConsumerProps)
    {
      for(Map.Entry<Object, Object> entry : overriddenConsumerProps.entrySet())
      {
        props.setProperty((String)entry.getKey(), (String)entry.getValue());
      }
    }

    LOG.info(" Merged props: " + props);

    Config configBuilder = new Config();

    ConfigLoader<StaticConfig> configLoader
    = new ConfigLoader<StaticConfig>("databus2.", configBuilder);

    configLoader.loadConfig(props);

    _clientConfig = configBuilder.build();

    _httpClient = new DatabusHttpClientImpl(_clientConfig.getClient());
    registerConsumers(_clientConfig.getSources(),enableStreamConsumers, streamConsumers, enableBootstrapConsumers, bootstrapConsumers, filterProps);
  }

  /*
   * Register a single Stream and Bootstrap Consumer for sources listed
   */
  public DatabusGenericClient(
                                 String appName,
                                 Properties filterProps,
                                 Properties consumerProps,
                                 Properties overriddenConsumerProps,
                                 DatabusStreamConsumer streamConsumer,
                                 DatabusBootstrapConsumer bootstrapConsumer)
  throws Exception
  {
	  this(appName, filterProps, consumerProps, overriddenConsumerProps, true, streamConsumer, true, bootstrapConsumer);
  }

  /*
   * Register a single Stream and Bootstrap Consumer for sources listed
   */
  public DatabusGenericClient(
                                 String appName,
                                 Properties filterProps,
                                 Properties consumerProps,
                                 Properties overriddenConsumerProps,
                                 boolean enableStreamConsumer,
                                 DatabusStreamConsumer streamConsumer,
                                 boolean enableBootstrapConsumer,
                                 DatabusBootstrapConsumer bootstrapConsumer)
  throws Exception
  {
    LOG.info("DatabusGenericClient : appName =" + appName);
    LOG.info("DatabusGenericClient : filterProps =" + filterProps);
    LOG.info("DatabusGenericClient : consumerProps =" + consumerProps);
    LOG.info("DatabusGenericClient : overridenConsumerProps =" + overriddenConsumerProps);
    LOG.info("DatabusGenericClient : enableStreamConsumers =" + enableStreamConsumer );
    LOG.info("DatabusGenericClient : Stream Consumer =" + streamConsumer);
    LOG.info("DatabusGenericClient : enableBootstrapConsumer =" + enableBootstrapConsumer );
    LOG.info("DatabusGenericClient : Bootstrap Consumer =" + bootstrapConsumer);

    // Merge the default properties and override properties into a single new properties object
    Properties props = new Properties();
    for(Map.Entry<Object, Object> entry : consumerProps.entrySet())
    {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }

    if ( null != overriddenConsumerProps)
    {
      for(Map.Entry<Object, Object> entry : overriddenConsumerProps.entrySet())
      {
        props.setProperty((String)entry.getKey(), (String)entry.getValue());
      }
    }

    LOG.info(" Merged props: " + props);

    _appName = appName;
    Config configBuilder = new Config();

    ConfigLoader<StaticConfig> configLoader
    = new ConfigLoader<StaticConfig>("databus2.", configBuilder);

    configLoader.loadConfig(props);

    _clientConfig = configBuilder.build();

    _httpClient = new DatabusHttpClientImpl(_clientConfig.getClient());
    registerConsumer(_clientConfig.getSources(), enableStreamConsumer, streamConsumer, enableBootstrapConsumer, bootstrapConsumer, filterProps);
  }

  public String getAppName()
  {
    return _appName;
  }

  @Override
  public void shutdown() {
    LOG.info("" + _appName + " : Shutdown requested");
    _httpClient.shutdown();
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
  IllegalStateException {
    LOG.info("" + _appName + " : Waiting for shutdown");
    _httpClient.awaitShutdown();
    LOG.info("" + _appName + ": shutdown");
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
  IllegalStateException, TimeOutException {
    // TODO add timeout
    LOG.info("" + _appName + " : Waiting for shutdown");
    _httpClient.awaitShutdown();
    LOG.info("" + _appName + ": shutdown");
  }


  @Override
  public void start() throws Exception {
    LOG.info("" + _appName + " : Starting client");
    _httpClient.start();
  }

  public static class StaticConfig
  {
    private final String[][] _sources;
    private final DatabusHttpClientImpl.StaticConfig _client;

    public StaticConfig(String[][] sources, DatabusHttpClientImpl.StaticConfig client)
    {
      super();
      _sources = sources;
      _client = client;
    }

    public String[][] getSources()
    {
      return _sources;
    }

    public DatabusHttpClientImpl.StaticConfig getClient()
    {
      return _client;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private final ArrayList<String> _sources;
    private DatabusHttpClientImpl.Config _client;

    public Config()
    {
      _sources = new ArrayList<String>(10);
      _client = new DatabusHttpClientImpl.Config();
    }

    public String getSources(int index)
    {
      return getOrAddSources(index);
    }

    public void setSources(int index, String value)
    {
      getOrAddSources(index);
      _sources.set(index, value);
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      ArrayList<String[]> parsedArray = new ArrayList<String[]>(_sources.size());

      for (String sourcesList: _sources)
      {
        sourcesList = sourcesList.trim();
        if (sourcesList.isEmpty()) continue;

        String[] parsedList = sourcesList.split(",");
        for (int i = 0; i < parsedList.length; ++i)
        {
          parsedList[i] = parsedList[i].trim();
        }

        if (parsedList.length > 0) parsedArray.add(parsedList);
      }

      String[][] compactedList = new String[parsedArray.size()][];
      parsedArray.toArray(compactedList);

      return new StaticConfig(compactedList, _client.build());
    }

    private String getOrAddSources(int index)
    {
      for (int i = _sources.size(); i <= index; ++i)
      {
        _sources.add("");
      }

      return _sources.get(index);
    }

    public DatabusHttpClientImpl.Config getClient()
    {
      return _client;
    }

    public void setClient(DatabusHttpClientImpl.Config client)
    {
      _client = client;
    }
  }

  @Override
  public DatabusHttpClientImpl getDatabusHttpClientImpl() {
	return _httpClient;
  }

}