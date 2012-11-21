package com.linkedin.databus.client.test;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.client.DatabusClientRunnable;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.LastWriteTimeTrackerImpl;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.consumer.PerfTestLoggingConsumer;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.exceptions.BusinessException;

/**
 * A test Databus 2.0 client with a consumer that can be controlled to simulate various consumer
 * behaviors.
 */
public class SimulatingTestClientRunnable implements Startable, Shutdownable, DatabusClientRunnable
{
  public static final String MODULE = SimulatingTestClientRunnable.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final SimulatingTestClientRunnable.StaticConfig _clientConfig;
  private final DatabusHttpClientImpl _httpClient;


  public SimulatingTestClientRunnable(Properties props,LastWriteTimeTrackerImpl repTimeTracker) throws Exception
  {
    SimulatingTestClientRunnable.Config configBuilder = new SimulatingTestClientRunnable.Config();

    ConfigLoader<SimulatingTestClientRunnable.StaticConfig> configLoader
        = new ConfigLoader<SimulatingTestClientRunnable.StaticConfig>("databus.test.consumer.",
                                                                      configBuilder);
    configLoader.loadConfig(props);

    _clientConfig = configBuilder.build();

    _httpClient = new DatabusHttpClientImpl(_clientConfig.getClient());
    _httpClient.setWriteTimeTracker(repTimeTracker);

    LoggingConsumer logConsumer = _httpClient.getLoggingListener();
    PerfTestLoggingConsumer perfLogConsumer = new PerfTestLoggingConsumer(logConsumer);
    perfLogConsumer.setBusyLoadTimeMs(_clientConfig.getBusyLoadTimeMs());
    perfLogConsumer.setIdleLoadTimeMs(_clientConfig.getIdleLoadTimeMs());
    
    for (String[] sourcesList: _clientConfig.getSources())
    {
      if (null != sourcesList)
      {
        _httpClient.registerDatabusStreamListener(perfLogConsumer, null,sourcesList);
        _httpClient.registerDatabusBootstrapListener(perfLogConsumer, null, sourcesList);
      }
    }

  }
  @Override
  public DatabusHttpClientImpl getDatabusHttpClientImpl()
  {
    return _httpClient;
  }

  @Override
  public void shutdown()
  {
    LOG.info("Shutdown requested");
    _httpClient.shutdown();
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
      IllegalStateException
  {
    LOG.info("Waiting for shutdown");
    _httpClient.awaitShutdown();
    LOG.info(getClass().getSimpleName()+ ": shutdown");
  }

  @Override
  public void waitForShutdown(long timeout) throws InterruptedException,
      IllegalStateException,
      TimeOutException
  {
    // TODO add timeout
    LOG.info("Waiting for shutdown");
    _httpClient.awaitShutdown();
    LOG.info(getClass().getSimpleName()+ ": shutdown");
  }

  @Override
  public void start() throws Exception
  {
    LOG.info("Starting client");
    _httpClient.startAsynchronously();
  }

  public static class StaticConfig
  {
    private final String[][] _sources;
    private final DatabusHttpClientImpl.StaticConfig _client;
    private final long _busyLoadTimeMs;
    private final long _idleLoadTimeMs;

    public StaticConfig(String[][] sources, DatabusHttpClientImpl.StaticConfig client,
    		long busyLoadTimeMs,long idleLoadTimeMs)
    {
      super();
      _sources = sources;
      _client = client;
      _busyLoadTimeMs = busyLoadTimeMs;
      _idleLoadTimeMs = idleLoadTimeMs;
    }

    public String[][] getSources()
    {
      return _sources;
    }

    public DatabusHttpClientImpl.StaticConfig getClient()
    {
      return _client;
    }
    
    public long getIdleLoadTimeMs()
    {
    	return _idleLoadTimeMs;
    }
    
    public long getBusyLoadTimeMs()
    {
    	return _busyLoadTimeMs;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private final ArrayList<String> _sources;
    private DatabusHttpClientImpl.Config _client;
    private long _busyLoadTimeMs;
    private long _idleLoadTimeMs;

    public Config()
    {
      _sources = new ArrayList<String>(10);
      _client = new DatabusHttpClientImpl.Config();
      setBusyLoadTimeMs(0);
      setIdleLoadTimeMs(0);
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

      return new StaticConfig(compactedList, _client.build(),_busyLoadTimeMs,_idleLoadTimeMs);
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

	public long getBusyLoadTimeMs()
	{
		return _busyLoadTimeMs;
	}

	public void setBusyLoadTimeMs(long busyLoadTimeMs)
	{
		_busyLoadTimeMs = busyLoadTimeMs;
	}

	public long getIdleLoadTimeMs()
	{
		return _idleLoadTimeMs;
	}

	public void setIdleLoadTimeMs(long idleLoadTimeMs)
	{
		_idleLoadTimeMs = idleLoadTimeMs;
	}
    
    
  }

}
