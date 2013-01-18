package com.linkedin.databus2.core.filter;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;


/*
 * Configuration Holder for DbusKeyCompositeFilter
 * Contains mapping of SourceName to
 *
 * @see https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+V2+Server+Side+Filtering+for+LIAR
 */
public class DbusKeyCompositeFilterConfig
{
	public static final String MODULE = DbusKeyCompositeFilterConfig.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private Map<String, KeyFilterConfigHolder> configMap;

	public DbusKeyCompositeFilterConfig()
	{
		configMap = new HashMap<String, KeyFilterConfigHolder>();
	}

	public DbusKeyCompositeFilterConfig(StaticConfig conf)
	{
		this.configMap = conf.getConfigMap();
	}

	public Map<String, KeyFilterConfigHolder> getConfigMap() {
		return configMap;
	}

	public void setConfigMap(Map<String, KeyFilterConfigHolder> configMap) {
		this.configMap = configMap;
	}


	public void addFilterConfig(String srcName,  KeyFilterConfigHolder config)
	{
		configMap.put(srcName, config);
	}

	public static class StaticConfig
	{
		private final Map<String, KeyFilterConfigHolder > configMap;

		public Map<String, KeyFilterConfigHolder> getConfigMap() {
			return configMap;
		}

		public StaticConfig(Map<String, KeyFilterConfigHolder> configMap) {
			super();
			this.configMap = configMap;
		}

	}

	public static class Config
		implements ConfigBuilder<StaticConfig>
	{
		private Map<String, KeyFilterConfigHolder.Config> _filterMap;

		public Config()
		{
			_filterMap = new HashMap<String, KeyFilterConfigHolder.Config>();
		}

		public void setFilter(String key, KeyFilterConfigHolder.Config conf)
		{
			_filterMap.put(key, conf);
		}

		public KeyFilterConfigHolder.Config getFilter(String key)
		{
			KeyFilterConfigHolder.Config conf = _filterMap.get(key);

			if ( null == conf)
			{
				conf = new KeyFilterConfigHolder.Config();
				_filterMap.put(key, conf);
			}

			return conf;
		}

		@Override
		public StaticConfig build()
			throws InvalidConfigException
		{
			Map<String, KeyFilterConfigHolder> confMap =
						new HashMap<String, KeyFilterConfigHolder>();

			for ( Entry<String, KeyFilterConfigHolder.Config>  e : _filterMap.entrySet())
			{
				KeyFilterConfigHolder f = new KeyFilterConfigHolder(e.getValue().build());
				confMap.put(e.getKey(), f);
			}
			LOG.info("FilterConfigMap is:" + confMap);
			return new StaticConfig(confMap);
		}
	}



	@Override
	public String toString() {
		return "DbusKeyCompositeFilterConfig [configMap=" + configMap + "]";
	}

	public static void main(String[] args)
		throws Exception
	{
		String configFile = "/Users/bvaradar/Documents/workspace/BR_DDS_SERVER_SIDE_FILTERING/integration-test/config/server-filter-test.properties";

		Properties startupProps = new Properties();
		startupProps.load(new FileInputStream(configFile));
	    Config staticConfigBuilder = new Config();
	    ConfigLoader<StaticConfig> staticConfigLoader =
	        new ConfigLoader<StaticConfig>("serversidefilter.", staticConfigBuilder);

	    StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

	    DbusKeyCompositeFilterConfig filterConf = new DbusKeyCompositeFilterConfig(staticConfig);
	    System.out.println("FilterConf :" + filterConf);
	}
}
