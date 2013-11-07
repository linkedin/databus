package com.linkedin.databus2.core.filter;
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


import java.util.Properties;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;


import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author bvaradar
 */
public class KeyFilterConfigHolder 
{
	public static final String MODULE = KeyFilterConfigHolder.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static final String NO_PARTITIONING = "NONE";
	public static final String RANGE_PARTITIONING = "RANGE";
	public static final String MOD_PARTITIONING = "MOD";

	public static final Long UPPER_END_UNLIMITED = Long.MAX_VALUE;


	private PartitionType partitionType;
	private KeyFilterConfig filterConfig;

	/*
	 * Partition Types Supported
	 */
	public static enum PartitionType
	{
		NONE,
		RANGE,
		MOD
	};

	public KeyFilterConfigHolder(StaticConfig config)
	{
		partitionType = config.getType();
		filterConfig = config.getConfig();
	}

	public KeyFilterConfigHolder()
	{}

	public PartitionType getPartitionType() {
		return partitionType;
	}

	public void setPartitionType(PartitionType partitionType) {
		this.partitionType = partitionType;
	}

	public KeyFilterConfig getFilterConfig() {
		return filterConfig;
	}

	public void setFilterConfig(KeyFilterConfig filterConfig) {
		this.filterConfig = filterConfig;
	}

	@Override
	public String toString() {
		return "KeyFilterConfigHolder [partitionType=" + partitionType
		+ ", filterConfig=" + filterConfig + "]";
	}

	
	public static class StaticConfig
	{
		private PartitionType type;
		private KeyFilterConfig config;

		public PartitionType getType() {
			return type;
		}

		public void setType(PartitionType type) {
			this.type = type;
		}

		public KeyFilterConfig getConfig() {
			return config;
		}

		public void setConfig(KeyFilterConfig config) {
			this.config = config;
		}

		public StaticConfig(PartitionType type, KeyFilterConfig config) {
			super();
			this.type = type;
			this.config = config;
		}		
	}

	public static class Config 
	implements ConfigBuilder<StaticConfig>
	{
		private KeyRangeFilterConfig.Config range;
		private KeyModFilterConfig.Config mod;
		private String type;

		public Config()
		{
			range = new KeyRangeFilterConfig.Config();
			mod = new KeyModFilterConfig.Config();
		}
		public KeyRangeFilterConfig.Config getRange() {
			return range;
		}

		public void setRange(KeyRangeFilterConfig.Config range) {
			this.range = range;
		}

		public KeyModFilterConfig.Config getMod() {
			return mod;
		}

		public void setMod(KeyModFilterConfig.Config mod) {
			this.mod = mod;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public StaticConfig build() 
		throws InvalidConfigException
		{
			PartitionType pType = PartitionType.valueOf(type);

			KeyFilterConfig config = null;
			if ( (null == range.getPartitions()) && (pType == PartitionType.RANGE))
			{
				throw new InvalidConfigException("PartitionType is set to range but range configuration is not given.");				
			}

			if ( (null == mod.getBuckets()) && (pType == PartitionType.MOD))
			{
				throw new InvalidConfigException("PartitionType is set to mod but mod configuration is not given.");

			}

			if (pType == PartitionType.RANGE)
				config = new KeyRangeFilterConfig(range.build());
			else if (pType == PartitionType.MOD)
				config = new KeyModFilterConfig(mod.build());

			return new StaticConfig(pType,config);
		} 
	}

	public static void main(String[] args)
	throws Exception
	{
		Properties props = new Properties();
		props.setProperty("dummy.type", "MOD");
		props.setProperty("dummy.mod.numBuckets", "10");
		props.setProperty("dummy.mod.buckets", "[1,2,4-7]");

		Config config = new Config();

		ConfigLoader<StaticConfig> configLoader =
			new ConfigLoader<StaticConfig>("dummy.", config);
		StaticConfig staticConfig = configLoader.loadConfig(props);	

		KeyFilterConfigHolder holder = new KeyFilterConfigHolder(staticConfig);

		System.out.println("Holder is :" + holder.toString());
		
		ObjectMapper mapper = new ObjectMapper();
		String s = mapper.writeValueAsString(holder);
		
		System.out.println("Config Holder JSON:" + s);
				
		JSONObject obj = new JSONObject(s);
		String type = obj.getString("partitionType");
		String configStr = obj.getString("filterConfig");
		
		KeyFilterConfigHolder configHolder = new KeyFilterConfigHolder();
        configHolder.setPartitionType(PartitionType.valueOf(type));
        KeyFilterConfig conf = null;
		if (PartitionType.MOD == configHolder.getPartitionType())
		{
		   conf = mapper.readValue(configStr,KeyModFilterConfig.class);
		} else if (PartitionType.RANGE == configHolder.getPartitionType()) {
		   conf = mapper.readValue(configStr,KeyRangeFilterConfig.class); 
		}
		configHolder.setFilterConfig(conf);
		
		System.out.println("Holder2 is :" + configHolder.toString());
	    
	}
}
