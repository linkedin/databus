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


import java.util.List;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author bvaradar
 */
public class KeyModFilterConfig
	extends KeyFilterConfig
{
	private long numBuckets;
	private IDConfig buckets;

	public KeyModFilterConfig()
	{
	}

	public KeyModFilterConfig(StaticConfig config)
	{
		numBuckets = config.getNumBuckets();
		buckets = config.getIdConfig();
	}

	public long getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(long numBuckets) {
		this.numBuckets = numBuckets;
	}

	public IDConfig getBuckets() {
		return buckets;
	}

	public void setBuckets(IDConfig buckets) {
		this.buckets = buckets;
	}


	@Override
	public String toString() {
		return "KeyModFilterConfig [numBuckets=" + numBuckets + ", buckets="
				+ buckets + "]";
	}



	public static class StaticConfig
	{
		private long numBuckets;
		private IDConfig idConfig;

		public long getNumBuckets() {
			return numBuckets;
		}

		public void setNumBuckets(long numBuckets) {
			this.numBuckets = numBuckets;
		}

		public IDConfig getIdConfig() {
			return idConfig;
		}

		public void setIdConfig(IDConfig idConfig) {
			this.idConfig = idConfig;
		}

		@Override
		public String toString() {
			return "StaticConfig [numBuckets=" + numBuckets + ", idConfig="
					+ idConfig + "]";
		}

		public StaticConfig(long numBuckets, IDConfig idConfig) 
			throws InvalidConfigException
		{
			super();
			this.numBuckets = numBuckets;
			this.idConfig = idConfig;
			
			if ( numBuckets <= 0)
				throw new InvalidConfigException("Mod Numbuckets (" + numBuckets + ") must be greater than 0");
			
			List<IDConfigEntry> idConfigs = idConfig.getIdConfigs();
			
			for (IDConfigEntry entry : idConfigs)
			{
				if ( (entry.getIdMax() < 0 ) || (entry.getIdMin() < 0)
						|| (entry.getIdMin() > entry.getIdMax()) 
						|| (entry.getIdMin() > numBuckets)
						|| (entry.getIdMax() > numBuckets))
				{
					throw new InvalidConfigException("Mod idConfig Entry (" + entry + ") is invalid for bucket size (" + numBuckets + ")");
				}
			}
		}
	}

	public static class Config
		implements ConfigBuilder<StaticConfig>
	{
		private long numBuckets;
		private String buckets;

		public long getNumBuckets() {
			return numBuckets;
		}

		public void setNumBuckets(long numBuckets) {
			this.numBuckets = numBuckets;
		}

		public String getBuckets() {
			return buckets;
		}

		public void setBuckets(String buckets) {
			this.buckets = buckets;
		}

		@Override
		public StaticConfig build()
			throws InvalidConfigException
		{			
			StaticConfig config = null;
			try
			{
				config = new StaticConfig(numBuckets, IDConfig.fromString(buckets));
			} catch (Exception ex) {
				throw new InvalidConfigException(ex);
			}			
			return config;
		}

	}

}
