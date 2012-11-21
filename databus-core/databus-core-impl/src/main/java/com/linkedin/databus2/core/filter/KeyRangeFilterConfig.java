package com.linkedin.databus2.core.filter;

import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author bvaradar
 */
public class KeyRangeFilterConfig
	extends KeyFilterConfig
{
	private long rangeSize;
	private IDConfig rangeIds;

	protected static ObjectMapper _mapper;

	public KeyRangeFilterConfig()
	{
	}

	public  KeyRangeFilterConfig(StaticConfig config)
	{
		super();

		rangeSize = config.getSize();
		rangeIds = config.getIdConfig();
	}

	public long getRangeSize() {
		return rangeSize;
	}

	public void setRangeSize(long rangeSize) {
		this.rangeSize = rangeSize;
	}

	public IDConfig getRangeIds() {
		return rangeIds;
	}

	public void setRangeIds(IDConfig rangeIds) {
		this.rangeIds = rangeIds;
	}

	@Override
	public String toString() {
		return "KeyRangeFilterConfig [rangeSize=" + rangeSize + ", rangeIds="
				+ rangeIds + "]";
	}

	public static class StaticConfig
	{
		private long size;
		private IDConfig idConfig;
		
		public long getSize() {
			return size;
		}
		
		public void setSize(long size) {
			this.size = size;
		}
		
		public IDConfig getIdConfig() {
			return idConfig;
		}
		
		public void setIdConfig(IDConfig idConfig) {
			this.idConfig = idConfig;
		}
		
		public StaticConfig(long size, IDConfig idConfig) 
			throws InvalidConfigException
		{
			super();
			this.size = size;
			this.idConfig = idConfig;
		}
		@Override
		public String toString() {
			return "KeyRangeFilterConfig.StaticConfig [size=" + size + ", idConfig=" + idConfig
					+ "]";
		}
	}

	public static class Config
		implements ConfigBuilder<StaticConfig>
	{
		private long size;
		private String partitions;

		public long getSize() {
			return size;
		}

		public void setSize(long size) {
			this.size = size;
		}

		public String getPartitions() {
			return partitions;
		}

		public void setPartitions(String partitions) {
			this.partitions = partitions;
		}

		@Override
		public StaticConfig build()
			throws InvalidConfigException
		{
			if ( size <= 0)
				throw new InvalidConfigException("Range size (" + size + ") must be greater than 0");
		
			StaticConfig sConf = null;
			
			try
			{
				sConf = new StaticConfig(size, IDConfig.fromString(partitions));
			} catch (Exception ex) {
				throw new InvalidConfigException(ex);
			}
			
			
			return sConf;
		}

	}
}
