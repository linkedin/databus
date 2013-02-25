/**
 *
 */
package com.linkedin.databus.core;
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


import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author bvaradar
 *
 *This class holds config information for applying server-side filtering
 *
 *Detailed Description available in :
 *https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+V2+Server+Side+Filtering+for+LIAR#DatabusV2ServerSideFilteringforLIAR-
 *
 *Please note that all the ranges specified for both range and mod based partitioning includes both the endpoints [ begin, end ] of each range.
 */
public class KeyBasedPartitioningConfig
{
	public static final String MODULE = KeyBasedPartitioningConfig.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static final String NO_PARTITIONING = "NONE";
	public static final String RANGE_PARTITIONING = "RANGE";
	public static final String MOD_PARTITIONING = "MOD";

	public static final Long UPPER_END_UNLIMITED = Long.MAX_VALUE;

	private String sourceName;
	private PartitionType partitionType;
	private Long rangeSize;
	private Long numBuckets;
	private IDConfig partitions;
	private IDConfig buckets;

	/**
	 * No-Arg Constructor
	 */
	public KeyBasedPartitioningConfig() {
		super();
	}

	public KeyBasedPartitioningConfig(StaticConfig config)
	{
		sourceName = config.getSourceName();
		partitionType = config.getType();
		rangeSize = config.getRangeSize();
		numBuckets = config.getNumBuckets();
		partitions = config.getPartitions();
		buckets = config.getBuckets();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "KeyBasedPartitioningConfig [sourceName=" + sourceName
				+ ", partitionType=" + partitionType + ", rangeSize="
				+ rangeSize + ", numBuckets=" + numBuckets + ", partitions="
				+ partitions + ", buckets=" + buckets + "]";
	}

	/**
	 * @return the sourceName
	 */
	public String getSourceName() {
		return sourceName;
	}

	/**
	 * @param sourceName the sourceName to set
	 */
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	/**
	 * @return the partitionType
	 */
	public PartitionType getPartitionType() {
		return partitionType;
	}

	/**
	 * @param partitionType the partitionType to set
	 */
	public void setPartitionType(PartitionType partitionType) {
		this.partitionType = partitionType;
	}

	/**
	 * @return the rangeSize
	 */
	public Long getRangeSize() {
		return rangeSize;
	}

	/**
	 * @param rangeSize the rangeSize to set
	 */
	public void setRangeSize(Long rangeSize) {
		this.rangeSize = rangeSize;
	}

	/**
	 * @return the numBuckets
	 */
	public Long getNumBuckets() {
		return numBuckets;
	}

	/**
	 * @param numBuckets the numBuckets to set
	 */
	public void setNumBuckets(Long numBuckets) {
		this.numBuckets = numBuckets;
	}

	/**
	 * @return the partitions
	 */
	public IDConfig getPartitions() {
		return partitions;
	}

	/**
	 * @param partitions the partitions to set
	 */
	public void setPartitions(IDConfig partitions) {
		this.partitions = partitions;
	}

	/**
	 * @return the buckets
	 */
	public IDConfig getBuckets() {
		return buckets;
	}

	/**
	 * @param buckets the buckets to set
	 */
	public void setBuckets(IDConfig buckets) {
		this.buckets = buckets;
	}


	/*
	 * IDConfig is a list of IDConfigEntry
	 */
	public static class IDConfig
	{
		public static final String PREFIX = "[";
		public static final String DELIMITER = ",";
		public static final String SUFFIX = "]";

		public ArrayList<IDConfigEntry> idConfigs;

		public IDConfig()
		{
		}

		public static IDConfig fromString(String entryStr)
		{
			if ( null == entryStr)
				return null;

			String entry = entryStr.trim();

			if ((! entry.startsWith(PREFIX)) || (! entry.endsWith(SUFFIX)))
			{
				throw new RuntimeException("IDConfig missing PREFIX/SUFFIX. Config should be of the format : [ <IDConfigEntry1>, <IDConfigEntry2> ...]");
			}

			ArrayList<IDConfigEntry> idConfigs = new ArrayList<IDConfigEntry>();

			// Remove prefix and suffix and split
			String[] vals = entry.substring(1, entry.length() - 1).split(DELIMITER);

			for (String v : vals)
			{
				idConfigs.add(IDConfigEntry.fromString(v));
			}

			IDConfig idConf = new IDConfig();
			idConf.setIdConfigs(idConfigs);
			return idConf;
		}

		/**
		 * @return the idConfigs
		 */
		public ArrayList<IDConfigEntry> getIdConfigs() {
			return idConfigs;
		}

		/**
		 * @param idConfigs the idConfigs to set
		 */
		public void setIdConfigs(ArrayList<IDConfigEntry> idConfigs) {
			this.idConfigs = idConfigs;
		}

		@Override
    public String toString()
		{
			StringBuilder str = new StringBuilder();
			str.append(PREFIX);
			boolean first = true;
			for( IDConfigEntry id : idConfigs)
			{
				if ( !first) str.append(DELIMITER);
				first = false;
				str.append(id.toString());
			}
			str.append(SUFFIX);
			return str.toString();
		}

		public boolean matches(long id)
		{
			boolean match = false;

			for( IDConfigEntry idConfig : idConfigs)
			{
				match = idConfig.matches(id);

				if ( match)
					return true;
			}

			return false;
		}
	}

	/*
	 * IDConfigEntry can be of 2 formats
	 * Single : <id>
	 * Range : <id1> - <id2>
	 */
	public static class IDConfigEntry
	{
		public static final String RANGE_DELIMITER = "-";
		public enum Type
		{
			SINGLE,
			RANGE
		};

		private long idMin;
		private long idMax;
		private Type type;

		public IDConfigEntry()
		{

		}

		/**
		 * @return the idMin
		 */
		public long getIdMin() {
			return idMin;
		}


		/**
		 * @param idMin the idMin to set
		 */
		public void setIdMin(long idMin) {
			this.idMin = idMin;
		}


		/**
		 * @return the idMax
		 */
		public long getIdMax() {
			return idMax;
		}


		/**
		 * @param idMax the idMax to set
		 */
		public void setIdMax(long idMax) {
			this.idMax = idMax;
		}


		/**
		 * @return the type
		 */
		public Type getType() {
			return type;
		}


		/**
		 * @param type the type to set
		 */
		public void setType(Type type) {
			this.type = type;
		}


		public static IDConfigEntry fromString(String entryStr)
		{
			IDConfigEntry idConf = new IDConfigEntry();

			String entry = entryStr.trim();

			if ( entry.contains(RANGE_DELIMITER))
			{
				idConf.setType(Type.RANGE);
				String[] vals = entry.split(RANGE_DELIMITER);
				long v1 = Long.parseLong(vals[0]);
				long v2 = Long.parseLong(vals[1]);
				idConf.setIdMin(v1);
				idConf.setIdMax(v2);

				if (v1 > v2)
					throw new RuntimeException("IDConfigEntry is invalid. idMin is greater than idMax. Entry :" + entry);
			}  else {
				idConf.setType(Type.SINGLE);
				long val = Long.parseLong(entry);
				idConf.setIdMin(val);
				idConf.setIdMax(val);
			}
			return idConf;
		}


		@Override
    public String toString()
		{
			if (type == Type.RANGE)
				return idMin + RANGE_DELIMITER + idMax;
			else
				return "" + idMin;
		}


		public boolean matches(long id)
		{
			if (type == Type.SINGLE)
			{
				return id == idMin;
			} else {
				return ( (id >= idMin) && ( id <= idMax));
			}

		}
	}


	public static class StaticConfig
	{
		private final String sourceName;
		private final PartitionType type;
		private final IDConfig partitions;
		private final IDConfig buckets;
		private final Long rangeSize;
		private final Long numBuckets;

		public StaticConfig(String sourceName, PartitionType type,
				IDConfig partitions, IDConfig buckets, Long rangeSize,
				Long numBuckets) {
			super();
			this.sourceName = sourceName;
			this.type = type;
			this.partitions = partitions;
			this.buckets = buckets;
			this.rangeSize = rangeSize;
			this.numBuckets = numBuckets;

		}

		/**
		 * @return the sourcesName
		 */
		public String getSourceName() {
			return sourceName;
		}

		/**
		 * @return the type
		 */
		public PartitionType getType() {
			return type;
		}

		/**
		 * @return the partitions
		 */
		public IDConfig getPartitions() {
			return partitions;
		}

		/**
		 * @return the buckets
		 */
		public IDConfig getBuckets() {
			return buckets;
		}

		/**
		 * @return the rangeSize
		 */
		public Long getRangeSize() {
			return rangeSize;
		}

		/**
		 * @return the numBuckets
		 */
		public Long getNumBuckets() {
			return numBuckets;
		}

	}


	/*
	 * Partition Types Supported
	 */
	public enum PartitionType
	{
		NONE,
		RANGE,
		MOD
	};



	public static class Config
		implements ConfigBuilder<StaticConfig>
	{
		private String sourceName;
		private String type;
		private Long rangeSize;
		private Long numBuckets;
		private String partitionIds;
		private String bucketIds;

		/**
		 * @return the sourcesName
		 */
		public String getSourceName() {
			return sourceName;
		}

		/**
		 * @param sourceName the sourcesName to set
		 */
		public void setSourceName(String sourceName) {
			this.sourceName = sourceName;
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the rangeSize
		 */
		public Long getRangeSize() {
			return rangeSize;
		}

		/**
		 * @param rangeSize the rangeSize to set
		 */
		public void setRangeSize(Long rangeSize) {
			this.rangeSize = rangeSize;
		}

		/**
		 * @return the numBuckets
		 */
		public Long getNumBuckets() {
			return numBuckets;
		}

		/**
		 * @param numBuckets the numBuckets to set
		 */
		public void setNumBuckets(Long numBuckets) {
			this.numBuckets = numBuckets;
		}

		/**
		 * @return the partitionIds
		 */
		public String getPartitionIds() {
			return partitionIds;
		}

		/**
		 * @param partitionIds the partitionIds to set
		 */
		public void setPartitionIds(String partitionIds) {
			this.partitionIds = partitionIds;
		}

		/**
		 * @return the bucketIds
		 */
		public String getBucketIds() {
			return bucketIds;
		}

		/**
		 * @param bucketIds the bucketIds to set
		 */
		public void setBucketIds(String bucketIds) {
			this.bucketIds = bucketIds;
		}

	    public StaticConfig build()
	    	throws InvalidConfigException
	    {
	    	return new StaticConfig(sourceName,PartitionType.valueOf(type),
	    			 				IDConfig.fromString(partitionIds),IDConfig.fromString(bucketIds),
	    			 				rangeSize,numBuckets);
	    }

	}

	public static void main(String[] args)
		throws Exception
	{
		KeyBasedPartitioningConfig.Config config = new KeyBasedPartitioningConfig.Config();
		KeyBasedPartitioningConfig.Config configB = new KeyBasedPartitioningConfig.Config();

		ArrayList<String> sources = new ArrayList<String>();
		sources.add("Source1");
		sources.add("Source2");

		config.setType("RANGE");
		configB.setType("MOD");

		config.setRangeSize(100L);
		configB.setNumBuckets(100L);
		config.setPartitionIds("[1-5,8,10]");
		configB.setBucketIds("[1-5,8,10]");
		config.setSourceName(sources.get(0));

		KeyBasedPartitioningConfig part = new KeyBasedPartitioningConfig(config.build());
		KeyBasedPartitioningConfig partB = new KeyBasedPartitioningConfig(configB.build());

		System.out.println("Config is :" + part);
		System.out.println("ConfigB is :" + partB);

		ObjectMapper mapper = new ObjectMapper();
		String configStr = mapper.writeValueAsString(part);
		String configStr2 = mapper.writeValueAsString(partB);

		System.out.println("JSON Config is :" + configStr);
		System.out.println("JSON ConfigB is :" + configStr2);

		KeyBasedPartitioningConfig part2 = mapper.readValue(new ByteArrayInputStream(configStr.getBytes()),KeyBasedPartitioningConfig.class);
		KeyBasedPartitioningConfig part2B = mapper.readValue(new ByteArrayInputStream(configStr2.getBytes()),KeyBasedPartitioningConfig.class);

		HashMap<String,KeyBasedPartitioningConfig> configMap = new HashMap<String,KeyBasedPartitioningConfig>();
		configMap.put("src1", part);
		configMap.put("src2", partB);

		String mapStr = mapper.writeValueAsString(configMap);
		System.out.println("ConfigMap1 is :" + configMap);
		System.out.println("Map String is:" + mapStr);

		HashMap<String,KeyBasedPartitioningConfig> configMap2 = null;
		TypeReference<HashMap<String,KeyBasedPartitioningConfig>> typeRef = new TypeReference< HashMap<String,KeyBasedPartitioningConfig> >() {};
		configMap2 = mapper.readValue(mapStr, typeRef);
		System.out.println("ConfigMap2 is :" + configMap2);

		System.out.println("Config2 is :" + part2);
		System.out.println("Config2B is :" + part2B);

	}


}
