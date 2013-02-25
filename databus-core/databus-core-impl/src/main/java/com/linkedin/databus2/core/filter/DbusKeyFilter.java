/**
 *
 */
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


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.KeyBasedPartitioningConfig;
import com.linkedin.databus2.core.filter.KeyFilterConfig.IDConfigEntry;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder.PartitionType;



/**
 * @author bvaradar
 *
 *Implements Server Side Filtering Logic for one SourceId
 *
 *Supports
 *
 *(1) Range Based Partitioning
 *(2) Mod Based Partitioning
 *
 *In both the cases, clients can specify list/range of bucketIds (or) partition ranges that they are interested in. The keys in the DbusEvent have to be
 * numeric (or convertible to numeric).
 *The configuration specification is handled by the KeyBasedPartitioningConfig class.
 *
 *Further Details are available in : https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+V2+Server+Side+Filtering+for+LIAR
 */
public class DbusKeyFilter implements DbusFilter
{
	public static final String MODULE = DbusKeyFilter.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private PartitionType partitionType;
	private ArrayList<DbusFilter> filters;

	public DbusKeyFilter(KeyFilterConfigHolder config)
	{
		partitionType = config.getPartitionType();
		init(config);
	}

	public DbusKeyFilter()
	{
		partitionType = PartitionType.NONE;
		filters = new ArrayList<DbusFilter>();
	}

	public PartitionType getPartitionType() {
		return partitionType;
	}

	public void setPartitionType(PartitionType partitionType) {
		this.partitionType = partitionType;
	}

	public ArrayList<DbusFilter> getFilters() {
		return filters;
	}

	public void setFilters(ArrayList<DbusFilter> filters) {
		this.filters = filters;
	}


	@Override
	public String toString() {
		return "DbusKeyFilter [partitionType=" + partitionType + ", filters="
				+ filters + "]";
	}

	private void init(KeyFilterConfigHolder config)
	{
		boolean debugEnabled = LOG.isDebugEnabled();

		if ( PartitionType.NONE == config.getPartitionType())
		{
			filters = new ArrayList<DbusFilter>();
			return;
		} else if (config.getPartitionType() == PartitionType.RANGE)
		{
			filters = new ArrayList<DbusFilter>();
			KeyRangeFilterConfig filterConfig = (KeyRangeFilterConfig)(config.getFilterConfig());

			long rangeSize = filterConfig.getRangeSize();
			List<IDConfigEntry> partList = filterConfig.getRangeIds().getIdConfigs();

			for (IDConfigEntry entry : partList)
			{
				long keyMin = entry.getIdMin()*rangeSize;
				long keyMax = 0;

				if ( entry.getIdMax() == KeyBasedPartitioningConfig.UPPER_END_UNLIMITED )
				{
					keyMax = Long.MAX_VALUE;
				} else {
					keyMax = (entry.getIdMax() + 1)*rangeSize ;
				}

				if ( debugEnabled) LOG.debug("KeyBasedFilter: keyMin is:" + keyMin + ", keyMax is :" + keyMax);

				filters.add(new KeyRangeFilter(keyMin,keyMax));
			}
		} else {
			filters = new ArrayList<DbusFilter>();
			KeyModFilterConfig filterConfig = (KeyModFilterConfig)(config.getFilterConfig());

			long numBuckets = filterConfig.getNumBuckets();

			if ( numBuckets <= 0)
			{
				throw new RuntimeException("NumBuckets for ModBasedFiltering should be positive. Config was :" + config);
			}

			List<IDConfigEntry> partList = filterConfig.getBuckets().getIdConfigs();
			for (IDConfigEntry entry : partList)
			{
				long bktMin = entry.getIdMin();
				long bktMax = 0;

				if ( entry.getIdMax() == KeyBasedPartitioningConfig.UPPER_END_UNLIMITED )
				{
					bktMax = Long.MAX_VALUE;
				} else {
					bktMax = (entry.getIdMax() + 1);
				}

				if (debugEnabled) LOG.debug("ModBasedFilter: StartBucket is:" + bktMin + ", BktMax is :" + bktMax);

				filters.add(new KeyModFilter(bktMin,bktMax,numBuckets));
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.linkedin.databus.core.DbusFilter#allow(com.linkedin.databus.core.DbusEvent)
	 */
	@Override
	public boolean allow(DbusEvent e)
	{
		boolean allow = false;
		if ( partitionType == PartitionType.NONE)
		{
			allow =  true;

		} else {
			for (DbusFilter filter : filters )
			{
				allow = filter.allow(e);

				if ( allow )
					break;
			}
		}
		return allow;
	}

	/*
	 * Merge Two DbusKeyFilters
	 *
	 * @param Filter to be merged
	 * @throws RuntimeException if both partitionTypes are not NONE and they do not match.
	 */
	public void merge(DbusKeyFilter filter)
	{
		if ( (filter.getPartitionType() != KeyFilterConfigHolder.PartitionType.NONE) &&
			 (partitionType != KeyFilterConfigHolder.PartitionType.NONE) &&
			 (partitionType != filter.getPartitionType() ) )
		{
			String msg = "Partition Type not same in merge. this.filter is :" + toString() + ", Merging Filter is :" + filter;
			LOG.error(msg);
			throw new RuntimeException(msg);
		}

		//TODO: Currently, a naive-way to merge is happening. Implement smarter way (DDSDBUS-106)
		if ( null == filters)
			filters = new ArrayList<DbusFilter>();

		filters.addAll(filter.getFilters());
	}


	/*
	 * Dedupe Filters
	 */
	public void dedupe()
	{
		ArrayList<DbusFilter> dedupFilters = new ArrayList<DbusFilter>();
		Set<DbusFilter> filterSet = new HashSet<DbusFilter>();

		for(DbusFilter f : filters)
			filterSet.add(f);

		for (DbusFilter f : filterSet)
			dedupFilters.add(f);

		filters = dedupFilters;

	}
}
