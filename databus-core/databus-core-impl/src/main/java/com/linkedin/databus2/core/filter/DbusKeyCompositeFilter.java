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


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalReadable;

/**
 * @author bvaradar
 *
 * Implements Server Side Filtering Logic for a collection of SourceId
 *
 * There is a DbusKeyFilter defined for each SourceId inside the compositeFilter
 *
 */
public class DbusKeyCompositeFilter implements DbusFilter
{
	public static final String MODULE = DbusKeyCompositeFilter.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private Map<Long, DbusKeyFilter> filterMap;

	public DbusKeyCompositeFilter(Map<Long, KeyFilterConfigHolder> configMap)
	{
		filterMap = new HashMap<Long, DbusKeyFilter>();

		for (Map.Entry<Long, KeyFilterConfigHolder> entry : configMap.entrySet())
		{
			filterMap.put(entry.getKey(), new DbusKeyFilter(entry.getValue()));
		}
	}

	public DbusKeyCompositeFilter()
	{
		filterMap = new HashMap<Long, DbusKeyFilter>();
	}

	@Override
	public String toString() {
		return "DbusKeyCompositeFilter [filterMap=" + filterMap + "]";
	}

	public Map<Long, DbusKeyFilter> getFilterMap() {
		return filterMap;
	}

	public void setFilterMap(Map<Long, DbusKeyFilter> filterMap) {
		this.filterMap = filterMap;
	}

	@Override
	public boolean allow(DbusEvent e)
	{
		long srcId = e.srcId();

		DbusKeyFilter filter = filterMap.get(srcId);

		if ( null != filter)
		{
			boolean allow = filter.allow(e);

			//TODO: DDSDBUS-3263
			if ( LOG.isDebugEnabled() && e instanceof DbusEventInternalReadable)
			  LOG.debug("Filtered Response was :" + allow + " for key :" + ((DbusEventInternalReadable)e).getDbusEventKey());

			return allow;
		}

		// If Filtering for SrcId specified, allow the event to pass through
		return true;
	}

	/*
	 * Merge Two DbusKeyCompositeFilters
	 *
	 * @param Filter to be merged
	 * @throws RuntimeException if both partitionTypes are not NONE and they do not match.
	 */
	public void merge(DbusKeyCompositeFilter filter)
	{
		Set<Entry<Long, DbusKeyFilter>> entries = filter.getFilterMap().entrySet();
		for(Entry<Long, DbusKeyFilter> e : entries)
		{
			DbusKeyFilter f1 = e.getValue();
			DbusKeyFilter f2 = filterMap.get(e.getKey());

			if ( null == f2)
			{
				filterMap.put(e.getKey(), f1);
			} else {
				f2.merge(f1);
				filterMap.put(e.getKey(), f2);
			}
		}
	}

	/*
	 * Dedupe Filters
	 */
	public void dedupe()
	{
		Set<Entry<Long, DbusKeyFilter>> entries = filterMap.entrySet();

		for(Entry<Long, DbusKeyFilter> e : entries)
		{
			DbusKeyFilter f = e.getValue();

			if ( null != f)
			{
				f.dedupe();
			}
		}
	}
}
