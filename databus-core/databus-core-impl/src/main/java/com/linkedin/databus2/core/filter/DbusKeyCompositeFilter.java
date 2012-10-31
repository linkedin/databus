package com.linkedin.databus2.core.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;

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
			LOG.debug("Filtered Response was :" + allow + " for key :" + e.key());
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
