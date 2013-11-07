package com.linkedin.databus.core.monitoring.mbean;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.StatsCollectorCallback;


/**
 *
 * @author snagaraj
 * Collection of T  objects , one for each PhysicalSource
 */
public class StatsCollectors<T extends StatsCollectorMergeable<T>>
{

	/** Elapsed time since the last merge that will trigger a new merge*/
	public static final long OLD_MERGE_THRESHOLD_MS = 10;

	/* Contains per physical partitions stats */
	private final HashMap<String, T> _statsCollectors;

	/* For backward compat - where collectors don't have physical source partition name */
	/* In case of multiple buffers; these hold the aggregate of all individual buffer stats*/
	private T _statsCollector;
	volatile long _lastMergeTstamp = 0;
    private final Logger _log;

	/**
	 * Callback for addition/removal of stats objects
	 */
	private StatsCollectorCallback<T> _statsCallback;

	public StatsCollectors()
	{
		this(null);
	}

	/**
	 * Convert single instance of incoming and outgoing stats Collectors
	 * @param defaultCollector
	 */
	public StatsCollectors(T statsCollector)
	{
		_statsCollectors = new HashMap<String,T> (16);
		_statsCollector = statsCollector;
		_log =  Logger.getLogger(StatsCollectors.class.getName() + "-" +
	   	                         (null != statsCollector ? statsCollector.getClass().getSimpleName()
	   	                                                 : "unknown"));
	}

	public synchronized void addStatsCollector(String name, T coll)
	{
	  _log.info("adding stats collector: " + name + " -> " + coll);
		_statsCollectors.put(name, coll);
		if (_statsCallback != null)
			_statsCallback.addedStats(coll);
	}


	public synchronized T getStatsCollector(String name)
	{
		return _statsCollectors.get(name);
	}

	public T getStatsCollector()
	{
		//avoid frequent merges
		if ((System.currentTimeMillis() - _lastMergeTstamp) > OLD_MERGE_THRESHOLD_MS)
		{
			mergeStatsCollectors();
		}
		return _statsCollector;
	}

	public synchronized T removeStatsCollector(String name)
	{
      _log.info("removing stats collector: " + name);
		T c = _statsCollectors.remove(name);
		if (_statsCallback != null)
			_statsCallback.removedStats(c);
		return c;
	}


	public ArrayList<String> getStatsCollectorKeys()
	{
		ArrayList<String> l = new ArrayList<String>();
		synchronized(this)
		{
			Set<String> set = _statsCollectors.keySet();
			for(String s: set)
			{
				l.add(s);
			}
		}
		return l;
	}


	public ArrayList<T> getStatsCollectors()
	{
		ArrayList<T> l = new ArrayList<T>();
		synchronized (this)
		{
			for (Map.Entry<String,T> entry: _statsCollectors.entrySet())
			{
				l.add(entry.getValue());
			}
		}
		return l;
	}


	public void mergeStatsCollectors()
	{
		ArrayList<T> stats = getStatsCollectors();
    _lastMergeTstamp = System.currentTimeMillis();
		if (_statsCollector != null && null != stats)
		{
			//_statsCollector thread safety assumed : but reset and merge should be atomic
			_statsCollector.resetAndMerge(stats);
		}
	}

	public synchronized StatsCollectorCallback<T> getStatsCollectorCallback()
	{
		return _statsCallback;
	}

	public synchronized void setStatsCollectorCallback(StatsCollectorCallback<T> c)
	{
		_statsCallback = c;
	}
}
