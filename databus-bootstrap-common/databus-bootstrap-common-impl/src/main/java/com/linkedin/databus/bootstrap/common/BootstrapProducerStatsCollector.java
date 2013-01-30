package com.linkedin.databus.bootstrap.common;
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
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.monitoring.producer.mbean.DbusBootstrapProducerStats;
import com.linkedin.databus.bootstrap.monitoring.producer.mbean.DbusBootstrapProducerStatsMBean;
import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectorMergeable;
import com.linkedin.databus.core.util.ReadWriteSyncedObject;

public class BootstrapProducerStatsCollector
extends ReadWriteSyncedObject
implements BootstrapProducerStatsCollectorMBean,StatsCollectorMergeable<BootstrapProducerStatsCollector>
{
	private final static String NO_SOURCE = "NONE";

	public static final String MODULE = BootstrapProducerStatsCollector.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	private final DbusBootstrapProducerStats _totalStats;
	private final HashMap<String, DbusBootstrapProducerStats> _perClientStats;
	private final MBeanServer _mbeanServer;
	private final ObjectName _collectorObjName;

	private final int _id;
	private final String _name;
	private final String _perSourcePrefix;
	private final String _curSource;
	private final AtomicBoolean _enabled;
	private final List<String> _logicalSources;


	public BootstrapProducerStatsCollector(int id, String name, boolean enabled, boolean threadSafe,
			MBeanServer mbeanServer, List<String> logicalSources)
	{
		this(id, name, enabled, threadSafe, NO_SOURCE, mbeanServer, logicalSources);
	}

	public BootstrapProducerStatsCollector(
			int id,
			String name,
			boolean enabled,
			boolean threadSafe,
			String source,
			MBeanServer mbeanServer,
			List<String> logicalSources)
	{
		super(threadSafe);
		_mbeanServer = mbeanServer;
		_curSource = source;
		_enabled = new AtomicBoolean(enabled);
		_id = id;
		_name = name;
		_perSourcePrefix = _name + ".source.";
		_logicalSources = logicalSources;

		_totalStats = new DbusBootstrapProducerStats(_id, _name + ".total", enabled, threadSafe, null);
		_perClientStats = new HashMap<String, DbusBootstrapProducerStats>(1000);
		ObjectName jmxName = null;
		try
		{
			Hashtable<String, String> mbeanProps = new Hashtable<String, String>(5);
			mbeanProps.put("name", _name);
			mbeanProps.put("type", BootstrapHttpStatsCollector.class.getSimpleName());
			mbeanProps.put("bootstrap", Integer.toString(id));
			jmxName = new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
		}
		catch (Exception e)
		{
			LOG.error("Error creating JMX object name", e);
		}

		_collectorObjName = jmxName;

		registerAsMBeans();
	}

	@Override
	public void reset()
	{
		_totalStats.reset();

		Lock readLock = acquireReadLock();
		try
		{
			for (String source: _perClientStats.keySet())
			{
				_perClientStats.get(source).reset();
			}
		}
		finally
		{
			releaseLock(readLock);
		}
	}

	@Override
	public boolean isEnabled()
	{
		return _enabled.get();
	}

	@Override
	public void setEnabled(boolean enabled)
	{
		_enabled.set(enabled);
	}

	@Override
	public DbusBootstrapProducerStatsMBean getTotalStats() {
		return _totalStats;
	}

	@Override
	public DbusBootstrapProducerStatsMBean getSourceStats(String source) {
		Lock writeLock = acquireWriteLock();
		try
		{
			DbusBootstrapProducerStatsMBean result = getOrAddPerSourceCollector(source,writeLock);
			return result;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	@Override
	public void registerFellOffRelay() {
		_totalStats.registerFellOffRelay();
	}

	@Override
	public void registerSQLException() {
		_totalStats.registerSQLException();
	}

	@Override
	public void registerBatch(String source, long latency, long numEvents, long currentSCN,
			long currentLogId, long currentRowId)
	{
		Lock writeLock = acquireWriteLock();
		try
		{
			DbusBootstrapProducerStats srcStats = getOrAddPerSourceCollector(source, writeLock);
			srcStats.registerBatch(latency, numEvents, currentSCN, currentLogId, currentRowId);
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	@Override
	public void registerEndWindow(long latency, long numEvents, long currentSCN)
	{
		_totalStats.registerBatch(latency, numEvents, currentSCN, 0, 0);
	}


	protected void registerAsMBeans()
	{
		if (null != _mbeanServer && null != _collectorObjName)
		{
			try
			{
				if (_mbeanServer.isRegistered(_collectorObjName))
				{
					LOG.warn("unregistering stale mbean: " + _collectorObjName);
					_mbeanServer.unregisterMBean(_collectorObjName);
				}
				_totalStats.registerAsMbean(_mbeanServer);
				_mbeanServer.registerMBean(this, _collectorObjName);
				LOG.info("MBean registered " + _collectorObjName);
			}
			catch (Exception e)
			{
				LOG.error("JMX registration failed", e);
			}
		}
	}

	public void unregisterMBeans()
	{
		if (null != _mbeanServer && null != _collectorObjName)
		{
			try
			{
				_mbeanServer.unregisterMBean(_collectorObjName);
				_totalStats.unregisterMbean(_mbeanServer);

				for (String clientName: _perClientStats.keySet())
				{
					_perClientStats.get(clientName).unregisterMbean(_mbeanServer);
				}

				LOG.info("MBean unregistered " + _collectorObjName);
			}
			catch (Exception e)
			{
				LOG.error("JMX deregistration failed", e);
			}
		}
	}

	@Override
  public void merge(BootstrapProducerStatsCollector other)
	{
		_totalStats.mergeStats(other._totalStats);

		Lock otherReadLock = other.acquireReadLock();
		Lock writeLock = acquireWriteLock(otherReadLock);
		try
		{

			for (String sourceName: other._perClientStats.keySet())
			{
				DbusBootstrapProducerStats bean = other._perClientStats.get(sourceName);
				mergePerSource(sourceName, bean, writeLock);
			}
		}
		finally
		{
			releaseLock(writeLock);
			releaseLock(otherReadLock);
		}
	}

	private void mergePerSource(String source, DbusBootstrapProducerStats other, Lock writeLock)
	{
		DbusBootstrapProducerStats curBean = getOrAddPerSourceCollector(source, writeLock);
		curBean.mergeStats(other);
	}

	private DbusBootstrapProducerStats getOrAddPerSourceCollector(String client, Lock writeLock)
	{
		Lock myWriteLock = null;
		if (null == writeLock) myWriteLock = acquireWriteLock();
		try
		{
			DbusBootstrapProducerStats clientStats = _perClientStats.get(client);
			if (null == clientStats)
			{
				clientStats = new DbusBootstrapProducerStats(_id, _perSourcePrefix + client, true, isThreadSafe(), null);
				_perClientStats.put(client, clientStats);

				if (null != _mbeanServer)
				{
					clientStats.registerAsMbean(_mbeanServer);
				}
			}

			return clientStats;
		}
		finally
		{
			releaseLock(myWriteLock);
		}
	}

  public List<String> getLogicalSources()
  {
    return _logicalSources;
  }


	public String getPhysicalSourceName()
	{
			return _name;
	}

@Override
public void resetAndMerge(List<BootstrapProducerStatsCollector> objList) {
	
}


}
