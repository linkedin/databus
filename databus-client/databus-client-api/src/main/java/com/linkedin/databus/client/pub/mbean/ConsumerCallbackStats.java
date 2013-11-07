package com.linkedin.databus.client.pub.mbean;
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


import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import com.linkedin.databus.client.pub.monitoring.events.ConsumerCallbackStatsEvent;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectorMergeable;

public class ConsumerCallbackStats extends AbstractMonitoringMBean<ConsumerCallbackStatsEvent>
                                   implements ConsumerCallbackStatsMBean,StatsCollectorMergeable<ConsumerCallbackStats>

{


	private final String _name;
	private final String _dimension;
	private final MBeanServer _mbeanServer;

	public ConsumerCallbackStats(int ownerId, String name, String dimension,
			boolean enabled, boolean threadSafe,
			ConsumerCallbackStatsEvent initData
			) {
		this(ownerId, name, dimension,enabled,threadSafe,initData,null);
	}

	public ConsumerCallbackStats(int ownerId, String name, String dimension,
			boolean enabled, boolean threadSafe,
			ConsumerCallbackStatsEvent initData,
			MBeanServer server) {
		super(enabled, threadSafe, initData);
		_event.ownerId = ownerId;
		_name = name;
		_dimension = dimension;
		_event.dimension = dimension;
		_event.timestampCreationMs = System.currentTimeMillis();
		_event.timestampLastMergeMs =_event.timestampCreationMs;
		_mbeanServer = server;
		resetData();
		registerAsMbean();

	}

	public String getDimension()
	{
		return _dimension;
	}

	public String getName()
	{
		return _name;
	}

	public void registerAsMbean()
	{
		super.registerAsMbean(_mbeanServer);

	}

	public void unregisterAsMbean()
	{
		super.unregisterMbean(_mbeanServer);
	}

	@Override
	public long getTimestampLastResetMs() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.timestampLastResetMs;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getTimeSinceLastResetMs() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampLastResetMs;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public JsonEncoder createJsonEncoder(OutputStream out) throws IOException {
		return new JsonEncoder(_event.getSchema(), out);
	}

	@Override
	public ObjectName generateObjectName() throws MalformedObjectNameException {
		Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
		mbeanProps.put("ownerId", Integer.toString(_event.ownerId));
		mbeanProps.put("dimension", _dimension);
		return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
	}

	@Override
	protected void resetData()
	{
		_event.maxSeenWinScn = 0;
		_event.minSeenWinScn = 0;
		_event.latencyEventsProcessed = 0;
		_event.numDataEventsProcessed = 0;
		_event.numDataEventsReceived = 0;
		_event.numErrorsReceived = 0;
		_event.numErrorsProcessed = 0;
		_event.numSysEventsReceived = 0;
		_event.numSysEventsProcessed = 0;
		_event.numSysErrorsProcessed = 0;
		_event.numDataErrorsProcessed = 0;
		_event.numEventsProcessed = 0;
		_event.numEventsReceived = 0;
		_event.timestampLastResetMs = System.currentTimeMillis();
		_event.timestampLastEventProcessed = 0;
		_event.timestampLastEventReceived = 0;
		_event.timestampOfLastEventReceived = 0;
		_event.timestampOfLastEventProcessed = 0;
		_event.scnOfLastEventProcessed = 0;
		_event.maxSeenWinTimestamp = 0;
	}

	@Override
	protected void cloneData(ConsumerCallbackStatsEvent event)
	{
		event.maxSeenWinScn = _event.maxSeenWinScn ;
		event.minSeenWinScn = _event.minSeenWinScn;

		event.latencyEventsProcessed = _event.latencyEventsProcessed ;

		event.numDataEventsReceived = _event.numDataEventsReceived;
		event.numDataEventsProcessed = _event.numDataEventsProcessed;
		event.numDataErrorsProcessed = _event.numDataErrorsProcessed;

		event.numErrorsReceived = _event.numErrorsReceived;
		event.numErrorsProcessed = _event.numErrorsProcessed;

		event.numSysEventsReceived = _event.numSysEventsReceived;
		event.numSysEventsProcessed = _event.numSysEventsProcessed;
		event.numSysErrorsProcessed = _event.numSysErrorsProcessed;

		event.numEventsProcessed = _event.numEventsProcessed;
		event.numEventsReceived = _event.numEventsReceived;

		event.scnOfLastEventProcessed = _event.scnOfLastEventProcessed;
		event.maxSeenWinTimestamp = _event.maxSeenWinTimestamp;
	}

	@Override
	protected ConsumerCallbackStatsEvent newDataEvent() {
		return new ConsumerCallbackStatsEvent();
	}

	@Override
	protected SpecificDatumWriter<ConsumerCallbackStatsEvent> getAvroWriter() {
		return new SpecificDatumWriter<ConsumerCallbackStatsEvent>(ConsumerCallbackStatsEvent.class);
	}


	@Override
	protected void doMergeStats(Object eventData)
	{
		if (! (eventData instanceof ConsumerCallbackStatsEvent))
		{
		  if (! (eventData instanceof ConsumerCallbackStats))
		  {
			LOG.warn("Attempt to merge unknown event class" + eventData.getClass().getName());
			return;
		  }
		  eventData = ((ConsumerCallbackStats)eventData)._event;
		}
		ConsumerCallbackStatsEvent e = (ConsumerCallbackStatsEvent)eventData;

		_event.numDataEventsReceived += e.numDataEventsReceived;
		_event.numDataEventsProcessed += e.numDataEventsProcessed;
		_event.numDataErrorsProcessed += e.numDataErrorsProcessed;

		_event.numEventsReceived += e.numEventsReceived;
		_event.numEventsProcessed += e.numEventsProcessed;

		_event.numErrorsReceived += e.numErrorsReceived;
		_event.numErrorsProcessed += e.numErrorsProcessed;

		_event.numSysEventsProcessed += e.numSysEventsProcessed;
		_event.numSysEventsReceived+=e.numSysEventsReceived;
		_event.numSysErrorsProcessed += e.numSysErrorsProcessed;

		_event.latencyEventsProcessed += e.latencyEventsProcessed;
		_event.minSeenWinScn = _event.minSeenWinScn != 0 ? Math.min(_event.minSeenWinScn,e.minSeenWinScn) : e.minSeenWinScn;
		_event.maxSeenWinScn = _event.maxSeenWinScn != 0 ? Math.max(_event.maxSeenWinScn,e.maxSeenWinScn) : e.maxSeenWinScn;
		_event.timestampLastMergeMs = System.currentTimeMillis();
		_event.timestampLastEventProcessed = _event.timestampLastEventProcessed != 0 ? Math.max(_event.timestampLastEventProcessed,e.timestampLastEventProcessed) : e.timestampLastEventProcessed;
		_event.timestampLastEventReceived = _event.timestampLastEventReceived != 0 ? Math.max(_event.timestampLastEventReceived,e.timestampLastEventReceived) : e.timestampLastEventReceived;
		_event.timestampOfLastEventProcessed = _event.timestampOfLastEventProcessed != 0 ? Math.max(_event.timestampOfLastEventProcessed,e.timestampOfLastEventProcessed) : e.timestampOfLastEventProcessed;
		_event.timestampOfLastEventReceived = _event.timestampOfLastEventReceived != 0 ? Math.max(_event.timestampOfLastEventReceived,e.timestampOfLastEventReceived) : e.timestampOfLastEventReceived;

		_event.scnOfLastEventProcessed = 0 != _event.scnOfLastEventProcessed ? Math.max(_event.scnOfLastEventProcessed, e.scnOfLastEventProcessed) : e.scnOfLastEventProcessed;
        _event.maxSeenWinTimestamp = 0 != _event.maxSeenWinTimestamp ? Math.max(_event.maxSeenWinTimestamp, e.maxSeenWinTimestamp) : e.maxSeenWinTimestamp;
	}


	@Override
	public void resetAndMerge(List<ConsumerCallbackStats> objList) 
	{
		Lock writeLock = acquireWriteLock();
		try
		{
			
			reset();
			for (ConsumerCallbackStats t: objList)
			{
				merge(t);
			}
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	@Override
	public long getNumDataEventsReceived()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numDataEventsReceived;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getNumDataEventsProcessed()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numDataEventsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;

	}

	@Override
  public long getNumDataErrorsProcessed()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numDataErrorsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;

	}

	@Override
	public long getNumSysEventsReceived()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numSysEventsReceived;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getMinSeenWinScn() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.minSeenWinScn;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getMaxSeenWinScn() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.maxSeenWinScn;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getNumErrorsReceived() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numErrorsReceived;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getNumErrorsProcessed() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numErrorsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getTimeSinceCreation() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampCreationMs;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public long getLatencyEventsProcessed() {
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.latencyEventsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;
	}

	@Override
	public double getAveLatencyEventsProcessed() {
		Lock readLock = acquireReadLock();
		double result = 0;
		try
		{
			result = (_event.numEventsProcessed != 0) ? (double) _event.latencyEventsProcessed/_event.numEventsProcessed : 0.0;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	public void registerDataEventReceived(DbusEvent e)
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numEventsReceived++;
			_event.numDataEventsReceived++;
			_event.timestampLastEventReceived = System.currentTimeMillis();
			_event.timestampOfLastEventReceived = e.timestampInNanos()/1000000;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerSystemEventReceived()
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numEventsReceived++;
			_event.numSysEventsReceived++;
			_event.timestampLastEventReceived = System.currentTimeMillis();
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerSystemEventProcessed(long timeElapsed)
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numEventsProcessed++;
			_event.numSysEventsProcessed++;
			_event.timestampLastEventProcessed = System.currentTimeMillis();
			_event.latencyEventsProcessed += timeElapsed;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}


	public void registerEventsReceived(int size)
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numEventsReceived+=size;
			_event.timestampLastEventReceived = System.currentTimeMillis();
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerEventsProcessed(int size,long timeElapsed)
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numEventsProcessed+=size;
			_event.timestampLastEventProcessed = System.currentTimeMillis();
			_event.latencyEventsProcessed+=timeElapsed;
		}
		finally
		{
			releaseLock(writeLock);
		}

	}

	public void registerDataEventsProcessed(int size, long timeElapsed, DbusEvent e)
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numEventsProcessed+=size;
			_event.numDataEventsProcessed+=size;
			_event.latencyEventsProcessed+=timeElapsed;
			_event.timestampLastEventProcessed = System.currentTimeMillis();
			_event.timestampOfLastEventProcessed = e.timestampInNanos()/1000000;
			_event.scnOfLastEventProcessed = e.sequence();
		}
		finally
		{
			releaseLock(writeLock);
		}
	}


	public void registerErrorEventsProcessed(int size)
	{

		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numErrorsProcessed+=size;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerSysErrorsProcessed()
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numErrorsProcessed++;
			_event.numSysErrorsProcessed++;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerDataErrorsProcessed()
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numErrorsProcessed++;
			_event.numDataErrorsProcessed++;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerSrcErrors()
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();

		try
		{
			_event.numErrorsReceived++;
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

	public void registerWindowSeen(long timestamp, long scn)
	{
      Lock writeLock = acquireWriteLock();
      try
      {
        _event.maxSeenWinScn = _event.maxSeenWinScn==0 ? scn : Math.max(_event.maxSeenWinScn, scn);
        _event.minSeenWinScn = _event.minSeenWinScn==0 ? scn : Math.min(_event.minSeenWinScn, scn);
        _event.maxSeenWinTimestamp = 0 == _event.maxSeenWinTimestamp ? timestamp / 1000000 :
            Math.max(_event.maxSeenWinTimestamp, timestamp / 1000000);
      }
      finally
      {
          releaseLock(writeLock);
      }
	}

	@Override
	/** Deprecated **/
	public long getTimeSinceLastMergeMs()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampLastMergeMs;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public long getTimeSinceLastEventReceived()
	{

		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampLastEventReceived;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;

	}

	@Override
	public long getTimeSinceLastEventProcessed()
	{

		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampLastEventProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public long getTimeDiffLastEventReceived()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampOfLastEventReceived;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;

	}

	@Override
	public long getTimeDiffLastEventProcessed()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = System.currentTimeMillis() - _event.timestampOfLastEventProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public long getNumSysErrorsProcessed()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numSysErrorsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}

		return result;


	}

	@Override
	public long getNumEventsReceived()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numEventsReceived;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public long getNumEventsProcessed()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numEventsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public long getNumSysEventsProcessed()
	{
		Lock readLock = acquireReadLock();
		long result = 0;
		try
		{
			result = _event.numSysEventsProcessed;
		}
		finally
		{
			releaseLock(readLock);
		}
		return result;
	}

	@Override
	public void merge(ConsumerCallbackStats obj)
	{
		if (! _enabled.get()) return;
		Lock writeLock = acquireWriteLock();
		try
		{
			doMergeStats(obj);
		}
		finally
		{
			releaseLock(writeLock);
		}
	}

  @Override
  public long getScnOfLastEventProcessed()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
        result = _event.scnOfLastEventProcessed;
    }
    finally
    {
        releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMaxSeenWinTimestamp()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
        result = _event.maxSeenWinTimestamp;
    }
    finally
    {
        releaseLock(readLock);
    }
    return result;
  }
}
