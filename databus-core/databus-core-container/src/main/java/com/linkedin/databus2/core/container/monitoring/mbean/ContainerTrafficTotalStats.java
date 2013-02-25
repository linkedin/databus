package com.linkedin.databus2.core.container.monitoring.mbean;
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
import java.net.ConnectException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;
import org.jboss.netty.handler.timeout.TimeoutException;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;
import com.linkedin.databus2.core.container.monitoring.events.ContainerTrafficTotalStatsEvent;

public class ContainerTrafficTotalStats
       extends AbstractMonitoringMBean<ContainerTrafficTotalStatsEvent>
       implements ContainerTrafficTotalStatsMBean
{
  public static final String MODULE = ContainerTrafficTotalStats.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final HashSet<Object> _clients;
  private final String _dimension;
  /** Used to keep track of the timestamp for the last open connection for thread-unsafe (per
   * connection) beans*/
  private long _lastConnOpenTimestamp;
  private long _lastConnOpenMergeTimestamp;

  public ContainerTrafficTotalStats(int containerId, String dimension, boolean enabled,
                                    boolean threadSafe, ContainerTrafficTotalStatsEvent initData)
  {
    super(enabled, threadSafe, initData);
    _clients = new HashSet<Object>(1000);
    _dimension = dimension;
    _event.containerId = containerId;
    _event.dimension = dimension;
    reset();
  }

  public ContainerTrafficTotalStats clone(boolean threadSafe)
  {
    return new ContainerTrafficTotalStats(_event.containerId, _dimension, _enabled.get(),
                                          threadSafe, getStatistics(null));
  }

  @Override
  public long getNumBytes()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numBytes;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public int getNumClients()
  {
    Lock readLock = acquireReadLock();
    int result = 0;
    try
    {
      result = _event.numClients;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumClosedConns()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numClosedConns;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumOpenConns()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numOpenConns - _event.numClosedConns;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getTimeSinceLastResetMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return System.currentTimeMillis() - _event.timestampLastResetMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimestampLastResetMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timestampLastResetMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeClosedConnLifeMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timeClosedConnLifeMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeOpenConnLifeMs()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      long now = (0 == _lastConnOpenTimestamp) ? 0 : System.currentTimeMillis();
      long currentOpenTime = (0 == _lastConnOpenTimestamp) ? 0 : now - _lastConnOpenMergeTimestamp;
      _event.timeOpenConnLifeMs += currentOpenTime;
      _lastConnOpenMergeTimestamp = now;
      return _event.timeOpenConnLifeMs - _event.timeClosedConnLifeMs;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public long getErrorTotalCount()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.errorTotalCount;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getErrorConnectCount()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.errorConnectCount;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getErrorTimeoutCount()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.errorTimeoutCount;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getOpenConnsRate()
  {
    return getNumOpenConns();
  }

  @Override
  public long getClosedConnsRate()
  {
    return getNumClosedConns();
  }

  @Override
  public int getClientsRate()
  {
    return getNumClients();
  }

  @Override
  public long getLatencyOpenConn()
  {
    long openConns = getOpenConnsRate();
    return 0 == openConns ? 0 : getTimeOpenConnLifeMs() / openConns;
  }

  @Override
  public long getLatencyClosedConn()
  {
    long closedConns = getClosedConnsRate();
    return 0 == closedConns ? 0 : getTimeClosedConnLifeMs() / closedConns;
  }

  public String getDimension()
  {
    return _dimension;
  }

  public void registerConnectionClose()
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      long now = isThreadSafe() ? 0 : System.currentTimeMillis();
      long connLifespanMs = isThreadSafe() ? 0 : now  - _lastConnOpenTimestamp;
      long unmergedConnLifespanMs = isThreadSafe() ? 0 : now - _lastConnOpenMergeTimestamp;
      _event.numClosedConns++;
      _event.timeOpenConnLifeMs += connLifespanMs;
      _event.timeClosedConnLifeMs += unmergedConnLifespanMs;
      _lastConnOpenTimestamp = 0;
      _lastConnOpenMergeTimestamp = 0;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerConnectionOpen(String client)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _lastConnOpenTimestamp = isThreadSafe() ? 0 : System.currentTimeMillis();
      _lastConnOpenMergeTimestamp = _lastConnOpenTimestamp;

      _event.numOpenConns++;
      _clients.add(client);
      _event.numClients = _clients.size();
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerConnectionError(Throwable error)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      ++_event.errorTotalCount;
      if (error instanceof TimeoutException) ++_event.errorTimeoutCount;
      else if (error instanceof ConnectException) ++_event.errorConnectCount;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void addResponseSize(int bytesSent)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numBytes += bytesSent;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  protected void resetData()
  {
    _event.timestampLastResetMs = System.currentTimeMillis();
    _event.timeSinceLastResetMs = 0;
    _event.numBytes = 0;
    _event.numClients = 0;
    _event.numClosedConns = 0;
    _event.numOpenConns = 0;
    _event.timeClosedConnLifeMs = 0;
    _event.timeOpenConnLifeMs = 0;
    _clients.clear();
    _lastConnOpenTimestamp = 0;
    _lastConnOpenMergeTimestamp = 0;
    _event.errorConnectCount = 0;
    _event.errorTimeoutCount = 0;
    _event.errorTotalCount = 0;
  }

  @Override
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    return new JsonEncoder(_event.getSchema(), out);
  }

  @Override
  protected void cloneData(ContainerTrafficTotalStatsEvent event)
  {
    event.containerId = _event.containerId;
    event.dimension = _event.dimension;
    event.timestampLastResetMs = _event.timestampLastResetMs;
    event.timeSinceLastResetMs = System.currentTimeMillis() - _event.timestampLastResetMs;
    event.numBytes = _event.numBytes;
    event.numClients = _event.numClients;
    event.numClosedConns = _event.numClosedConns;
    event.numOpenConns = _event.numOpenConns;
    event.timeClosedConnLifeMs = _event.timeClosedConnLifeMs;
    event.timeOpenConnLifeMs = _event.timeOpenConnLifeMs;
    event.errorConnectCount = _event.errorConnectCount;
    event.errorTimeoutCount = _event.errorTimeoutCount;
    event.errorTotalCount = _event.errorTotalCount;
  }

  @Override
  protected ContainerTrafficTotalStatsEvent newDataEvent()
  {
    return new ContainerTrafficTotalStatsEvent();
  }

  @Override
  protected SpecificDatumWriter<ContainerTrafficTotalStatsEvent> getAvroWriter()
  {
    return new SpecificDatumWriter<ContainerTrafficTotalStatsEvent>(
        ContainerTrafficTotalStatsEvent.class);
  }

  @Override
  public void mergeStats(DatabusMonitoringMBean<ContainerTrafficTotalStatsEvent> other)
  {
    super.mergeStats(other);
    if (other instanceof ContainerTrafficTotalStats)
    {
      ContainerTrafficTotalStats o = (ContainerTrafficTotalStats)other;
      o.getLatencyOpenConn(); //hack to update the open connections latency which is computed
                              //on demand
      mergeClients(o);
    }
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    if (! (eventData instanceof ContainerTrafficTotalStatsEvent))
    {
      LOG.warn("Attempt to merge unknown event class: " + eventData.getClass().getName());
      return;
    }
    ContainerTrafficTotalStatsEvent e = (ContainerTrafficTotalStatsEvent)eventData;

    /** Allow use negative relay IDs for aggregation across multiple relays */
    if (_event.containerId > 0 && e.containerId != _event.containerId)
    {
      LOG.warn("Attempt to data for a different relay " + e.containerId);
      return;
    }

    _event.numBytes += e.numBytes;
    _event.numClosedConns += e.numClosedConns;
    _event.numOpenConns += e.numOpenConns;
    _event.timeClosedConnLifeMs += e.timeClosedConnLifeMs;
    _event.timeOpenConnLifeMs += e.timeOpenConnLifeMs;
    _event.errorConnectCount += e.errorConnectCount;
    _event.errorTotalCount += e.errorTotalCount;
    _event.errorTimeoutCount += e.errorTimeoutCount;
    // numClients cannot be merged
  }

  /** A bit of a hack to merge state outside the event state */
  private void mergeClients(ContainerTrafficTotalStats other)
  {
    Lock otherReadLock = other.acquireReadLock();
    Lock writeLock = acquireReadLock(otherReadLock);

    try
    {
      _clients.addAll(other._clients);
      _event.numClients = _clients.size();
    }
    finally
    {
      releaseLock(writeLock);
      releaseLock(otherReadLock);
    }
  }

  @Override
  public ObjectName generateObjectName() throws MalformedObjectNameException
  {
    Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
    mbeanProps.put("containerId", Integer.toString(_event.containerId));
    mbeanProps.put("dimension", getDimension());

    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
  }
}
