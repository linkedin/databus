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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;
import com.linkedin.databus2.core.container.monitoring.events.DbusHttpTotalStatsEvent;

public class DbusHttpTotalStats extends AbstractMonitoringMBean<DbusHttpTotalStatsEvent>
                                       implements DbusHttpTotalStatsMBean
{
  public static final String MODULE = DbusHttpTotalStats.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final HashSet<Object> _peers;
  private final String _dimension;

  public DbusHttpTotalStats(int ownerId, String dimesion, boolean enabled, boolean threadSafe,
                                   DbusHttpTotalStatsEvent initData)
  {
    super(enabled, threadSafe, initData);
    _event.ownerId = ownerId;
    _event.dimension = dimesion;
    _peers = new HashSet<Object>(1000);
    _dimension = dimesion;
    reset();
  }

  public DbusHttpTotalStats clone(boolean threadSafe)
  {
    return new DbusHttpTotalStats(_event.ownerId, _dimension, _enabled.get(), threadSafe,
                                         getStatistics(null));
  }

  @Override
  public int getNumPeers()
  {
    Lock readLock = acquireReadLock();
    int result = 0;
    try
    {
      result = _event.numPeers;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public int getNumRegisterCalls()
  {
    Lock readLock = acquireReadLock();
    int result = 0;
    try
    {
      result = _event.numRegisterCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public int getNumSourcesCalls()
  {
    Lock readLock = acquireReadLock();
    int result = 0;
    try
    {
      result = _event.numSourcesCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumStreamCalls()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numStreamCalls;
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
  public long getTimestampLastResetMs()
  {
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
  public long getMaxStreamWinScn()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.maxStreamWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getMinStreamWinScn()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.minStreamWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public double getLatencyStreamCalls()
  {
    Lock readLock = acquireReadLock();
    try
    {
      long numCalls = _event.numStreamCalls;
      return (0 == numCalls) ? 0.0 : 1.0 * _event.timeStreamCallsMs / numCalls;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  /** Not thread-safe! Make sure you are holding a write lock*/
  private void registerPeer(String peer)
  {
    _peers.add(peer);
    _event.numPeers = _peers.size();
  }

  public void registerRegisterCall(String peer)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numRegisterCalls++;
      registerPeer(peer);
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerSourcesCall(String peer)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numSourcesCalls++;
      registerPeer(peer);
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerStreamResponse(long totalTime)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numStreamCalls++;
      _event.timeStreamCallsMs += totalTime;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerStreamRequest(String peer, Checkpoint cp)
  {
    long winScn = cp.getWindowScn();

    Lock writeLock = acquireWriteLock();
    try
    {
      _event.maxStreamWinScn = maxValue(_event.maxStreamWinScn, winScn);
      _event.minStreamWinScn = minValue(_event.minStreamWinScn, winScn);
      registerPeer(peer);
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
    _event.numPeers = 0;
    _event.numRegisterCalls = 0;
    _event.numSourcesCalls = 0;
    _event.numStreamCalls = 0;
    _event.timeStreamCallsMs = 0;
    _event.maxStreamWinScn = DEFAULT_MAX_LONG_VALUE;
    _event.minStreamWinScn = DEFAULT_MIN_LONG_VALUE;
    _event.numErrInvalidParamsRegisterCalls = 0;
    _event.numErrRegisterCalls = 0;
    _event.numErrInvalidParamsStreamCalls = 0;
    _event.numErrScnNotFoundStreamCalls = 0;
    _event.numErrSourcesCalls = 0;
    _event.numErrStreamCalls = 0;
    _event.numErrInvalidParamsRegisterCalls = 0;
    _event.numErrInvalidParamsSourcesCalls = 0;
    _event.mastershipStatus = 0;
    _peers.clear();
  }

  @Override
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    return new JsonEncoder(_event.getSchema(), out);
  }

  @Override
  protected void cloneData(DbusHttpTotalStatsEvent event)
  {
    event.ownerId = _event.ownerId;
    event.dimension = _event.dimension;
    event.timestampLastResetMs = _event.timestampLastResetMs;
    event.timeSinceLastResetMs = System.currentTimeMillis() - _event.timestampLastResetMs;
    event.numPeers = _event.numPeers;
    event.numRegisterCalls = _event.numRegisterCalls;
    event.numSourcesCalls = _event.numSourcesCalls;
    event.numStreamCalls = _event.numStreamCalls;
    event.timeStreamCallsMs  = _event.timeStreamCallsMs;
    event.maxStreamWinScn = _event.maxStreamWinScn;
    event.minStreamWinScn = _event.minStreamWinScn;
    event.numErrInvalidParamsRegisterCalls = _event.numErrInvalidParamsRegisterCalls;
    event.numErrRegisterCalls = _event.numErrRegisterCalls;
    event.numErrInvalidParamsStreamCalls = _event.numErrInvalidParamsStreamCalls;
    event.numErrScnNotFoundStreamCalls = _event.numErrScnNotFoundStreamCalls;
    event.numErrSourcesCalls = _event.numErrSourcesCalls;
    event.numErrStreamCalls = _event.numErrStreamCalls;
    event.numErrInvalidParamsRegisterCalls = _event.numErrInvalidParamsRegisterCalls;
    event.numErrInvalidParamsSourcesCalls = _event.numErrInvalidParamsSourcesCalls;
    event.mastershipStatus = _event.mastershipStatus;
  }

  @Override
  protected DbusHttpTotalStatsEvent newDataEvent()
  {
    return new DbusHttpTotalStatsEvent();
  }

  @Override
  protected SpecificDatumWriter<DbusHttpTotalStatsEvent> getAvroWriter()
  {
    return new SpecificDatumWriter<DbusHttpTotalStatsEvent>(DbusHttpTotalStatsEvent.class);
  }

  @Override
  public void mergeStats(DatabusMonitoringMBean<DbusHttpTotalStatsEvent> other)
  {
    super.mergeStats(other);
    if (other instanceof DbusHttpTotalStats)
    {
      mergeClients((DbusHttpTotalStats)other);
    }
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    if (! (eventData instanceof DbusHttpTotalStatsEvent))
    {
      LOG.warn("Attempt to merge unknown event class" + eventData.getClass().getName());
      return;
    }
    DbusHttpTotalStatsEvent e = (DbusHttpTotalStatsEvent)eventData;

    /** Allow use negative relay IDs for aggregation across multiple relays */
    if (_event.ownerId > 0 && e.ownerId != _event.ownerId)
    {
      LOG.warn("Attempt to data for a different relay " + e.ownerId);
      return;
    }

    _event.numPeers += e.numPeers;
    _event.numRegisterCalls += e.numRegisterCalls;
    _event.numSourcesCalls += e.numSourcesCalls;
    _event.numStreamCalls += e.numStreamCalls;
    _event.timeStreamCallsMs += e.timeStreamCallsMs;
    _event.maxStreamWinScn = maxValue(_event.maxStreamWinScn, e.maxStreamWinScn);
    _event.minStreamWinScn = minValue(_event.minStreamWinScn, e.minStreamWinScn);
    _event.numErrInvalidParamsRegisterCalls += e.numErrInvalidParamsRegisterCalls;
    _event.numErrRegisterCalls += e.numErrRegisterCalls;
    _event.numErrInvalidParamsStreamCalls += e.numErrInvalidParamsStreamCalls;
    _event.numErrScnNotFoundStreamCalls += e.numErrScnNotFoundStreamCalls;
    _event.numErrSourcesCalls += e.numErrSourcesCalls;
    _event.numErrStreamCalls += e.numErrStreamCalls;
    _event.numErrInvalidParamsRegisterCalls += e.numErrInvalidParamsRegisterCalls;
    _event.numErrInvalidParamsSourcesCalls += e.numErrInvalidParamsSourcesCalls;
    _event.mastershipStatus  = Math.max(e.mastershipStatus,_event.mastershipStatus);
    // numClients cannot be merged
  }

  /** A bit of a hack to merge state outside the event state */
  private void mergeClients(DbusHttpTotalStats other)
  {
    Lock otherReadLock = other.acquireReadLock();
    Lock writeLock = acquireWriteLock(otherReadLock);

    try
    {
      _peers.addAll(other._peers);
      _event.numPeers = _peers.size();
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
    mbeanProps.put("ownerId", Integer.toString(_event.ownerId));
    mbeanProps.put("dimension", _dimension);

    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
  }

  @Override
  public String getDimension()
  {
    return _dimension;
  }

  public void registerInvalidStreamRequest(String peer)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      registerPeer(peer);
      _event.numErrStreamCalls++;
      _event.numErrInvalidParamsStreamCalls++;

    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerScnNotFoundStreamResponse(String peer)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      registerPeer(peer);
      _event.numErrStreamCalls++;
      _event.numErrScnNotFoundStreamCalls++;

    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerInvalidSourceRequest(String peer)
  {
    Lock writeLock = acquireWriteLock();
    try
    {

      registerPeer(peer);
      _event.numErrInvalidParamsSourcesCalls++;
      _event.numErrSourcesCalls++;
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  public void registerInvalidRegisterCall(String peer)
  {
    Lock writeLock = acquireWriteLock();
    try
    {

      registerPeer(peer);
      _event.numErrStreamCalls++;
      _event.numErrInvalidParamsRegisterCalls++;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public long getNumScnNotFoundStream()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrScnNotFoundStreamCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrStream()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrStreamCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrStreamReq()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrInvalidParamsStreamCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrRegister()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrRegisterCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrRegisterReq()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrInvalidParamsRegisterCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;

  }

  @Override
  public long getNumErrSources()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrSourcesCalls;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrSourcesReq()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrInvalidParamsSourcesCalls;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public int getMastershipStatus()
  {
    Lock readLock = acquireReadLock();
    int result = 0;
    try
    {
      result = _event.mastershipStatus;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  public void registerMastershipStatus(int i)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.mastershipStatus = i;

    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public double getHttpErrorRate()
  {
    Lock readLock = acquireReadLock();
    try
    {
      long totalCalls = _event.numStreamCalls + _event.numRegisterCalls + _event.numSourcesCalls;
      long totalErrors = _event.numErrStreamCalls + _event.numErrSourcesCalls +
                         _event.numErrRegisterCalls;
      return 0 == totalCalls ? 0.0 : 1.0 * totalErrors / totalCalls;
    }
    finally
    {
      releaseLock(readLock);
    }
  }
}
