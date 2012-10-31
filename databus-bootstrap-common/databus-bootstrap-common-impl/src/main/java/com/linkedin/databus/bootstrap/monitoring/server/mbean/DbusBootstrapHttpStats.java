package com.linkedin.databus.bootstrap.monitoring.server.mbean;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import com.linkedin.databus.bootstrap.monitoring.server.events.DbusBootstrapHttpStatsEvent;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;

public class DbusBootstrapHttpStats extends AbstractMonitoringMBean<DbusBootstrapHttpStatsEvent> implements DbusBootstrapHttpStatsMBean
{

  public DbusBootstrapHttpStats(int id,
                                String dimension,
                                boolean enabled,
                                boolean threadSafe,
                                DbusBootstrapHttpStatsEvent initData)
  {
    super(enabled, threadSafe, initData);
    _event.ownerId = id;
    _event.dimension = dimension;
    reset();

  }

  @Override
  public long getNumReqBootstrap()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrReqBootstrap;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;

  }

  @Override
  public long getNumReqSnapshot()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numReqSnapshot;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumReqCatchup()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numReqCatchup;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrReqBootstrap()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrReqBootstrap;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrReqDatabaseTooOld()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrReqDatabaseTooOld;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrSqlException()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrSqlException;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumReqStartSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numReqStartSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumReqTargetSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numReqTargetSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrStartSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrTargetSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumErrTargetSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numErrTargetSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getLatencySnapshot()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.latencySnapshot;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getLatencyCatchup()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.latencyCatchup;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;

  }

  @Override
  public long getLatencyStartSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.latencyStartSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getLatencyTargetSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.latencyTargetSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getSizeBatch()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.sizeBatch;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getMinBootstrapSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.minBootstrapSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getMaxBootstrapSCN()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.maxBootstrapSCN;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public void registerBootStrapReq(Checkpoint cp, long latency, long size)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numErrReqBootstrap++;
      DbusClientMode mode = cp.getConsumptionMode();
      if (mode==DbusClientMode.BOOTSTRAP_CATCHUP) {
        _event.latencyCatchup += latency;
        _event.numReqCatchup++;
      } else if (mode==DbusClientMode.BOOTSTRAP_SNAPSHOT) {
        _event.numReqSnapshot++;
        _event.latencySnapshot += latency;
      }
      _event.maxBootstrapSCN = Math.max(cp.getWindowScn(),_event.maxBootstrapSCN);
      _event.minBootstrapSCN = Math.min(cp.getWindowScn(),_event.minBootstrapSCN);
      _event.sizeBatch += size;
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public void registerStartSCNReq(long latency)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numReqStartSCN++;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerTargetSCNReq(long latency)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numReqTargetSCN++;
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public void registerErrBootstrap()
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numErrReqBootstrap++;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerErrStartSCN()
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numErrStartSCN++;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerErrTargetSCN()
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numErrTargetSCN++;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerErrSqlException()
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numErrSqlException++;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerErrDatabaseTooOld()
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
     _event.numErrReqDatabaseTooOld++;
    }
    finally
    {
      releaseLock(writeLock);
    }
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
  public long getTimeSinceLastResetMs()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.timeSinceLastResetMs;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    return new JsonEncoder(_event.getSchema(), out);
  }

  @Override
  public ObjectName generateObjectName() throws MalformedObjectNameException
  {
    Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
    mbeanProps.put("ownerId", Integer.toString(_event.ownerId));
    mbeanProps.put("dimension", _event.dimension.toString());
    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
  }

  @Override
  protected void resetData()
  {
    _event.timestampLastResetMs = System.currentTimeMillis();
    _event.timeSinceLastResetMs = 0;
    _event.numReqBootstrap=0;
    _event.numReqSnapshot=0;
    _event.numReqCatchup=0;
    _event.numErrReqBootstrap=0;
    _event.numErrReqDatabaseTooOld=0;
    _event.numErrSqlException=0;
    _event.numReqStartSCN=0;
    _event.numReqTargetSCN=0;
    _event.numErrStartSCN=0;
    _event.numErrTargetSCN=0;
    _event.latencySnapshot=0;
    _event.latencyCatchup=0;
    _event.latencyStartSCN=0;
    _event.latencyTargetSCN=0;
    _event.sizeBatch=0;
    _event.minBootstrapSCN=Long.MAX_VALUE;
    _event.maxBootstrapSCN=Long.MIN_VALUE;

  }

  @Override
  protected void cloneData(DbusBootstrapHttpStatsEvent eventData)
  {
    eventData.timestampLastResetMs=_event.timestampLastResetMs;
    eventData.timeSinceLastResetMs=_event.timeSinceLastResetMs;
    eventData.numReqBootstrap=_event.numReqBootstrap;
    eventData.numReqSnapshot=_event.numReqSnapshot;
    eventData.numReqCatchup=_event.numReqCatchup;
    eventData.numErrReqBootstrap=_event.numErrReqBootstrap;
    eventData.numErrReqDatabaseTooOld=_event.numErrReqDatabaseTooOld;
    eventData.numErrSqlException=_event.numErrSqlException;
    eventData.numReqStartSCN=_event.numReqStartSCN;
    eventData.numReqTargetSCN=_event.numReqTargetSCN;
    eventData.numErrStartSCN=_event.numErrStartSCN;
    eventData.numErrTargetSCN=_event.numErrTargetSCN;
    eventData.latencySnapshot=_event.latencySnapshot;
    eventData.latencyCatchup=_event.latencyCatchup;
    eventData.latencyStartSCN=_event.latencyStartSCN;
    eventData.latencyTargetSCN=_event.latencyTargetSCN;
    eventData.sizeBatch=_event.sizeBatch;
    eventData.minBootstrapSCN=_event.minBootstrapSCN;
    eventData.maxBootstrapSCN=_event.maxBootstrapSCN;
  }

  @Override
  protected DbusBootstrapHttpStatsEvent newDataEvent()
  {
    return new DbusBootstrapHttpStatsEvent();
  }

  @Override
  protected SpecificDatumWriter<DbusBootstrapHttpStatsEvent> getAvroWriter()
  {
    return new SpecificDatumWriter<DbusBootstrapHttpStatsEvent>(DbusBootstrapHttpStatsEvent.class);
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    if (! (eventData instanceof DbusBootstrapHttpStatsEvent))
    {
      LOG.warn("Attempt to merge unknown event class" + eventData.getClass().getName());
      return;
    }
    DbusBootstrapHttpStatsEvent e = (DbusBootstrapHttpStatsEvent) eventData;

    /** Allow use negative relay IDs for aggregation across multiple relays */
    if (_event.ownerId > 0 && e.ownerId != _event.ownerId)
    {
      LOG.warn("Attempt to data for a different relay " + e.ownerId);
      return;
    }

    _event.numReqBootstrap+=e.numReqBootstrap;
    _event.numReqSnapshot+=e.numReqSnapshot;
    _event.numReqCatchup+=e.numReqCatchup;
    _event.numErrReqBootstrap+=e.numErrReqBootstrap;
    _event.numErrReqDatabaseTooOld+=e.numErrReqDatabaseTooOld;
    _event.numErrSqlException+=e.numErrSqlException;
    _event.numReqStartSCN+=e.numReqStartSCN;
    _event.numReqTargetSCN+=e.numReqTargetSCN;
    _event.numErrStartSCN+=e.numErrStartSCN;
    _event.numErrTargetSCN+=e.numErrTargetSCN;
    _event.latencySnapshot+=e.latencySnapshot;
    _event.latencyCatchup+=e.latencyCatchup;
    _event.latencyStartSCN+=e.latencyStartSCN;
    _event.latencyTargetSCN+=e.latencyTargetSCN;
    _event.sizeBatch+=e.sizeBatch;
    _event.minBootstrapSCN=Math.min(_event.minBootstrapSCN,e.minBootstrapSCN);
    _event.maxBootstrapSCN=Math.max(_event.maxBootstrapSCN,e.maxBootstrapSCN);

  }

}
