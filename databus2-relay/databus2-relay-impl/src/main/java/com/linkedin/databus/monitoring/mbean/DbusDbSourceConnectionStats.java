package com.linkedin.databus.monitoring.mbean;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.monitoring.events.DbusDbSourceConnectionStatsEvent;

public class DbusDbSourceConnectionStats extends AbstractMonitoringMBean<DbusDbSourceConnectionStatsEvent>
                                      implements DbusDbSourceConnectionStatsMBean
{
  public static final String MODULE = DbusDbSourceConnectionStats.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public DbusDbSourceConnectionStats(int relayId, boolean enabled, boolean threadSafe,
                                  DbusDbSourceConnectionStatsEvent initData)
  {
    super(enabled, threadSafe, initData);
    _event.relayId = relayId;
    reset();
  }

  @Override
  public long getNumClosedDbConns()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numClosedDbConns;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getNumOpenDbConns()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numOpenDbConns;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getNumRowsUpdated()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numRowsUpdated;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeClosedDbConnLifeMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timeClosedDbConnLifeMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeOpenDbConnLifeMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timeOpenDbConnLifeMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimestampLastDbConnCloseMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timestampLastDbConnCloseMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimestampLastDbConnOpenMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timestampLastDbConnOpenMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeSinceLastResetMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timeSinceLastResetMs;
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
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    return new JsonEncoder(_event.getSchema(), out);
  }


  @Override
  public void registerDbConnClose(long timestamp, long lifespan)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      ++ _event.numClosedDbConns;
      -- _event.numOpenDbConns;
      _event.timeClosedDbConnLifeMs += lifespan;
      _event.timestampLastDbConnCloseMs = Math.max(_event.timestampLastDbConnCloseMs, timestamp);
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerDbConnOpen(long timestamp)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      ++ _event.numOpenDbConns;
      _event.timestampLastDbConnOpenMs = Math.max(_event.timestampLastDbConnOpenMs, timestamp);
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerDbRowsRead(int num)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.numRowsUpdated += num;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  protected void cloneData(DbusDbSourceConnectionStatsEvent event)
  {
    event.relayId = _event.relayId;
    event.timestampLastResetMs = _event.timestampLastResetMs;
    event.timeSinceLastResetMs = System.currentTimeMillis() - _event.timestampLastResetMs;

    event.numClosedDbConns = _event.numClosedDbConns;
    event.numOpenDbConns = _event.numOpenDbConns;
    event.numRowsUpdated = _event.numRowsUpdated;
    event.timeClosedDbConnLifeMs = _event.timeClosedDbConnLifeMs;
    event.timeOpenDbConnLifeMs = _event.timeOpenDbConnLifeMs;
    event.timestampLastDbConnCloseMs = _event.timestampLastDbConnCloseMs;
    event.timestampLastDbConnOpenMs = _event.timestampLastDbConnOpenMs;
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    if (! (eventData instanceof DbusDbSourceConnectionStatsEvent))
    {
      LOG.warn("Attempt to merge unknown event class" + eventData.getClass().getName());
      return;
    }
    DbusDbSourceConnectionStatsEvent e = (DbusDbSourceConnectionStatsEvent)eventData;

    /** Allow use negative relay IDs for aggregation across multiple relays */
    if (_event.relayId > 0 && e.relayId != _event.relayId)
    {
      LOG.warn("Attempt to data for a different relay " + e.relayId);
      return;
    }
    _event.numClosedDbConns += e.numClosedDbConns;
    _event.numOpenDbConns += e.numOpenDbConns;
    _event.numRowsUpdated += e.numRowsUpdated;
    _event.timeClosedDbConnLifeMs += e.timeClosedDbConnLifeMs;
    _event.timeOpenDbConnLifeMs += e.timeOpenDbConnLifeMs;
    _event.timestampLastDbConnCloseMs = Math.max(_event.timestampLastDbConnCloseMs,
                                                 e.timestampLastDbConnCloseMs);
    _event.timestampLastDbConnOpenMs = Math.max(_event.timestampLastDbConnOpenMs,
                                               e.timestampLastDbConnOpenMs);
  }

  @Override
  protected SpecificDatumWriter<DbusDbSourceConnectionStatsEvent> getAvroWriter()
  {
    return new SpecificDatumWriter<DbusDbSourceConnectionStatsEvent>(DbusDbSourceConnectionStatsEvent.class);
  }

  @Override
  protected DbusDbSourceConnectionStatsEvent newDataEvent()
  {
    return new DbusDbSourceConnectionStatsEvent();
  }

  @Override
  protected void resetData()
  {
    _event.timestampLastResetMs = System.currentTimeMillis();
    _event.timeSinceLastResetMs = 0;

    _event.numClosedDbConns = 0;
    _event.numOpenDbConns = 0;
    _event.numRowsUpdated = 0;
    _event.timeClosedDbConnLifeMs = 0;
    _event.timeOpenDbConnLifeMs = 0;
    _event.timestampLastDbConnCloseMs = 0;
    _event.timestampLastDbConnOpenMs = 0;
  }

  @Override
  public ObjectName generateObjectName() throws MalformedObjectNameException
  {
    Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
    mbeanProps.put("relayId", Integer.toString(_event.relayId));

    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
  }

}
