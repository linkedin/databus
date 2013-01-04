package com.linkedin.databus.core.monitoring.mbean;

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

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEvent.EventScanStatus;
import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;

public class DbusEventsTotalStats extends AbstractMonitoringMBean<DbusEventsTotalStatsEvent>
                                 implements DbusEventsTotalStatsMBean
{
  public static final String MODULE = DbusEventsTotalStats.class.getName();

  private final HashSet<Object> _peers;
  protected final String _dimension;
  private final Logger _log;

  public DbusEventsTotalStats(int ownerId, String dimension,
                              boolean enabled, boolean threadSafe,
                              DbusEventsTotalStatsEvent initData)
  {
    super(enabled, threadSafe, initData);
    _dimension = AbstractMonitoringMBean.sanitizeString(dimension);
    _event.ownerId = ownerId;
    _event.dimension = _dimension;
    _peers = new HashSet<Object>(1000);
    _event.timestampCreated= System.currentTimeMillis();
    _log = Logger.getLogger(MODULE + "." + dimension);
    reset();
  }

  private void resetBufferStats()
  {

    _event.minWinScn = Long.MAX_VALUE;
    _event.maxWinScn = 0;
    _event.sinceWinScn = Long.MAX_VALUE;
    _event.numFreeBytes = 0;
    _event.timestampMinScnEvent = Long.MAX_VALUE;
    _event.timestampMaxScnEvent = 0;

  }

  public DbusEventsTotalStats clone(boolean threadSafe)
  {
    return new DbusEventsTotalStats(_event.ownerId, _dimension, _enabled.get(), threadSafe,
                                    getStatistics(null));
  }

  public long getTimestampMaxScnEvent() {
     return _event.timestampMaxScnEvent;
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
  public long getNumDataEvents()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numDataEvents;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumDataEventsFiltered()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.numDataEventsFiltered;
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
  public long getMaxSeenWinScn()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.maxSeenWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }
  }


  @Override
  public long getMaxFilteredWinScn()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.maxFilteredWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }
  }


  @Override
  public long getMinSeenWinScn()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.minSeenWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getSizeDataEvents()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      long num = _event.numDataEvents;
      result = (0 == num) ? 0 : _event.sizeDataEvents / num;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getSizeDataEventsPayload()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      long num = _event.numDataEvents;
      result = (0 == num) ? 0 : _event.sizeDataEventsPayload / num;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getSizeDataEventsFiltered()
  {
    Lock readLock = acquireReadLock();
    try
    {
      long num = _event.numDataEventsFiltered;
      return (0 == num) ? 0 : _event.sizeDataEventsFiltered / num;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getSizeDataEventsPayloadFiltered()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.sizeDataEventsPayloadFiltered;
    }
    finally
    {
      releaseLock(readLock);
    }

    return result;
  }

  @Override
  public long getNumSysEvents()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numSysEvents;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getSizeSysEvents()
  {
    Lock readLock = acquireReadLock();
    try
    {
      long num = _event.numSysEvents;
      return (0 == num) ? 0 : _event.sizeSysEvents / num;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  public void registerDataEvent(DbusEvent e)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      //ms
      long eventTsInMs = e.timestampInNanos()/(1000*1000);
      long now = System.currentTimeMillis();
      _event.timestampMaxScnEvent = Math.max(_event.timestampMaxScnEvent,eventTsInMs);
      _event.timestampAccessed = now;
      _event.latencyEvent +=  (_event.timestampAccessed > eventTsInMs) ?  _event.timestampAccessed - eventTsInMs : 0;

      _event.numDataEvents++;
      _event.sizeDataEvents += e.size();
      _event.sizeDataEventsPayload += e.payloadLength();
      if (e.sequence() > _event.maxSeenWinScn)
      {
        // We have a new max event
        _event.maxSeenWinScn = e.sequence();
        _event.timeLag = (now > eventTsInMs) ? now - eventTsInMs : 0;
      }
      _event.minSeenWinScn = Math.min(_event.minSeenWinScn,e.sequence());
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerDataEventFiltered(DbusEvent e)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numDataEventsFiltered++;
      _event.sizeDataEventsFiltered += e.size();
      _event.sizeDataEventsPayloadFiltered += e.payloadLength();
      _event.maxFilteredWinScn = Math.max(_event.maxFilteredWinScn,e.sequence());
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerSysEvent(DbusEvent e)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.numSysEvents++;
      _event.sizeSysEvents += e.size();
      long now = System.currentTimeMillis();
      if (e.isEndOfPeriodMarker())
      {
        _event.minSeenWinScn = Math.min(_event.minSeenWinScn,e.sequence());
        long eventTsInMs = e.timestampInNanos()/(1000*1000);
        _event.timestampMaxScnEvent = Math.max(_event.timestampMaxScnEvent,eventTsInMs);
        if (e.sequence() > _event.maxSeenWinScn)
        {
          // We have a new max event
          _event.maxSeenWinScn = e.sequence();
          _event.timeLag = (now > eventTsInMs) ? now - eventTsInMs : 0;
        }
      }
      _event.timestampAccessed = now;
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
    _event.timestampAccessed = 0;
    _event.timeSinceLastResetMs = 0;
    _event.numPeers = 0;
    _event.numDataEvents = 0;
    _event.sizeDataEvents = 0;
    _event.sizeDataEventsPayload = 0;
    _event.numDataEventsFiltered = 0;
    _event.sizeDataEventsFiltered = 0;
    _event.sizeDataEventsPayloadFiltered = 0;
    _event.maxSeenWinScn = Long.MIN_VALUE;
    _event.minSeenWinScn = Long.MAX_VALUE;
    _event.numSysEvents = 0;
    _event.sizeSysEvents = 0;
    _event.numErrHeader = 0;
    _event.numErrPayload = 0;
    _event.numInvalidEvents = 0;
    _event.maxFilteredWinScn = 0;
    _event.latencyEvent = 0;
    _event.maxTimeSpan = Long.MIN_VALUE;    // Makes sense only in the aggregated class.
    _event.minTimeSpan = Long.MAX_VALUE;    // Makes sense only in the aggregated class.
    _event.maxTimestampAccessed = Long.MIN_VALUE;   // Makes sense only in the aggregated class.
    _event.minTimestampAccessed = Long.MAX_VALUE;   // Makes sense only in the aggregated class.
    _event.maxTimestampMaxScnEvent = Long.MIN_VALUE;    // Makes sense only in the aggregated class.
    _event.minTimestampMaxScnEvent = Long.MAX_VALUE;    // Makes sense only in the aggregated class.
    _event.timeLag = 0;
    _event.maxTimeLag = 0;
    _event.minTimeLag = Long.MAX_VALUE;
    resetBufferStats();
    _peers.clear();
  }

  @Override
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    return new JsonEncoder(_event.getSchema(), out);
  }

  /** clone this event to otherEvent atomically **/ 
  public void cloneData(DbusEventsTotalStats otherEvent)
  {
      Lock writeLock = acquireWriteLock();
      try
      {
    	  //note: otherEvent is RHS - and is read; _event is written to 
    	  otherEvent.cloneData(_event);
      }
      finally
      {
    	 releaseLock(writeLock);
      }
  }
  
  @Override
  protected void cloneData(DbusEventsTotalStatsEvent event)
  { 
	  event.ownerId = _event.ownerId;
	  event.dimension = _event.dimension;
	  event.timestampLastResetMs = _event.timestampLastResetMs;
	  event.timeSinceLastResetMs = System.currentTimeMillis() - _event.timestampLastResetMs;
	  event.numPeers = _event.numPeers;
	  event.numDataEvents = _event.numDataEvents;
	  event.sizeDataEvents = _event.sizeDataEvents;
	  event.sizeDataEventsPayload = _event.sizeDataEventsPayload;
	  event.numDataEventsFiltered = _event.numDataEventsFiltered;
	  event.sizeDataEventsFiltered = _event.sizeDataEventsFiltered;
	  event.sizeDataEventsPayloadFiltered = _event.sizeDataEventsPayloadFiltered;
	  event.maxSeenWinScn = _event.maxSeenWinScn;
	  event.minSeenWinScn = _event.minSeenWinScn;
	  event.numSysEvents = _event.numSysEvents;
	  event.sizeSysEvents = _event.sizeSysEvents;
	  event.minWinScn = _event.minWinScn;
	  event.maxWinScn = _event.maxWinScn;
	  event.numErrHeader = _event.numErrHeader;
	  event.numInvalidEvents = _event.numInvalidEvents;
	  event.numErrPayload = _event.numErrPayload;
	  event.timestampCreated = _event.timestampCreated;
	  event.timestampAccessed = _event.timestampAccessed;
	  event.sinceWinScn = _event.sinceWinScn;
	  event.timestampMaxScnEvent = _event.timestampMaxScnEvent;
	  event.timestampMinScnEvent = _event.timestampMinScnEvent;
	  event.maxFilteredWinScn = _event.maxFilteredWinScn;
	  event.latencyEvent = _event.latencyEvent;
	  event.timeLag = _event.timeLag;
	  event.minTimeLag = _event.minTimeLag;
	  event.maxTimeLag = _event.maxTimeLag;
  }

  @Override
  protected DbusEventsTotalStatsEvent newDataEvent()
  {
    return new DbusEventsTotalStatsEvent();
  }

  @Override
  protected SpecificDatumWriter<DbusEventsTotalStatsEvent> getAvroWriter()
  {
    return new SpecificDatumWriter<DbusEventsTotalStatsEvent>(DbusEventsTotalStatsEvent.class);
  }

  @Override
  public void mergeStats(DatabusMonitoringMBean<DbusEventsTotalStatsEvent> other)
  {
    if (!(this instanceof AggregatedDbusEventsTotalStats))
    {
      _log.error("Can use mergeStats only on AggregatedDbusEventsTotalStats");
      throw new RuntimeException("Can use mergeStats only on AggregatedDbusEventsTotalStats");
    }
    super.mergeStats(other);
    if (other instanceof DbusEventsTotalStats)
    {
      mergeClients((DbusEventsTotalStats)other);
    }
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    _log.error("Merging statistics into DbusEventsTotalStats not supported. Use AggregatedDbusEventsTotalStats");
    throw new RuntimeException("Merging statistics into DbusEventsTotalStats not supported. Use AggregatedDbusEventsTotalStats");
    // Merge logic is in AggregatedDbusEventsTotalStats
  }

  /** A bit of a hack to merge state outside the event state */
  private void mergeClients(DbusEventsTotalStats other)
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

  public String getDimension()
  {
    return _dimension;
  }

  public void registerPeer(String peerId)
  {
    Lock writeLock = acquireWriteLock();

    try
    {
      _peers.add(peerId);
      _event.numPeers = _peers.size();
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerEventError(EventScanStatus writingEventStatus)
  {
    if (writingEventStatus != EventScanStatus.OK) {
      if (! _enabled.get()) return;
      Lock writeLock = acquireWriteLock();
      try
      {

        ++_event.numInvalidEvents;
        switch(writingEventStatus) {
        case PARTIAL:
          ++_event.numErrHeader;
          break;
        case ERR:
          ++_event.numErrPayload;
          break;
        case OK: break;//NOOP
        }
      }

      finally
      {
        releaseLock(writeLock);
      }
    }
  }

  public void registerScnRange(long min, long max) {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();
    try {
      _event.minWinScn = min;
      _event.maxWinScn = max;
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public long getNumInvalidEvents()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numInvalidEvents;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getNumHeaderErrEvents()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numErrHeader;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getNumPayloadErrEvents()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numErrPayload;
    }
    finally
    {
      releaseLock(readLock);
    }

  }

  @Override
  public long getMinScn()
  {

    Lock readLock = acquireReadLock();
    try
    {
      return _event.minWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }


  }

  @Override
  public long getMaxScn()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.maxWinScn;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeSinceLastAccess()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return System.currentTimeMillis() - _event.timestampAccessed;
    }
    finally
    {
      releaseLock(readLock);
    }

  }

  @Override
  public long getTimeSinceCreation()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return System.currentTimeMillis()- _event.timestampCreated;
    }
    finally
    {
      releaseLock(readLock);
    }
  }


  public void registerCreationTime(long s)
  {
    Lock writeLock = acquireWriteLock();

    try
    {
      _event.timestampCreated = s;
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public long getFreeSpace()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.numFreeBytes;
    }
    finally
    {
      releaseLock(readLock);
    }

  }

  public void registerBufferMetrics(long min, long max, long since, long freeSpace)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.minWinScn = min;
      _event.maxWinScn = max;
      _event.sinceWinScn = since;
      _event.numFreeBytes = freeSpace;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerTimestampOfFirstEvent(long ts)
  {
    if (! _enabled.get()) return;
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.timestampMinScnEvent =  ts;
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public long getPrevScn() {
	  return _event.sinceWinScn;
  }

  @Override
  public long getTimeLag()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timeLag;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getMinTimeLag()
  {
    return getTimeLag();
  }

  @Override
  public long getMaxTimeLag()
  {
    return getTimeLag();
  }

  @Override
  public long getTimeSpan()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return (_event.timestampMaxScnEvent - _event.timestampMinScnEvent);
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeSinceLastEvent()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return System.currentTimeMillis() - _event.timestampMaxScnEvent ;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  public long getTimestampMinScnEvent()
  {
    return _event.timestampMinScnEvent;
  }

  @Override
  public long getLatencyEvent()
  {
	  Lock readLock = acquireReadLock();
	  try
	  {
		  long num = _event.numDataEvents;
		  return  (num==0) ? 0 : _event.latencyEvent/num;
	  }
	  finally
	  {
		  releaseLock(readLock);
	  }
  }

  // For the methods that make sense only in the aggregated class, return the same value
  // evey time so that we don't make any inferences out of them.
  @Override
  public long getMinTimeSinceLastAccess()
  {
    return 0;
  }

  @Override
  public long getMaxTimeSinceLastAccess()
  {
    return 0;
  }

  @Override
  public long getMinTimeSinceLastEvent()
  {
    return 0;
  }

  @Override
  public long getMaxTimeSinceLastEvent()
  {
    return 0;
  }

  @Override
  public long getMinTimeSpan()
  {
    return 0;
  }

  @Override
  public long getMaxTimeSpan()
  {
    return 0;
  }
}
