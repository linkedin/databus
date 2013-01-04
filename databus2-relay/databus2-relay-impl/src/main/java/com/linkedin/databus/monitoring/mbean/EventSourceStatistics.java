/*
 * $Id: EventSourceStatistics.java 265083 2011-04-28 22:18:04Z snagaraj $
 */
package com.linkedin.databus.monitoring.mbean;



/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 265083 $
 */
public class EventSourceStatistics
implements EventSourceStatisticsMBean
{
  private final String _sourceName;
  private int _numConsecutiveCyclesWithEvents;
  private int _numConsecutiveCyclesWithoutEvents;
  private int _numCyclesWithEvents;
  private int _numCyclesWithoutEvents;
  private int _totalEvents;
  private long _totalEventSerializedSize;
  private long _totalEventFactoryTimeMillis;
  private long _lastCycleWithEventsTimestamp;
  private long _maxScn;
  private long _maxDBScn;
  private long _numErrors;
  private long _timestampLastDBAccess;

  public EventSourceStatistics(String sourceName)
  {
    _sourceName = sourceName;
  }

  public EventSourceStatistics(String name, int numDataEvents,
		long timeSinceLastAccess, long maxScn, long numErrors,
		long sizeDataEvents)
{
	  _sourceName = name;
	  _totalEvents=numDataEvents;
	  _timestampLastDBAccess = timeSinceLastAccess;
	  _maxScn = maxScn;
	  _totalEventSerializedSize = sizeDataEvents;
	  _numErrors = numErrors;
	  _totalEventFactoryTimeMillis = 0;
	  _numConsecutiveCyclesWithEvents = 0;
	  _numConsecutiveCyclesWithoutEvents = 0;
	  _numCyclesWithEvents = 0;
	  _numCyclesWithoutEvents = 0;
	  _lastCycleWithEventsTimestamp = 0;
}

public synchronized void addEmptyEventCycle()
  {
    // Cycle did not have events
    _numConsecutiveCyclesWithEvents = 0;
    _numConsecutiveCyclesWithoutEvents ++;
    _numCyclesWithoutEvents ++;
  }

  public synchronized void addEventCycle(int numEvents, long eventFactoryTimeMillis, long eventSerializedSize, long maxScn)
  {
    if(numEvents > 0)
    {
      // Cycle had events
      _numConsecutiveCyclesWithEvents ++;
      _numConsecutiveCyclesWithoutEvents = 0;
      _numCyclesWithEvents ++;
      _totalEvents += numEvents;
      _totalEventSerializedSize += eventSerializedSize;
      _totalEventFactoryTimeMillis += eventFactoryTimeMillis;
      _lastCycleWithEventsTimestamp = System.currentTimeMillis();
      _maxScn = maxScn;
    }
    else
    {
      addEmptyEventCycle();
    }
  }

  public synchronized void addError()
  {
     ++_numErrors;
  }


  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getAvgEventFactoryTimeMillisPerEvent()
   */
  @Override
  public synchronized long getAvgEventFactoryTimeMillisPerEvent()
  {
    return _totalEvents != 0 ? _totalEventFactoryTimeMillis / _totalEvents : 0;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getAvgEventSerializedSize()
   */
  @Override
  public synchronized long getAvgEventSerializedSize()
  {
    return _totalEvents != 0 ? _totalEventSerializedSize / _totalEvents : 0;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getAvgNumEventsPerNonEmptyCycle()
   */
  @Override
  public synchronized int getAvgNumEventsPerNonEmptyCycle()
  {
    return (_numCyclesWithEvents != 0 ? _totalEvents / _numCyclesWithEvents : 0);
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getNumConsecutiveCyclesWithEvents()
   */
  @Override
  public synchronized int getNumConsecutiveCyclesWithEvents()
  {
    return _numConsecutiveCyclesWithEvents;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getNumConsecutiveCyclesWithoutEvents()
   */
  @Override
  public synchronized int getNumConsecutiveCyclesWithoutEvents()
  {
    return _numConsecutiveCyclesWithoutEvents;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getNumCyclesWithEvents()
   */
  @Override
  public synchronized int getNumCyclesWithEvents()
  {
    return _numCyclesWithEvents;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getNumCyclesWithoutEvents()
   */
  @Override
  public synchronized int getNumCyclesWithoutEvents()
  {
    return _numCyclesWithoutEvents;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getSourceName()
   */
  @Override
  public synchronized String getSourceName()
  {
    return _sourceName;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getNumCyclesTotal()
   */
  @Override
  public synchronized int getNumCyclesTotal()
  {
    return _numCyclesWithEvents + _numCyclesWithoutEvents;
  }

  @Override
  public synchronized long getMillisSinceLastCycleWithEvents()
  {
    return System.currentTimeMillis() - _lastCycleWithEventsTimestamp;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getMaxScn()
   */
  @Override
  public synchronized long getMaxScn()
  {
    return _maxScn;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#getTotalEvents()
   */
  @Override
  public synchronized int getNumTotalEvents()
  {
    return _totalEvents;
  }

  /*
   * @see com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean#reset()
   */
  @Override
  public synchronized void reset()
  {
    _numConsecutiveCyclesWithEvents = 0;
    _numConsecutiveCyclesWithoutEvents = 0;
    _numCyclesWithEvents = 0;
    _numCyclesWithoutEvents = 0;
    _totalEvents = 0;
    _totalEventSerializedSize = 0;
    _totalEventFactoryTimeMillis = 0;
    _lastCycleWithEventsTimestamp = 0;
    _maxScn = 0;
    _maxDBScn = 0;
    _numErrors = 0;
    _timestampLastDBAccess = 0;
  }

  public synchronized void addTimeOfLastDBAccess(long ts)
  {
    _timestampLastDBAccess = ts;
  }

  public synchronized void addMaxDBScn(long dbscn)
  {
	   _maxDBScn = dbscn;
  }

  @Override
  public synchronized long getMaxDBScn()
  {
	  return _maxDBScn;
  }

  @Override
  public synchronized long getNumErrors()
  {
    return _numErrors;
  }

  @Override
  public synchronized long getTimeSinceLastDBAccess()
  {
    return System.currentTimeMillis() - _timestampLastDBAccess;
  }
}
