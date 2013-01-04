/*
 * $Id: EventSourceStatisticsMBean.java 265083 2011-04-28 22:18:04Z snagaraj $
 */
package com.linkedin.databus.monitoring.mbean;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 265083 $
 */
public interface EventSourceStatisticsMBean
{
  /** Returns the name of this source. */
  String getSourceName();
  /** Return the number of consecutive event cycles with at least one event. */
  int getNumConsecutiveCyclesWithEvents();
  /** Return the number of consecutive event cycles with zero events. */
  int getNumConsecutiveCyclesWithoutEvents();
  /** Return the total number of event cycles with at least one event. */
  int getNumCyclesWithEvents();
  /** Return the total number of event cycles with zero events. */
  int getNumCyclesWithoutEvents();
  /** Returns the total number of cycles, both with and without events. */
  int getNumCyclesTotal();
  /** Returns the average number of events per cycle, excluding cycles with zero events. */
  int getAvgNumEventsPerNonEmptyCycle();
  /** Returns the average size (in bytes) of a serialized event */
  long getAvgEventSerializedSize();
  /** Returns the average time (in milliseconds) spent in the event factory, per event. */
  long getAvgEventFactoryTimeMillisPerEvent();
  /** Elapsed time in milliseconds since the last cycle with at least one event. */
  long getMillisSinceLastCycleWithEvents();
  /** Max SCN seen in this source. */
  long getMaxScn();
  /** Return the total number of events processed by this source. */
  int getNumTotalEvents();
  /** Return maximum SCN of source in source database */
  long getMaxDBScn();
  /** Return number of errors/exceptions seen in source database */
  long getNumErrors();
  /** Return time since last DB access in ms */
  long getTimeSinceLastDBAccess();

  /** Reset all statistics to zero. Typically for debugging / testing purposes. */
  void reset();
}
