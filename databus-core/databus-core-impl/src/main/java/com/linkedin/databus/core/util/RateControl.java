package com.linkedin.databus.core.util;

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

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus2.core.DatabusException;

/**
 * A class that can be used to monitor rate of events in a sequence of intervals
 *
 */
public class RateControl
{
  private RateMonitor _ra = null;
  private long _maxEventsPerSec = Long.MIN_VALUE, _maxthrottleDurationInSecs = Long.MIN_VALUE;
  final private boolean _enabled;
  private boolean _expired = false;
  private long _numSleeps = 0;

  public static final String MODULE = RateControl.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);


  public RateControl(long numEventsPerSec, long throttleDurationInSecs)
  {
    final String name = "internalRateControl";
    _ra = new RateMonitor(name);
    _ra.start();
    _maxEventsPerSec = numEventsPerSec;
    _maxthrottleDurationInSecs = throttleDurationInSecs;
    _numSleeps = 0;
    if ((_maxEventsPerSec > 0) && (_maxthrottleDurationInSecs > 0))
    {
      _enabled = true;
    }
    else
    {
      _enabled = false;
    }
    _expired = false;
  }

  /**
   * To be called when an event is received.
   * Internally keeps track of the rate, and throttles by sleeping appropriately
   * @throws DatabusException
   * @return Number of events received since start
   */
  public long incrementEventCount() throws DatabusException
  {
    // If throttle is not enabled, this is a no-op ( rate metrics need not be maintained )
    if (! isEnabled())
    {
      return Long.MIN_VALUE;
    }

    // Enabled, but has expired, this is a no-op ( rate metrics need not be maintained )
    if ( checkExpired())
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Throttle duration has expired. Accepting events without rate control");
      }

      return Long.MIN_VALUE;
    }

    // Enabled, not expired; add the event and compute if we need to throttle
    _ra.tick();
    if (! checkRateExceeded())
    {
      // This event can be accepted without sleeping. Return number of events received so far
      return _ra.getNumTicks();
    }

    try
    {
      // Compute how long we need to sleep
      long duration = computeSleepDuration();
      sleepToMaintainRate(duration);
    }
    catch (InterruptedException ie){}
    return _ra.getNumTicks();
  }

  public boolean isEnabled()
  {
    return _enabled;
  }

  /**
   * Sleeps for specified time duration ( specified in nanoseconds )
   * 
   * @throws InterruptedException
   * @throws DatabusException
   */
  protected long sleepToMaintainRate(long duration) throws InterruptedException, DatabusException
  {
    if (duration <= 0 )
    {
      throw new DatabusException("Negative duration specified");
    }

    long remainingTimeNSec = duration;
    long remainingTimeMSec = remainingTimeNSec / DbusConstants.NUM_NSECS_IN_MSEC;
    remainingTimeNSec -= (remainingTimeMSec * DbusConstants.NUM_NSECS_IN_MSEC);

    // Make sleep at msec resolution. Err on the side of sleeping for a msec resolution instead of nsecs
    // to keep rate strictly less than specified rate
    if (remainingTimeNSec > 0)
      remainingTimeMSec++;

    boolean needToSleep = (remainingTimeMSec > 0);
    if (needToSleep)
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Sleeping for remainingTimeMSec = " + remainingTimeMSec);        	
      }
      _ra.sleep(remainingTimeMSec);
      _numSleeps++;
    }
    else
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("No need to sleep. remainingTimeMSec = " + remainingTimeMSec);
      }
    }

    return remainingTimeMSec;
  }

  /**
   * Checks if the elapsed time since throttling started has gone over throttleDuration specified
   * This method checks the following
   * - Throttling has been enabled
   * - Time since startup is less than throttleDurationInSecs
   * It does *not* check if the instantaneous rate is higher than the specified rate
   * @return true if throttling is still in effect
   */
  protected boolean checkExpired() throws DatabusException
  {
    if (!_enabled)
    {
      return false;
    }

    if (_expired)
    {
      return true;
    }

    long throttleDurationInNs = _ra.getDuration();
    if (throttleDurationInNs < (_maxthrottleDurationInSecs*DbusConstants.NUM_NSECS_IN_SEC))
    {
      if (_expired != false)
      {
        throw new DatabusException("Throttle duration has not expired. _expired must be set to false");
      }
    }
    else
    {
      LOG.info("Ending throttling of events as " + _maxthrottleDurationInSecs + " have expired");
      _expired = true;
    }
    return _expired;
  }

  /**
   * Computes the instantaneous rate and check if it exceeds the specified rate
   * @return true if incoming (instantaneous) rate is higher than specified rate
   *         false otherwise
   */
  protected boolean checkRateExceeded()
  {
    double rate = _ra.getRate();
    return (rate > _maxEventsPerSec);
  }

  /**
   * Computes the amount of time to sleep to maintain a specified rate
   * @return amount of time to sleep if rate exceeds limit, or zero
   */
  protected long computeSleepDuration() throws DatabusException
  {
    long numEvents = _ra.getNumTicks();
    long numNSecsToHaveExpired = (numEvents * DbusConstants.NUM_NSECS_IN_SEC) / _maxEventsPerSec;
    long numNSecsExpiredSoFar = _ra.getDuration();
    if (numNSecsToHaveExpired > numNSecsExpiredSoFar)
    {
      return (numNSecsToHaveExpired - numNSecsExpiredSoFar);
    }
    else
    {
      return 0;
    }
  }


  /**
   * Statistics on number of sleeps
   * @return Num of sleeps attempted since last reset
   */
  public long getNumSleeps()
  {
    return _numSleeps;
  }

  public void resetNumSleeps()
  {
    _numSleeps = 0;
  }

}
