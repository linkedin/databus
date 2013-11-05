package com.linkedin.databus.core.util;

import com.linkedin.databus.core.DbusConstants;
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



/**
 * A class that can be used to monitor rate of events in a sequence of intervals
 */
public class RateMonitor
{
  private final String _name;
  private long _startTime;
  private long _numTicks;
  private double _rate;
  private long  _durationSoFar;
  private State _state;

  /*
   * Rate Monitor internal state
   */
  private enum State
  {
    INIT,
    STARTED,
    SUSPENDED,
    RESUMED,
    STOPPED
  }

  public RateMonitor(String name)
  {
    _name = name;
    _startTime = -1;
    _numTicks = 0;
    _state = State.INIT;
  }

  /*
   * @return true if the Monitor has been started
   */
  public boolean started()
  {
    return (_startTime > 0);
  }

  /*
   * Suspends the started/resumed monitor
   */
  public void suspend()
  {
    if ( (_state == State.STARTED ) ||
         (_state == State.RESUMED))
    {
      _durationSoFar += (getNanoTime() - _startTime);
      _startTime = 0;
      _state = State.SUSPENDED;
    }
  }

  /*
   * Resumes the suspended monitor
   *
   */
  public void resume()
  {
    if ( _state == State.SUSPENDED)
    {
      _startTime = getNanoTime();
      _state = State.RESUMED;
    }
  }

  /*
   * Initializes and Starts the Monitor.
   */
  public void start()
  {
    _startTime = getNanoTime();
    _durationSoFar = 0;
    _numTicks = 0;
    _state = State.STARTED;
  }

  /*
   * Increment the monitored event by one
   */
  public void tick()
  {
    assert( ( _state != State.STOPPED ) &&
            (_state != State.SUSPENDED));
    _numTicks ++;
  }

  /*
   * Increment the monitored event by ticks
   *
   * @param ticks : Number of events to be added
   */
  public void ticks(long ticks)
  {
    assert( ( _state != State.STOPPED ) &&
            (_state != State.SUSPENDED));

    _numTicks += ticks;
  }

  /*
   * @return the rate of events discounting the time it was in suspended/stopped state
   *
   */
  public double getRate()
  {
    long duration = getDuration();

    if (_numTicks > 1000000000)
    {
      _rate = 1000000000 * ((double)_numTicks / (double) duration );
    }
    else
    {
      _rate = _numTicks * 1000000000.0 / duration;
    }
    return _rate;
  }

  /*
   * @return the latency/duration discounting the time it was in suspended/stopped state
   */
  public long getDuration()
  {
    long duration = 0;

    switch ( _state)
    {
      case STARTED   :  duration = getNanoTime() - _startTime;
                        break;
      case RESUMED   :  duration = getNanoTime() - _startTime + _durationSoFar;
                        break;

      case STOPPED   :
      case SUSPENDED :  duration = _durationSoFar;
                        break;
      case INIT      :  throw new RuntimeException("RateMonitor not started !!");
    }

    return duration;
  }

  /*
   * Returns the number of ticks
   */
  public long getNumTicks()
  {
    return _numTicks;
  }

  /*
   * Stops the monitor. Equivalent to suspend state except that it cannot be resumed
   */
  public void stop()
  {
    if ( _state != State.SUSPENDED)
      _durationSoFar += (getNanoTime() - _startTime);

    _state = State.STOPPED;
  }

  @Override
  public String toString()
  {
    return "RateMonitor [_name=" + _name + ", _numTicks=" + _numTicks + ", _rate="
        + _rate + ", _durationSoFar=" + _durationSoFar + ", state = " + _state + "]";
  }

  /*
   * @return the current State of the RateMonitor
   */
  public State getState()
  {
	  return _state;
  }

  public boolean isStarted()
  {
    return _state == State.STARTED;
  }

  public long getNanoTime()
  {
    return System.nanoTime();
  }
  
  public void sleep(long msec) throws InterruptedException
  {
    Thread.sleep(msec);	  
  }
  
  /**
   * A rate monitor that mocks time to avoid ambiguity to running unit tests
   */
  public static class MockRateMonitor extends RateMonitor
  {
    private long _currentTimeInNs = 0L;
    public MockRateMonitor(String name)
    {
      super(name);
    }

    @Override
    public long getNanoTime()
    {
      return _currentTimeInNs;
    }

    public void setNanoTime(long ns)
    {
      _currentTimeInNs = ns;
    }
    
    @Override
    public void sleep(long msec) throws InterruptedException
    {
      _currentTimeInNs += (msec * DbusConstants.NUM_NSECS_IN_MSEC);
    }
  }

}
