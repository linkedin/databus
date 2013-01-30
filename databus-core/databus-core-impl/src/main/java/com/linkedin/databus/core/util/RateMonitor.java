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



/**
 * A class that can be used to monitor rate of events in a sequence of intervals
 *
 * @author sdas
 *
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
      _durationSoFar += (System.nanoTime() - _startTime);
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
      _startTime = System.nanoTime();
      _state = State.RESUMED;
    }
  }

  /*
   * Initializes and Starts the Monitor.
   */
  public void start()
  {
    _startTime = System.nanoTime();
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
      case STARTED   :  duration = System.nanoTime() - _startTime;
                        break;
      case RESUMED   :  duration = System.nanoTime() - _startTime + _durationSoFar;
                        break;

      case STOPPED   :
      case SUSPENDED :  duration = _durationSoFar;
                        break;
      case INIT      :  throw new RuntimeException("RateMonitor not started !!");
    }

    return duration;
  }

  /*
   * Stops the monitor. Equivalent to suspend state except that it cannot be resumed
   */
  public void stop()
  {
    if ( _state != State.SUSPENDED)
      _durationSoFar += (System.nanoTime() - _startTime);

    _state = State.STOPPED;
  }

  @Override
  public String toString()
  {
   StringBuilder sb = new StringBuilder();
   sb.append("RateMonitor:")
     .append(_name)
     .append(":avg = ")
     .append(getRate())
     .append(":state = ")
     .append(_state);

   return sb.toString();

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
}
