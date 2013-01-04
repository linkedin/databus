package com.linkedin.databus2.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/** Implements a timer for linearly or exponentially increasing sleeps */
public class BackoffTimer
{
  public static final String MODULE = BackoffTimerStaticConfigBuilder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final long NO_MORE_RETRIES = -1L;
  
  private final BackoffTimerStaticConfig _config;
  private final String _name;
  private long _retrySleepMs;
  private int _retriesNum;
  private long _retryStartTs;

  public BackoffTimer(String name, BackoffTimerStaticConfig config)
  {
    _config = config;
    _name = name;
    reset();
  }

  /** Returns the counter configuration */
  public BackoffTimerStaticConfig getConfig()
  {
    return _config;
  }

  /** The current sleep duration; -1 after a reset/success */
  public long getCurrentSleepMs()
  {
    return _retrySleepMs;
  }

  /** The current number of error retries */
  public int getRetriesNum()
  {
    return _retriesNum;
  }

  public int getRemainingRetriesNum()
  {
    return _config.getMaxRetryNum() >= 0 ? _config.getMaxRetryNum() - _retriesNum : Integer.MAX_VALUE;
  }

  public long getTotalRetryTime()
  {
    return _retryStartTs > 0 ? System.currentTimeMillis() - _retryStartTs : 0;
  }

  /** Reset the counter; e.g. after a success */
  public void reset()
  {
    _retrySleepMs = _config.getInitSleep();
    _retriesNum = 0;
    _retryStartTs = -1;
  }

  /**
   * Increments retries number and sleep
   * @return the new sleep or NO_MORE_RETRIES if no more retries are allowed
   * */
  public long backoff()
  {
    if (0 == _retriesNum) _retryStartTs = System.currentTimeMillis();
    ++ _retriesNum;
    _retrySleepMs = _config.calcNextSleep(_retrySleepMs);
    if (_config.getMaxRetryNum() >= 0 && _retriesNum > _config.getMaxRetryNum()) return NO_MORE_RETRIES;

    return _retrySleepMs;
  }

  /**
   * Sleep for the current sleep time, including any backoff that has occurred.
   * @return true if we slept for the total time; false if we were interrupted
   */
  public boolean sleep()
  {
    if (0L >= _retrySleepMs) return true;
    boolean debugLogEnabled  = LOG.isDebugEnabled();

    try
    {
      if (_retriesNum > 0) LOG.info(_name + ": error sleep, ms: " + _retrySleepMs);
      else if(debugLogEnabled) LOG.debug(_name + ": sleeping for " + _retrySleepMs);
      Thread.sleep(_retrySleepMs);
      return true;
    }
    catch(InterruptedException ex)
    {
      LOG.info(_name + ": sleep interrupted");
      return false;
    }
  }

  /**
   * Sleep for the current sleep time, including any backoff that has occurred.
   * @return true if we slept for the total time; false if we were interrupted
   */
  public boolean backoffAndSleep()
  {
    backoff();
    return sleep();
  }


  @Override
  public String toString() {
	return "BackoffTimer [_config=" + _config + ", _name=" + _name
			+ ", _retrySleepMs=" + _retrySleepMs + ", _retriesNum="
			+ _retriesNum + ", _retryStartTs=" + _retryStartTs + "]";
  }  
}
