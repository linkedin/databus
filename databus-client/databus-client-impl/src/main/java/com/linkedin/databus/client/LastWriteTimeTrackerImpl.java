package com.linkedin.databus.client;
import com.linkedin.databus.client.pub.LastWriteTimeTracker;

public class LastWriteTimeTrackerImpl implements LastWriteTimeTracker
{

  private long _lastWriteTimeMillis = 0;
  private long _overallLastWriteTimeMillis = 0;

  public LastWriteTimeTrackerImpl()
  {

  }

  public synchronized void setLastWriteTimeMillis(long lastWriteTimeMillis)
  {
    _lastWriteTimeMillis = lastWriteTimeMillis;
    _overallLastWriteTimeMillis = _lastWriteTimeMillis;

  }

  public synchronized void setOverallLastWriteTimeMillis(long overallLastWriteMillis)
  {
    _overallLastWriteTimeMillis = overallLastWriteMillis;
  }

  @Override
  public synchronized  long getLastWriteTimeMillis()
  {
    return _lastWriteTimeMillis;
  }

  @Override
  public synchronized long getOverallLastWriteTimeMillis()
  {
    return _overallLastWriteTimeMillis;
  }

}
