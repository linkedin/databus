package com.linkedin.databus.client.consumer;

import java.util.concurrent.Callable;

/** A callable that keeps track of scheduling and run time of tasks */
public abstract class ConsumerCallable<C> implements Callable<C>
{
  private final long _creationTime;
  private volatile long _startTime;
  private volatile long _endTime;

  
  protected ConsumerCallable(long currentNanos)
  {
    _creationTime = -1 == currentNanos ? System.nanoTime() : currentNanos;
    _startTime = -1;
    _endTime = -1;
  }

  /** Checks if the task been started. It will return true even if the task has already completed. */
  public boolean isStarted()
  {
    return -1 != _startTime;
  }

  /** Checks if the task has already completed. */
  public boolean isDone()
  {
    return -1 != _endTime;
  }

  /**
   * Returns time from creation of the callable till its start. If the callable has not been started yet,
   * returns the time since creation till the current moment. */
  public long getNanoTimeInQueue()
  {
    return -1 != _startTime ? _startTime - _creationTime : System.nanoTime() - _startTime;
  }

  /**
   * Returns how much time it took to run the callable. If the callable has not been started,
   * the method returns 0. If the callable has been started but it has not completed yet, the method
   * returns the current running time.
   **/
  public long getNanoRunTime()
  {
    return -1 == _startTime ? 0 : (-1 == _endTime ? System.nanoTime() - _startTime
                                                  : _endTime - _startTime);
  }

  /** Callable creation timestamp in nanoseconds */
  public long getCreationTime()
  {
    return _creationTime;
  }

  /** Call start timestamp in nanoseconds */
  public long getStartTime()
  {
    return _startTime;
  }

  /** Call end timestamp in nanoseconds */
  public long getEndTime()
  {
    return _endTime;
  }

  @Override
  public C call() throws Exception
  {
    _startTime = System.nanoTime();
    try
    {
      return doCall();
    }
    finally
    {
      _endTime = System.nanoTime();
    }
  }

  /** Called when the callable object has finished executing 
   * This is called serially for each callable object */
  final public void endCall(C result)
  {
	 
	 doEndCall(result);
	  
  }
  
  protected abstract C doCall() throws Exception;

  /** Callback when the call is done and has been removed from execution 
   * This is called serially for each callable object */
  protected void doEndCall(C result) 
  {
	  //no-op
  }

}
