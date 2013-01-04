/**
 *
 */
package com.linkedin.databus.core.util;

/**
 * Simple extension of java.lang.Thread to keep track if the thread died due to an uncaught
 * exception
 */
public class UncaughtExceptionTrackingThread extends Thread
                                             implements Thread.UncaughtExceptionHandler
{
  public UncaughtExceptionTrackingThread()
  {
    super();
    setUncaughtExceptionHandler(this);
  }

  public UncaughtExceptionTrackingThread(Runnable target, String name)
  {
    super(target, name);
    setUncaughtExceptionHandler(this);
  }

  public UncaughtExceptionTrackingThread(Runnable target)
  {
    super(target);
    setUncaughtExceptionHandler(this);
  }

  public UncaughtExceptionTrackingThread(String name)
  {
    super(name);
    setUncaughtExceptionHandler(this);
  }

  private Throwable _lastException = null;

  @Override
    public void uncaughtException(Thread t, Throwable e) {
        _lastException = e;
    }

  public Throwable getLastException()
  {
    return _lastException;
  }

}
