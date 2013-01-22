/**
 *
 */
package com.linkedin.databus.core.util;

import org.apache.log4j.Logger;

/**
 * Simple extension of java.lang.Thread to keep track if the thread died due to an uncaught
 * exception
 */
public class UncaughtExceptionTrackingThread extends Thread
                                             implements Thread.UncaughtExceptionHandler
{
  public static final Logger LOG = Logger.getLogger(UncaughtExceptionTrackingThread.class);

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
        LOG.error("uncaught exception in thread " + getName() + ": " + e.getMessage(), e);
  }

  public Throwable getLastException()
  {
    return _lastException;
  }

}
