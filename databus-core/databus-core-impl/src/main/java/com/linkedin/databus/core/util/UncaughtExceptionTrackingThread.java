/**
 *
 */
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
