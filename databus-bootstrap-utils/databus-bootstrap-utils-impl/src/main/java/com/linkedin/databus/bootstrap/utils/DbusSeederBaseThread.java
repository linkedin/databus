package com.linkedin.databus.bootstrap.utils;
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



import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.RateMonitor;


/*
 * Base Thread which implements interface for a stoppable
 * thread
 */
public class DbusSeederBaseThread 
    extends Thread
{

  private static final Logger LOG = Logger.getLogger(DbusSeederBaseThread.class);

  protected final    AtomicBoolean _stop = new AtomicBoolean(false);
  protected final    CountDownLatch _stopped;
  protected final    RateMonitor    _rate;
  protected volatile Exception      _exception;
  protected volatile boolean        _isError;
  protected volatile boolean        _started;
  
  public DbusSeederBaseThread(String name)
  {
    super(name);
    _stopped = new CountDownLatch(1);
    _rate = new RateMonitor(name);
    _isError = false;
  }
  
  public boolean done()
  {
	  return _started && !this.isAlive();
  }
  
  public void stop(boolean waitTillStop)
  {
    _stop.set(true);
    
    this.interrupt();

    while (waitTillStop)
    {
      try
      {
        _stopped.await();
        return;
      } catch (InterruptedException  ie) {
    	  LOG.error("Got interrupted while waiting for SeederBase thread to stop. Thread Name is :" + getName(), ie);
      }
    }
  }
  
  public boolean isError()
  {
	  return _isError;
  }
  
  public Exception getException()
  {
	  return _exception;
  }
  
  public double getRate()
  {
    return _rate.getRate();
  }
  
}
