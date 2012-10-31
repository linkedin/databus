package com.linkedin.databus.bootstrap.utils;


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
