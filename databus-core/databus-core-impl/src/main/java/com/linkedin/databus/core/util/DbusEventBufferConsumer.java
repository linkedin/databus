package com.linkedin.databus.core.util;

import java.util.HashSet;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;

public class DbusEventBufferConsumer implements Runnable , EventBufferConsumer
{
  public static final Logger LOG = Logger.getLogger(DbusEventBufferConsumer.class.getName());

  public DbusEventBufferConsumer(DbusEventBuffer buffer,int  maxEvents, int deletionInterval, Vector<DbusEvent> out) {
    _buffer = buffer;
    _out = out;
    _seenKeys = new HashSet<Long>();
    _maxEvents = maxEvents;
    _stop = false;
    _deletionInterval = deletionInterval;
    _invalidEvent = false;
    _eventsReadTillInvalidEvent = 0;
}

  @Override
  public void run()
  {
    reset();

    try
    {
      int totalEvents = 0;
      long  allTotalEvents = 0;
      DbusEventIterator iDbusEvent = _buffer.acquireIterator("Test")  ;
      do {
        if (!iDbusEvent.hasNext()) {
          if (!_invalidEvent) {
            if (_deletionInterval>0) {
              iDbusEvent.remove();
            }
            try
            {
              iDbusEvent.await();
            }
            catch (InterruptedException e)
            {
              // TODO Auto-generated catch block
              e.printStackTrace();
              return;
            }
          } else {
            //try and consume as many events as possible
            if (allTotalEvents >= eventsReadTillInvalidEvent()) {
              System.err.printf("Total Events read till invalid event=%d\n",allTotalEvents);
              stop();
            }
          }
        }

        while (iDbusEvent.hasNext()) {
          DbusEvent e = iDbusEvent.next();
          ++allTotalEvents;
          if (!e.isCheckpointMessage() && !e.isControlMessage() && !e.isEndOfPeriodMarker()) {
            //needs to be idempotent; so - ensure that duplicates are dropped;
            if (!_seenKeys.contains(e.key())) {
              //deep copy
              _out.add(e.createCopy());
              _seenKeys.add(e.key());
              ++totalEvents;
            }
          }
          if ((_deletionInterval>0) && allTotalEvents % _deletionInterval==0) {
            iDbusEvent.remove();
          }
        }
      }
      while (totalEvents < _maxEvents && !_stop);
      iDbusEvent.remove();
    }
    catch (RuntimeException e)
    {
      _exceptionThrown = e;
      LOG.error("consumer exception:" + e.getMessage(), e);
    }
    catch (Error e)
    {
      _exceptionThrown = e;
      LOG.error("consumer error:" + e.getMessage(), e);
    }
  }

  /** Run the consumer with timeout */
  public boolean runWithTimeout(long timeoutMs)
  {
    Thread runThread = new Thread(this, "runWithTimeout-" + this);
    runThread.setDaemon(true);
    runThread.start();
    try {
      runThread.join(timeoutMs);
    }
    catch (InterruptedException e){}
    
    final boolean success = !runThread.isAlive();
    if (!success)
    {
    	stop();
    	runThread.interrupt();
    }

    return success;
  }

  @Override
  public void onInvalidEvent(long numEventsRead) {
    _invalidEvent = true;
    _eventsReadTillInvalidEvent  =  numEventsRead;
  }

  public void stop() {
    _stop = true;
  }

  public boolean hasStopped() {
    return _stop;
  }

  public boolean hasInvalidEvent() {
    return _invalidEvent;
  }

  public long eventsReadTillInvalidEvent () {
    return _eventsReadTillInvalidEvent ;
  }

  public void reset() {
      _stop = false;
      _invalidEvent = false;
      _eventsReadTillInvalidEvent = 0;
      _exceptionThrown = null;
  }

  private final DbusEventBuffer _buffer;
  private final Vector<DbusEvent> _out;
  private final HashSet<Long> _seenKeys;
  private final int _maxEvents;
  private boolean _stop;
  private final int _deletionInterval;
  private boolean _invalidEvent;
  private long _eventsReadTillInvalidEvent;
  private volatile Throwable _exceptionThrown;

  public Throwable getExceptionThrown()
  {
    return _exceptionThrown;
  }
}
