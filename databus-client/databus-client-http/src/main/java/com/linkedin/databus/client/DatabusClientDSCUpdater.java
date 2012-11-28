package com.linkedin.databus.client;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.DatabusClientGroupMember;

public class DatabusClientDSCUpdater implements Runnable

{
  public static final String MODULE = DatabusClientDSCUpdater.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String DSCKEY = "dscLastWriteTimestamp";

  private enum State {INIT,RUNNING,STOPPED};

  private final DatabusClientGroupMember _groupMember;
  private final LastWriteTimeTrackerImpl _writeTimeTracker;
  private final long _dscUpdateDelayInMs ;
  private boolean _shutdownRequested =false;
  private State _state = State.INIT ;


  public DatabusClientDSCUpdater(DatabusClientGroupMember groupMember,
                                 LastWriteTimeTrackerImpl writeTimeTracker,
                                 long dscUpdateDelayInMs)
  {
    _groupMember=groupMember;
    _writeTimeTracker  = writeTimeTracker;
    _dscUpdateDelayInMs = dscUpdateDelayInMs;
  }

  public void init()
  {
    _state = State.INIT;
    _shutdownRequested = false;
  }

  /** Required to read timestamp **/
  @Override
  public void run()
  {
    try
    {
      if (_writeTimeTracker != null)
      {
        LOG.info("Started! DSC Updater thread started with clientNode=" + _groupMember);
        _state = State.RUNNING;
        while (!_shutdownRequested)
        {
          //read and update _writeTimeTracker
          long lastWriteTimeMillis = getTimestamp();
          if (lastWriteTimeMillis > 0 )
          {
            //thread-safe ; update time tracker locally to enable DSC router to obtain the last write update timestamp of the cluster
            if (lastWriteTimeMillis > _writeTimeTracker.getLastWriteTimeMillis())
            {
              _writeTimeTracker.setLastWriteTimeMillis(lastWriteTimeMillis);
            }
            else
            {
              LOG.info("DSCUpdater: Skipping update of lastWriteEventTimestamp: current= " + _writeTimeTracker.getLastWriteTimeMillis() + " new=" + lastWriteTimeMillis);
            }
          }
          Thread.sleep(_dscUpdateDelayInMs);
        }
      }
    } catch (InterruptedException e)
    {
      LOG.info("DSC Updater thread interrupted!");
    }
    synchronized (this)
    {
      _state = State.STOPPED;
      this.notify();
    }
    LOG.info("DSC Updater thread stopped!");
  }

  /** return 0 on error; otherwise returns timestamp */
  public long getTimestamp()
  {
    if (_groupMember != null)
    {
       Long ts = (Long) _groupMember.readSharedData(DSCKEY);
       if (ts != null)
       {
         return ts.longValue();
       }
       else
       {
         LOG.info("DSCUpdater: error reading " + DSCKEY + " from " + _groupMember);
       }
    }
    else
    {
      return getLocalTimestamp();
    }
    return 0;
  }

  /** return 0 on error; otherwise returns timestamp */
  public long getLocalTimestamp()
  {
    if (_writeTimeTracker != null)
    {
      return _writeTimeTracker.getLastWriteTimeMillis();
    }
    return 0;
  }


  /** Has thread terminated or interrupted? Returns false if thread was never run*/
  public boolean isStopped()
  {
    return _state == State.STOPPED;
  }

  /** Is thread running */
  public boolean isRunning()
  {
    return _state == State.RUNNING;
  }

  /** Asynchronous stop request **/
  public synchronized void stop()
  {
    _shutdownRequested = true;
  }

  /** blocking call
   * return status of thread; **/
  public synchronized boolean awaitShutdown()
  {
    try
    {
      while (_state != State.STOPPED)
      {
        this.wait();
      }
    }
    catch (InterruptedException e)
    {
      LOG.info("DSC Thread interrupted while awaiting shutdown! ");
    }
    return _state == State.STOPPED;
  }

  public void writeLocalTimestamp(long timestampInMillis)
  {
    // write to local memory
    if (_writeTimeTracker != null)
    {
      //threadsafe
      _writeTimeTracker.setLastWriteTimeMillis(timestampInMillis);
    }

  }


  public void writeTimestamp(long timestampInMillis)
  {
    writeLocalTimestamp(timestampInMillis);
    //write to shared location;
    if (_groupMember != null )
    {
      _groupMember.writeSharedData(DSCKEY,timestampInMillis);
    }
  }

}
