/*
 * $Id: OracleEventProducer.java 272015 2011-05-21 03:03:57Z cbotev $
 */
package com.linkedin.databus2.producers;
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


import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.mbean.DatabusReadOnlyStatus;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.db.MonitoredSourceInfo;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * Common implementation for producing events and adding those to a event buffer
 */
public abstract class AbstractEventProducer implements EventProducer
{
  //private final SourceDBEventReader _sourceDBEventReader;

  private final String _name;

  // The event generation thread and various related member variables.
  private EventProducerThread _thread;
  private boolean _pauseRequested = false;
  private boolean _shutdownRequested = false;
  private final AtomicLong _sinceSCN = new AtomicLong(-1);
  private final DbusEventBufferAppendable _eventBuffer;
  private final MaxSCNReaderWriter _maxScnReaderWriter;
  private final DatabusComponentStatus _status;
  private final DatabusReadOnlyStatus _statusMBean;
  protected final MBeanServer _mbeanServer;
  private long _restartScnOffset;
  private boolean _coldStart= true;


  // The all important log
  protected final Logger _log;
  protected final Logger _eventsLog;

  public AbstractEventProducer(
    DbusEventBufferAppendable eventBuffer,
    boolean enableTracing,
    DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
    MaxSCNReaderWriter maxScnReaderWriter,
    PhysicalSourceStaticConfig physicalSourceConfig,
    MBeanServer mbeanServer
    )
  {
    this(eventBuffer, maxScnReaderWriter, physicalSourceConfig, mbeanServer);
    _restartScnOffset = physicalSourceConfig.getRestartScnOffset();
    _coldStart =true;
  }

  public AbstractEventProducer(DbusEventBufferAppendable eventBuffer,
                             MaxSCNReaderWriter maxScnReaderWriter,
                             PhysicalSourceStaticConfig physicalSourceConfig,
                             MBeanServer mbeanServer)
  {
    _eventBuffer = eventBuffer;
    _name = physicalSourceConfig.getName();
    _maxScnReaderWriter = maxScnReaderWriter;
    _restartScnOffset = 0;
    _coldStart=true;
    _status = new DatabusComponentStatus(_name + ".dbPuller", physicalSourceConfig.getRetries());
    _mbeanServer = mbeanServer;
    _statusMBean = new DatabusReadOnlyStatus(_name, _status, -1);
    _statusMBean.registerAsMbean(_mbeanServer);
    _log = Logger.getLogger(getClass().getName() + "_" + _name);
    _eventsLog = Logger.getLogger("com.linkedin.databus2.producers.db.events." + _name);
  }

  public Logger getEventsLog()
  {
    return _eventsLog;
  }

  @Override
  public String getName()
  {
    return _name;
  }

  @Override
  public long getSCN()
  {
    return _sinceSCN.get();
  }

  @Override
  public synchronized void start(long sinceSCN)
  {

    _log.info("OracleEventProducer.start: sinceScn = " + sinceSCN + "restartScnOffset = " +
              _restartScnOffset);

    if (sinceSCN < 0 && null != _maxScnReaderWriter)
    {
      try
      {
        _log.info("attempting to read persistent MaxSCN ");
        sinceSCN = _maxScnReaderWriter.getMaxScn();
        if ((sinceSCN > 1) && _coldStart) {
          if (sinceSCN > _restartScnOffset) {
            _log.info("sinceSCN read from SCNRW = " + sinceSCN + " restartScnOffset = " + _restartScnOffset );
            sinceSCN -= _restartScnOffset;
          }
        }
        _log.info("Proposed sinceSCN = " + sinceSCN);
      }
      catch (Exception e)
      {
        _log.error("Unable to load MaxSCN: " + e.getMessage(), e);
      }
    }

    if(_thread == null)
    {
      // Thread is not already running. Go ahead and start a new one.
      _sinceSCN.set(sinceSCN);
      _log.info("Starting EventProducerThread from SCN: " + _sinceSCN.get());
      _pauseRequested = false;
      _shutdownRequested = false;

      _sinceSCN.set(sinceSCN);

      // init the buffer with the start SCN
      //only valid for first restart
      if (_coldStart) {
        _eventBuffer.start(sinceSCN);
      }

      _thread = new EventProducerThread(_name);
      _thread.setDaemon(true);
      _thread.start();
    }
    else
    {
      // We can't change the SCN if the Thread is already running, so really this should maybe raise an exception
      // TODO: Should this throw an IllegalThreadStateException?
      _pauseRequested = false;
      _shutdownRequested = false;
      notifyAll();
    }

    _log.info("" + _name + " started.");
  }

  @Override
  public synchronized boolean isPaused()
  {
    return _pauseRequested;
  }

  @Override
  public synchronized boolean isRunning()
  {
    return _thread != null && !_shutdownRequested && !_pauseRequested;
  }

  @Override
  public synchronized void unpause()
  {
    _pauseRequested = false;
    notifyAll();
    if(_thread == null)
    {
      _log.warn("Unpause requested when no event thread is running.");
    }
  }

  @Override
  public synchronized void pause()
  {
    _pauseRequested = true;
    notifyAll();
  }

  @Override
  public synchronized void shutdown()
  {
    _shutdownRequested = true;
    notifyAll();
    // Interrupt the thread so it can shut down, in case it is waiting on the sleep timer
    if(_thread != null)
    {
      _coldStart=false;
      _thread.interrupt();
    }
    _statusMBean.unregisterMbean(_mbeanServer);
  }

  @Override
  public void waitForShutdown() throws InterruptedException,
                                       IllegalStateException
  {
    while (null != _thread && _thread.isAlive()) _thread.join();

  }

  @Override
  public void waitForShutdown(long timeoutMs) throws InterruptedException,
                                                IllegalStateException
  {
    if (null != _thread && _thread.isAlive()) _thread.join(timeoutMs);
    if (null != _thread && _thread.isAlive()) throw new IllegalStateException();
  }

  private class EventProducerThread
  extends Thread
  {
    public EventProducerThread(String producerName)
    {
      super("EventProducerThread_" + producerName);
    }

    @Override
    public void run()
    {
      boolean firstPause = true;
      while(true)
      {
        synchronized(AbstractEventProducer.this)
        {
          while(_pauseRequested && !_shutdownRequested)
          {
            // If we are paused, then log a message to that effect.
            // Guarded by "firstPause" so we only log the message once per pause/unpause cycle.
            if(firstPause)
            {
              firstPause = false;
              _log.info("EventProducerThread is pausing because a pause was requested.");
            }
            try
            {
              AbstractEventProducer.this.wait();
            }
            catch(InterruptedException ex)
            {
              //ignore?
            }
          }
          if(_shutdownRequested)
          {
            // A shutdown has been requested. Log a message and end the thread.
            _log.info("EventProducerThread is stopping because a shutdown was requested.");
            _thread = null;
            _eventBuffer.rollbackEvents();
            return;
          }
        }

        try
        {
          // Read events from all sources
          ReadEventCycleSummary summary = readEventsFromAllSources(_sinceSCN.get());

          // Find the new max SCN across all sources and update _sinceSCN
          long newSinceSCN = Math.max(summary.getEndOfWindowScn(), _sinceSCN.get());
          _sinceSCN.set(newSinceSCN);

          // Friendly log messages
          if (_eventsLog.isDebugEnabled() || (_eventsLog.isInfoEnabled() && summary.getTotalEventNum() >0))
          {
            _eventsLog.info(summary.toString());
          }

          if (_status.getRetriesNum() > 0) _status.resume();
          _status.getRetriesCounter().reset();
          // Sleep until the next cycle
          _status.getRetriesCounter().sleep();
        }
        catch(EventCreationException ex)
        {
          // Log the error and backoff the sleep timer
          _log.error("EventCreationException occurred while reading events from " + _name +
                     ". This error is most likely configuration or data dependent and may require user intervention.", ex);
          _status.retryOnError(_name + " error: " + ex.getMessage());
        }
        catch(UnsupportedKeyException ex)
        {
          // Log the error and backoff the sleep timer
          _log.error("UnsupportedKeyException occurred while reading events from " + _name +
                     ". This error is most likely configuration or data dependent and may require user intervention.", ex);
          _status.retryOnError(_name + " error: " + ex.getMessage());
        }
        catch(DatabusException ex)
        {
          // Log the error and backoff the sleep timer
          _log.error("DatabusException occurred while reading events from " +  _name +
                     ". This error may be due to a transient issue (database is down?):" +
                     ex.getMessage(),
                     ex);
          _status.retryOnError(_name + " error: " + ex.getMessage());
        }
        catch(Exception e)
        {
          // Log the error and backoff the sleep timer
          _log.error("DatabusException occurred while reading events from " +  _name +
                     ". This error may be due to a transient issue (database is down?):" +
                     e.getMessage(),
                     e);
          _status.retryOnError(_name + " error: " + e.getMessage());
        }

        // Reset this flag since we are no longer paused
        firstPause = false;
      }
    }
  }

  public DatabusReadOnlyStatus getStatusMBean()
  {
    return _statusMBean;
  }

  protected abstract ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
            throws DatabusException, EventCreationException, UnsupportedKeyException;

  protected abstract List<MonitoredSourceInfo> getSources();

}
