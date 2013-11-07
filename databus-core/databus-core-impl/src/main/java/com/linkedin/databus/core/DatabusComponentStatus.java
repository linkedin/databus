package com.linkedin.databus.core;
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

import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;


public class DatabusComponentStatus
{
  public enum Status
  {
    INITIALIZING (0),
    RUNNING(100),
    PAUSED(200),
    ERROR_RETRY(300),
    SUSPENDED_ON_ERROR(400),
    SHUTDOWN(1000);

    private final int _code;
    Status(int code)
    {
      _code = code;
    }

    public int getCode()
    {
      return _code;
    }
  }

  public static final String INITIALIZING_MESSAGE = "The databus component is initializing...";
  public static final String RUNNING_MESSAGE = "The databus component is running normally.";
  public static final String PAUSED_MESSAGE = "The databus component is paused by the administrator.";
  public static final String SHUTDOWN_MESSAGE = "The databus component is being shutdown by the administrator...";

  protected final Logger _log;
  private final String _componentName;
  private BackoffTimer _retriesCounter;
  private Status _status;
  private String _message;
  private volatile long _lastStartTime;

  public DatabusComponentStatus(String componentName,
                                Status status, String detailedMessage,
                                BackoffTimer errorRetriesCounter)
  {
    _log = Logger.getLogger(DatabusComponentStatus.class.getName() + "_" + componentName);
    _componentName = componentName;
    _status = status;
    _message = detailedMessage;
    _retriesCounter = errorRetriesCounter;
    _lastStartTime = -1;
  }

  public DatabusComponentStatus(String componentName,
                                Status status, String detailedMessage,
                                BackoffTimerStaticConfig errorRetriesConf)
  {
    this(componentName, status, detailedMessage,
         new BackoffTimer(componentName + ".errorRetries", errorRetriesConf));
  }

  public DatabusComponentStatus(String componentName,Status status, String detailedMessage)
  {
    this(componentName, status, detailedMessage, BackoffTimerStaticConfig.UNLIMITED_RETRIES);
  }

  public DatabusComponentStatus(String componentName)
  {
    this(componentName, Status.INITIALIZING, INITIALIZING_MESSAGE);
  }

  public DatabusComponentStatus(String componentName, BackoffTimerStaticConfig errorRetriesConf)
  {
    this(componentName, Status.INITIALIZING, INITIALIZING_MESSAGE, errorRetriesConf);
  }

  /**
   * @return the status
   */
  public synchronized Status getStatus()
  {
    return _status;
  }

  /**
   * @return the message
   */
  public synchronized String getMessage()
  {
    return _message;
  }

  /**
   * @return status object
   */
  public synchronized DatabusComponentStatus getStatusSnapshot()
  {
    return new DatabusComponentStatus(_componentName, _status, _message, _retriesCounter);
  }

  /**
   * @return boolean
   */
  public synchronized boolean isRunningStatus()
  {
    return (Status.RUNNING == _status);
  }

  /**
   * @return boolean
   */
  public synchronized boolean isPausedStatus()
  {
    return (Status.PAUSED == _status || Status.SUSPENDED_ON_ERROR == _status);
  }

  public synchronized int getRetriesNum()
  {
    return _retriesCounter.getRetriesNum();
  }

  public synchronized int getRetriesLeft()
  {
    return _retriesCounter.getRemainingRetriesNum();
  }

  public synchronized long getLastRetrySleepMs()
  {
    return _retriesCounter.getCurrentSleepMs();
  }

  /**
   * block until the status is set to 'running'
   */
  public synchronized void waitForResume()
  {
    while (isPausedStatus())
    {
      try
      {
        _log.info("Entering into waiting state..." + _status);
        wait();
        _log.info("Existing waiting state with new state..." + _status);
      }
      catch (InterruptedException e)
      {
        _log.info("Interruptd while being suspended!");
      }
    }
  }

  protected synchronized void setStatus(Status status, String statusDetail)
  {
    // Not changing to any other status once we are shutting down
    if (Status.SHUTDOWN != _status)
    {
      Status oldStatus = _status;
      _status = status;
      if (Status.PAUSED == oldStatus ||
          Status.SUSPENDED_ON_ERROR == oldStatus)
      {
        notifyAll();
      }

      String newMessage = statusDetail;
      _lastStartTime = -1;
      switch (_status)
      {
        case RUNNING: _retriesCounter.reset(); _lastStartTime = System.currentTimeMillis(); break;
        case ERROR_RETRY:
        {
          _retriesCounter.backoff();
          if (Status.SUSPENDED_ON_ERROR == oldStatus || Status.ERROR_RETRY == oldStatus)
          {
            newMessage = _message + ";" + statusDetail;
          }
          break;
        }
        case SUSPENDED_ON_ERROR:
        {
          if (Status.SUSPENDED_ON_ERROR == oldStatus || Status.ERROR_RETRY == oldStatus)
          {
            newMessage = _message + ";" + statusDetail;
          }
          break;
        }
        case INITIALIZING: break;//NOOP
        case PAUSED: break;//NOOP
        case SHUTDOWN: break;//impossible
      }
      _message = newMessage;
    }
    _log.debug(status.toString() + ": " + getMessage());
  }

  public void start()
  {
    setStatus(Status.RUNNING, RUNNING_MESSAGE);
  }

  public void shutdown()
  {
    setStatus(Status.SHUTDOWN, SHUTDOWN_MESSAGE);
  }

  public void pause()
  {
    setStatus(Status.PAUSED, PAUSED_MESSAGE);
  }

  public void resume()
  {
    setStatus(Status.RUNNING, RUNNING_MESSAGE);
  }

  public boolean retryOnError(String message)
  {
    setStatus(Status.ERROR_RETRY, message);
    return _retriesCounter.backoffAndSleep();
  }

  public boolean retryOnLastError()
  {
    return _retriesCounter.backoffAndSleep();
  }

  public void suspendOnError(Throwable error)
  {
    setStatus(Status.SUSPENDED_ON_ERROR, null != error ? error.toString() : "unknown error");
  }

  public BackoffTimer getRetriesCounter()
  {
    return _retriesCounter;
  }

  public void setRetriesCounter(BackoffTimer retriesCounter)
  {
	  _retriesCounter = retriesCounter;
  }

  public long getUptimeMs()
  {
    long lastStart = _lastStartTime;
    return (0 < lastStart) ? System.currentTimeMillis() - lastStart : 0;
  }

  public String getComponentName()
  {
    return _componentName;
  }

  @Override
  public String toString() 
  {
    return "DatabusComponentStatus [_componentName=" + _componentName
          + ", _retriesCounter=" + _retriesCounter + ", _status=" + _status
          + ", _message=" + _message + ", _lastStartTime=" + _lastStartTime
          + "]";
  }
}
