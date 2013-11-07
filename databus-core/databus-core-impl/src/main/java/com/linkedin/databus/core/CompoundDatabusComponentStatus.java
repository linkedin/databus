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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

public class CompoundDatabusComponentStatus extends DatabusComponentStatus
{
  private static final long STATUS_EXPIRY_MS = 5;
  private long _lastStatusCheckMs = 0;

  private final Map<String, DatabusComponentStatus> _children =
      new HashMap<String, DatabusComponentStatus>();
  private final Map<String, DatabusComponentStatus> _readOnlyChildren =
      Collections.unmodifiableMap(_children);
  protected final List<String> _initializing = new ArrayList<String>();
  private final List<String> _initializingRO = Collections.unmodifiableList(_initializing);
  protected final List<String> _retryingOnError = new ArrayList<String>();
  private final List<String> _retryingOnErrorRO = Collections.unmodifiableList(_retryingOnError);
  protected final List<String> _suspended = new ArrayList<String>();
  private final List<String> _suspendedRO = Collections.unmodifiableList(_suspended);
  protected final List<String> _paused = new ArrayList<String>();
  private final List<String> _pausedRO = Collections.unmodifiableList(_paused);
  protected final List<String> _shutdown = new ArrayList<String>();
  private final List<String> _shutdownRO = Collections.unmodifiableList(_shutdown);

  public CompoundDatabusComponentStatus(String componentName,
                                        BackoffTimerStaticConfig errorRetriesConf,
                                        Collection<DatabusComponentStatus> children)
  {
    super(componentName, errorRetriesConf);
    addChildren(children);
  }

  public CompoundDatabusComponentStatus(String componentName,
                                        Status status,
                                        String detailedMessage,
                                        BackoffTimer errorRetriesCounter,
                                        Collection<DatabusComponentStatus> children)
  {
    super(componentName, status, detailedMessage, errorRetriesCounter);
    addChildren(children);
  }

  public CompoundDatabusComponentStatus(String componentName,
                                        Status status,
                                        String detailedMessage,
                                        BackoffTimerStaticConfig errorRetriesConf,
                                        Collection<DatabusComponentStatus> children)
  {
    super(componentName, status, detailedMessage, errorRetriesConf);
    addChildren(children);
  }

  public CompoundDatabusComponentStatus(String componentName,
                                        Status status,
                                        String detailedMessage,
                                        Collection<DatabusComponentStatus> children)
  {
    super(componentName, status, detailedMessage);
    addChildren(children);
  }

  public CompoundDatabusComponentStatus(String componentName,
                                        Collection<DatabusComponentStatus> children)
  {
    super(componentName);
    addChildren(children);
  }

  private void processChildStatus(DatabusComponentStatus child)
  {
    switch (child.getStatus())
    {
    case INITIALIZING: _initializing.add(child.getComponentName()); break;
    case RUNNING: break; //NOOP
    case ERROR_RETRY: _retryingOnError.add(child.getComponentName()); break;
    case SUSPENDED_ON_ERROR: _suspended.add(child.getComponentName()); break;
    case PAUSED: _paused.add(child.getComponentName()); break;
    case SHUTDOWN: _shutdown.add(child.getComponentName()); break;
    }
  }

  private void resetStatusLists()
  {
    _initializing.clear();
    _retryingOnError.clear();
    _suspended.clear();
    _paused.clear();
    _shutdown.clear();
  }

  public void addChildren(Collection<DatabusComponentStatus> children)
  {
    if (null == children) return;
    for (DatabusComponentStatus child: children)
    {
      addChild(child);
    }
  }

  public void addChild(DatabusComponentStatus child)
  {
    _children.put(child.getComponentName(), child);
    processChildStatus(child);
  }

  public Map<String, DatabusComponentStatus> getChildren()
  {
    return _readOnlyChildren;
  }

  public List<String> getInitializing()
  {
    return _initializingRO;
  }

  public List<String> getSuspended()
  {
    return _suspendedRO;
  }

  public List<String> getPaused()
  {
    return _pausedRO;
  }

  public List<String> getShutdown()
  {
    return _shutdownRO;
  }

  public List<String> getRetryingOnError()
  {
    return _retryingOnErrorRO;
  }

  @Override
  public Status getStatus()
  {
    refreshStatus();
    return super.getStatus();
  }

  @Override
  public String getMessage()
  {
    refreshStatus();
    return super.getMessage();
  }

  @Override
  public DatabusComponentStatus getStatusSnapshot()
  {
    refreshStatus();
    return super.getStatusSnapshot();
  }

  @Override
  public boolean isRunningStatus()
  {
    refreshStatus();
    return super.isRunningStatus();
  }

  @Override
  public boolean isPausedStatus()
  {
    refreshStatus();
    return super.isPausedStatus();
  }

  @Override
  public synchronized void start()
  {
    for (Map.Entry<String, DatabusComponentStatus> entry: _children.entrySet())
    {
      try
      {
        entry.getValue().start();
      }
      catch (RuntimeException e)
      {
        entry.getValue().suspendOnError(e);
      }
    }
    super.start();
    refreshStatus();
  }

  @Override
  public void shutdown()
  {
    for (Map.Entry<String, DatabusComponentStatus> entry: _children.entrySet())
    {
      try
      {
        entry.getValue().shutdown();
      }
      catch (RuntimeException e)
      {
        entry.getValue().suspendOnError(e);
      }
    }
    super.shutdown();
    refreshStatus();
  }

  @Override
  public void pause()
  {
    for (Map.Entry<String, DatabusComponentStatus> entry: _children.entrySet())
    {
      try
      {
        entry.getValue().pause();
      }
      catch (RuntimeException e)
      {
        entry.getValue().suspendOnError(e);
      }
    }
    super.pause();
    refreshStatus();
  }

  @Override
  public void resume()
  {
    for (Map.Entry<String, DatabusComponentStatus> entry: _children.entrySet())
    {
      try
      {
        entry.getValue().resume();
      }
      catch (RuntimeException e)
      {
        entry.getValue().suspendOnError(e);
      }
    }
    super.resume();
    refreshStatus();
  }

  @Override
  public void suspendOnError(Throwable error)
  {
    super.suspendOnError(error);
    refreshStatus();
  }


  public synchronized void refreshStatus()
  {
    long now = System.currentTimeMillis();
    if ((now - _lastStatusCheckMs) < STATUS_EXPIRY_MS) return; //use cached version
    _lastStatusCheckMs = now;

    resetStatusLists();
    for (Map.Entry<String, DatabusComponentStatus> entry: _children.entrySet())
    {
      processChildStatus(entry.getValue());
    }
  }

}
