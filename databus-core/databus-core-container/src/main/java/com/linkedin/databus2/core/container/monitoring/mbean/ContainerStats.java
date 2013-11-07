package com.linkedin.databus2.core.container.monitoring.mbean;
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


import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus2.core.container.monitoring.events.ContainerStatsEvent;

/*
 * Some of the seemingly odd method-naming in this class (and related classes)
 * is due to heuristics elsewhere that sniff for "count" or "num" and use that
 * to distinguish between RRD's "counter" and "gauge" types (i.e., aggregated
 * vs. instantaneous values).  In particular, where "rate" might normally imply
 * some sort of time-based value, here it's used as a label for instantaneous
 * (non-aggregated) count values of threads.
 *
 * TODO:  Change aggregation variable/method names and heuristics to use
 * "counter" instead?  Maybe change instantaneous values to use "gauge", too?
 */
public class ContainerStats extends AbstractMonitoringMBean<ContainerStatsEvent>
                            implements ContainerStatsMBean
{

  private final ThreadPoolExecutor _ioThreadPool;
  private final ThreadPoolExecutor _workerThreadPool;

  public ContainerStats(int containerId, boolean enabled, boolean threadSafe,
                        ContainerStatsEvent initData, ThreadPoolExecutor ioThreadPool,
                        ThreadPoolExecutor workerThreadPool)
  {
    super(enabled, threadSafe, initData);
    _event.containerId = containerId;
    _ioThreadPool = ioThreadPool;
    _workerThreadPool = workerThreadPool;
    reset();
  }

  @Override
  public long getErrorCount()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.errorCount;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getErrorUncaughtCount()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.errorUncaughtCount;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getErrorRequestProcessingCount()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.errorRequestProcessingCount;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimestampLastResetMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.timestampLastResetMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getTimeSinceLastResetMs()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return System.currentTimeMillis() - _event.timestampLastResetMs;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public int getIoThreadMax()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.ioThreadMax;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public int getIoThreadRate()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.ioThreadRate = null != _ioThreadPool ? _ioThreadPool.getActiveCount() : -1;
      _event.ioThreadMax = Math.max(_event.ioThreadRate, _event.ioThreadMax);
      return _event.ioThreadRate;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public long getIoTaskCount()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      if (null != _ioThreadPool)
      {
        long taskCount = _ioThreadPool.getTaskCount();
        long taskDelta = taskCount - _event.ioTaskCount;
        // Don't update max on the very first call; it will be artificially high
        // if there was a long delay between startup and the first call.
        if (_event.ioTaskCount > 0)
        {
          _event.ioTaskMax = (int)Math.max(taskDelta, _event.ioTaskMax);
        }
        _event.ioTaskCount = taskCount;
      }
      return _event.ioTaskCount;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public int getIoTaskMax()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.ioTaskMax;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  // We expect this metric to be zero virtually all the time since the time tasks
  // wait in the queue is vanishingly small in comparison to the update (call)
  // rate of the metric.  (Or so we believe, anyway.  To add timing instrumentation,
  // one would need to subclass BlockingQueue and track the entry and exit of each
  // item in the queue.)
  @Override
  public int getIoTaskQueueSize()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      if (null != _ioThreadPool)
      {
        _event.ioTaskQueueSize = _ioThreadPool.getQueue().size();
      }
      return _event.ioTaskQueueSize;  // FIXME:  prefer to return -1 if no _ioThreadPool?
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public int getWorkerThreadMax()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.workerThreadMax;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public int getWorkerThreadRate()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.workerThreadRate = _workerThreadPool.getActiveCount();
      _event.workerThreadMax = Math.max(_event.workerThreadRate, _event.workerThreadMax);
      return _event.workerThreadRate;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public long getWorkerTaskCount()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      long taskCount = _workerThreadPool.getTaskCount();
      long taskDelta = taskCount - _event.workerTaskCount;
      // Don't update max on the very first call; it will be artificially high
      // if there was a long delay between startup and the first call.
      if (_event.workerTaskCount > 0)
      {
        _event.workerTaskMax = (int)Math.max(taskDelta, _event.workerTaskMax);
      }
      _event.workerTaskCount = taskCount;
      return _event.workerTaskCount;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public int getWorkerTaskMax()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.workerTaskMax;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  // We expect this metric to be zero virtually all the time since the time tasks
  // wait in the queue is vanishingly small in comparison to the update (call)
  // rate of the metric.  (Or so we believe, anyway.  To add timing instrumentation,
  // one would need to subclass BlockingQueue and track the entry and exit of each
  // item in the queue.)
  @Override
  public int getWorkerTaskQueueSize()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.workerTaskQueueSize = _workerThreadPool.getQueue().size();
      return _event.workerTaskQueueSize;
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerError(Throwable error)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      ++_event.errorCount;
      //TODO Move the RequestProcessingException class to databus-container/core and use instanceof (DDSDBUS-104)
      if (error.getClass().getSimpleName().equals("RequestProcessingException"))
      {
        ++_event.errorRequestProcessingCount;
      }
      else
      {
        ++_event.errorUncaughtCount;
      }
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public JsonEncoder createJsonEncoder(OutputStream out) throws IOException
  {
    return new JsonEncoder(_event.getSchema(), out);
  }

  @Override
  public ObjectName generateObjectName() throws MalformedObjectNameException
  {
    Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
    mbeanProps.put("containerId", Integer.toString(_event.containerId));

    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
  }

  @Override
  protected void resetData()
  {
    _event.timestampLastResetMs = System.currentTimeMillis();
    _event.ioThreadRate = 0;
    _event.ioThreadMax = 0;
    _event.ioTaskCount = 0;
    _event.ioTaskMax = 0;
    _event.ioTaskQueueSize = 0;
    _event.workerThreadRate = 0;
    _event.workerThreadMax = 0;
    _event.workerTaskCount = 0;
    _event.workerTaskMax = 0;
    _event.workerTaskQueueSize = 0;
    _event.errorCount = 0;
    _event.errorRequestProcessingCount = 0;
    _event.errorUncaughtCount = 0;
  }

  @Override
  protected void cloneData(ContainerStatsEvent event)
  {
    event.containerId = _event.containerId;
    event.timestampLastResetMs = _event.timestampLastResetMs;
    event.ioThreadRate = _event.ioThreadRate;
    event.ioThreadMax = _event.ioThreadMax;
    event.ioTaskCount = _event.ioTaskCount;
    event.ioTaskMax = _event.ioTaskMax;
    event.ioTaskQueueSize = _event.ioTaskQueueSize;
    event.workerThreadRate = _event.workerThreadRate;
    event.workerThreadMax = _event.workerThreadMax;
    event.workerTaskCount = _event.workerTaskCount;
    event.workerTaskMax = _event.workerTaskMax;
    event.workerTaskQueueSize = _event.workerTaskQueueSize;
    event.errorCount = _event.errorCount;
    event.errorRequestProcessingCount = _event.errorRequestProcessingCount;
    event.errorUncaughtCount = _event.errorUncaughtCount;
  }

  @Override
  protected ContainerStatsEvent newDataEvent()
  {
    return new ContainerStatsEvent();
  }

  @Override
  protected SpecificDatumWriter<ContainerStatsEvent> getAvroWriter()
  {
    return new SpecificDatumWriter<ContainerStatsEvent>(ContainerStatsEvent.class);
  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    if (! (eventData instanceof ContainerStatsEvent))
    {
      LOG.warn("Attempt to merge unknown event class: " + eventData.getClass().getName());
      return;
    }
    ContainerStatsEvent e = (ContainerStatsEvent)eventData;

    /** Allow use negative relay IDs for aggregation across multiple relays */
    if (_event.containerId > 0 && e.containerId != _event.containerId)
    {
      LOG.warn("Attempt to data for a different relay " + e.containerId);
      return;
    }

    _event.ioThreadRate = e.ioThreadRate;
    _event.ioThreadMax = Math.max(e.ioThreadMax, _event.ioThreadMax);
    _event.ioTaskCount += e.ioTaskCount;
    _event.ioTaskMax = Math.max(e.ioTaskMax, _event.ioTaskMax);
    _event.ioTaskQueueSize = e.ioTaskQueueSize;
    _event.workerThreadRate = e.workerThreadRate;
    _event.workerThreadMax = Math.max(e.workerThreadMax, _event.workerThreadMax);
    _event.workerTaskCount += e.workerTaskCount;
    _event.workerTaskMax = Math.max(e.workerTaskMax, _event.workerTaskMax);
    _event.workerTaskQueueSize = e.workerTaskQueueSize;
    _event.errorCount += e.errorCount;
    _event.errorRequestProcessingCount += e.errorRequestProcessingCount;
    _event.errorUncaughtCount += e.errorUncaughtCount;
  }

}
