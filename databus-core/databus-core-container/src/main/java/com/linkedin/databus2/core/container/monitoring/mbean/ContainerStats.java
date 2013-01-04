package com.linkedin.databus2.core.container.monitoring.mbean;

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
  public int getIoThreadRate()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.ioThreadRate;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public int getWorkerThreadRate()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.workerThreadRate;
    }
    finally
    {
      releaseLock(readLock);
    }
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
  public int getIoActiveThreadRate()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.ioActiveThreadRate = null != _ioThreadPool ? _ioThreadPool.getActiveCount() : -1;
      return _event.ioActiveThreadRate;
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
      int taskCount = null != _ioThreadPool ? _ioThreadPool.getQueue().size() : -1;
      _event.ioTaskCount += taskCount;
      _event.ioTaskMax = Math.max(taskCount, _event.ioTaskMax);
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
  public int getWorkerActiveThreadRate()
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      _event.workerActiveThreadRate = _workerThreadPool.getActiveCount();
      return _event.workerActiveThreadRate;
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
      int taskCount = _workerThreadPool.getQueue().size();
      _event.workerTaskCount += taskCount;
      _event.workerTaskMax = Math.max(taskCount, _event.workerTaskMax);
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
    _event.ioActiveThreadRate = 0;
    _event.ioTaskCount = 0;
    _event.ioTaskMax = 0;
    _event.workerThreadRate = 0;
    _event.workerThreadMax = 0;
    _event.workerActiveThreadRate = 0;
    _event.workerTaskCount = 0;
    _event.workerTaskMax = 0;
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
    event.ioActiveThreadRate = _event.ioActiveThreadRate;
    event.ioTaskCount = _event.ioTaskCount;
    event.ioTaskMax = _event.ioTaskMax;
    event.workerThreadRate = _event.workerThreadRate;
    event.workerThreadMax = _event.workerThreadMax;
    event.workerActiveThreadRate = _event.workerActiveThreadRate;
    event.workerTaskCount = _event.workerTaskCount;
    event.workerTaskMax = _event.workerTaskMax;
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
    _event.ioActiveThreadRate = e.ioActiveThreadRate;
    _event.ioTaskCount += e.ioTaskCount;
    _event.ioTaskMax = Math.max(e.ioTaskMax, _event.ioTaskMax);
    _event.workerThreadRate = e.workerThreadRate;
    _event.workerThreadMax = Math.max(e.workerThreadMax, _event.workerThreadMax);
    _event.workerActiveThreadRate = e.workerActiveThreadRate;
    _event.workerTaskCount += e.workerTaskCount;
    _event.workerTaskMax = Math.max(e.workerTaskMax, _event.workerTaskMax);
    _event.errorCount += e.errorCount;
    _event.errorRequestProcessingCount += e.errorRequestProcessingCount;
    _event.errorUncaughtCount += e.errorUncaughtCount;
  }

}
