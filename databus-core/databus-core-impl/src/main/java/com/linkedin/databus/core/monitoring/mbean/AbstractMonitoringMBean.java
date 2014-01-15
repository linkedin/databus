package com.linkedin.databus.core.monitoring.mbean;
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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.JmxUtil;
import com.linkedin.databus.core.util.ReadWriteSyncedObject;

public abstract class AbstractMonitoringMBean<T> extends ReadWriteSyncedObject
                                                 implements DatabusMonitoringMBean<T>
{
  public static final String MODULE = AbstractMonitoringMBean.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String JMX_DOMAIN = "com.linkedin.databus2";
  private static final Pattern BAD_CHARS_PATTERN = Pattern.compile("[:,?=]");

  // we use -1 in both cases(instead of real MAX and MIN) because we want to
  // avoid spikes on the graphs.
  public static final long DEFAULT_MAX_LONG_VALUE = -1;
  public static final long DEFAULT_MIN_LONG_VALUE = -1;

  protected final AtomicBoolean _enabled;
  protected T _event;
  protected final SpecificDatumWriter<T> _avroWriter;

  protected AbstractMonitoringMBean(boolean enabled, boolean threadSafe, T initEvent)
  {
    super(threadSafe);
    _enabled = new AtomicBoolean(enabled);
    _event = null == initEvent ? newDataEvent() : initEvent;
    _avroWriter = getAvroWriter();
  }

  @Override
  public boolean isEnabled()
  {
    return _enabled.get();
  }

  @Override
  public void reset()
  {
    Lock writeLock = acquireWriteLock();

    try
    {
      resetData();
    }
    finally
    {
      releaseLock(writeLock);
    }
}

  @Override
  public void setEnabled(boolean enabled)
  {
    _enabled.set(enabled);
  }

  @Override
  public T getStatistics(T reuse)
  {
    if (null == reuse) reuse = newDataEvent();

    Lock readLock = acquireReadLock();

    try
    {
      cloneData(reuse);
    }
    finally
    {
      releaseLock(readLock);
    }

    return reuse;
  }

  @Override
  public String toJson()
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream(10000);

    try
    {
      JsonEncoder jsonEncoder = createJsonEncoder(out);
      toJson(jsonEncoder, null);
    }
    catch (IOException ioe)
    {
      LOG.error("JSON serialization error", ioe);
      out.reset();
    }

    return out.toString();
  }

  @Override
  public T toJson(JsonEncoder jsonEncoder, T eventReuse) throws IOException
  {
    eventReuse = getStatistics(eventReuse);
    _avroWriter.write(eventReuse, jsonEncoder);
    jsonEncoder.flush();

    return eventReuse;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void mergeStats(DatabusMonitoringMBean<T> other)
  {
    if (! (other instanceof AbstractMonitoringMBean<?>)) return;
    AbstractMonitoringMBean<Object> otherObj = (AbstractMonitoringMBean<Object>)other;
    if (! _event.getClass().isInstance(otherObj._event)) return;
    Object otherEvent = otherObj._event;

    Lock otherReadLock = otherObj.acquireReadLock();
    Lock thisWriteLock = null;
    try
    {
      thisWriteLock = acquireWriteLock(otherReadLock);
      doMergeStats(otherEvent);
    }
    finally
    {
      releaseLock(thisWriteLock);
      releaseLock(otherReadLock);
    }
  }

  @Override
  public void mergeStats(T otherEvent)
  {
    Lock writeLock = acquireWriteLock();
    try
    {
      doMergeStats(otherEvent);
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public boolean registerAsMbean(MBeanServer mbeanServer)
  {
    boolean success = false;
    if (null != mbeanServer)
    {
      try
      {
        ObjectName objName = generateObjectName();

        if (mbeanServer.isRegistered(objName))
        {
          if (LOG.isDebugEnabled())
          {
            LOG.debug("unregistering old MBean: " + objName);
          }
          mbeanServer.unregisterMBean(objName);
        }

        mbeanServer.registerMBean(this, objName);
        if (LOG.isDebugEnabled())
        {
          LOG.debug("MBean registered " + objName);
        }
        success = true;
      }
      catch (Exception e)
      {
        LOG.error("Unable to register to mbean server", e);
      }
    }

    return success;
  }

  public boolean unregisterMbean(MBeanServer mbeanServer)
  {
    boolean success = false;
    if (null != mbeanServer)
    {
      try
      {
        ObjectName objName = generateObjectName();
        if (!isThreadSafe())
        {
          // For e.g., ConsumerCallbackStats uses thread unsafe mode by design
          if (LOG.isDebugEnabled())
          {
            LOG.debug("The mbean " + objName.toString() + " is not thread-safe which is probably not a good idea.");
          }
        }
        JmxUtil.unregisterMBeanSafely(mbeanServer, objName, LOG);
        if (LOG.isDebugEnabled())
        {
          LOG.debug("MBean unregistered " + objName);
        }
        success = true;
      }
      catch (Exception e)
      {
        LOG.error("Unable to register to mbean server", e);
      }
    }

    return success;
  }

  public static String sanitizeString(String s)
  {
    return BAD_CHARS_PATTERN.matcher(s).replaceAll("_");
  }

  public abstract ObjectName generateObjectName() throws MalformedObjectNameException;

  /** Resets the actual event data; no need to be synchronized */
  protected abstract void resetData();

  /** Clones the actual event data; no need to be synchronized */
  protected abstract void cloneData(T eventObj);

  /** Creates ant empty event data object */
  protected abstract T newDataEvent();

  /** Constructs an avro writer object */
  protected abstract SpecificDatumWriter<T> getAvroWriter();

  /** Merges the stats from another event; no need to be synchronized */
  protected abstract void doMergeStats(Object eventData);

  /**
   * Creates a hash table with the base mbean properties to be shared across all mbeans
   * @return the hash table
   */
  protected Hashtable<String, String> generateBaseMBeanProps()
  {
    Hashtable<String, String> mbeanProps = new Hashtable<String, String>(5);
    mbeanProps.put("type", this.getClass().getSimpleName());

    return mbeanProps;
  }

  /**
   * @param val1
   * @param val2
   * @return max of two (takes into account default value)
   */
  protected long maxValue(long val1, long val2) {
    if(val1 == DEFAULT_MAX_LONG_VALUE)
      return val2;
    if(val2 == DEFAULT_MAX_LONG_VALUE)
      return val1;

    return Math.max(val1, val2);
  }

  /**
   *
   * @param val1
   * @param val2
   * @return min of two (takes into account default value)
   */
  protected long minValue(long val1, long val2) {
    if(val1 == DEFAULT_MIN_LONG_VALUE)
      return val2;
    if(val2 == DEFAULT_MIN_LONG_VALUE)
      return val1;

    return Math.min(val1, val2);
  }

  @Override
  public String toString()
  {
    return "AbstractMonitoringMBean [_event=" + _event + "]";
  }
}
