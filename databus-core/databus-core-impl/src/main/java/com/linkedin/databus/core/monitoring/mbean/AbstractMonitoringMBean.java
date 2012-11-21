package com.linkedin.databus.core.monitoring.mbean;

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
          LOG.warn("unregistering old MBean: " + objName);
          mbeanServer.unregisterMBean(objName);
        }

        mbeanServer.registerMBean(this, objName);
        LOG.info("MBean registered " + objName);
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
          LOG.warn("The mbean " + objName.toString() + " is not thread-safe which is probably not a good idea.");
        }
        JmxUtil.unregisterMBeanSafely(mbeanServer, objName, LOG);
        LOG.info("MBean unregistered " + objName);
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
}
