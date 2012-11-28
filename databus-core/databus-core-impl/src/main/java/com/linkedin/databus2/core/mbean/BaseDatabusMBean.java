package com.linkedin.databus2.core.mbean;

import java.util.Hashtable;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.JmxUtil;

public abstract class BaseDatabusMBean
{
  public static final String MODULE = BaseDatabusMBean.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String JMX_DOMAIN = "com.linkedin.databus2";

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

  public abstract ObjectName generateObjectName() throws MalformedObjectNameException;

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
