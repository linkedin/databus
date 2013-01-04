package com.linkedin.databus.core.util;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class JmxUtil
{

  /**
   * Unregisters an mbean without throwing exceptions
   * @param     mbeanServer         the mbean server to use
   * @param     mbeanName           the name of the mbean to unregister
   * @param     log                 optional (i.e. can be null) logger
   */
  public static void unregisterMBeanSafely(MBeanServer mbeanServer, ObjectName mbeanName, Logger log)
  {
    if (null == mbeanServer || null == mbeanName) return;
    try
    {
      mbeanServer.unregisterMBean(mbeanName);
    }
    catch (InstanceNotFoundException infe)
    {
      if (null != log) log.warn("mbean not registered:" + mbeanName.toString());
    }
    catch (Exception e)
    {
      if (null != log) log.error("failed to unregister mbean:" + e.getMessage(), e);
    }
  }

}
