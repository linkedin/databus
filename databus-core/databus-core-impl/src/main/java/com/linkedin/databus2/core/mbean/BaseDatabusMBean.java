package com.linkedin.databus2.core.mbean;
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
