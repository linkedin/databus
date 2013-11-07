package com.linkedin.databus.core.util;
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
