package com.linkedin.databus.monitoring.mbean;
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
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.container.netty.ServerContainer;

public class SourceDBStatistics implements SourceDBStatisticsMBean
{

  private final String _name;
  private long _scn;
  private long _lastUpdateTimestamp;
  private final Logger _log = Logger.getLogger(getClass());


  public SourceDBStatistics(String name)
  {
    _name = name;
    _lastUpdateTimestamp=0;
    reset();
  }

  @Override
  public String getSourceName()
  {
    return _name;
  }

  public synchronized void setMaxScn(long s)
  {
    _scn = s;
    _lastUpdateTimestamp = System.currentTimeMillis();
  }

  @Override
  public synchronized long getMaxScn()
  {
    return _scn;
  }

  @Override
  public synchronized void reset()
  {
    _scn = -1;
    _lastUpdateTimestamp=0;
  }

  protected synchronized boolean registerAsMbean(MBeanServer mbeanServer,boolean register)
  {
    try {
      Hashtable<String,String> props = new Hashtable<String,String>();
      props.put("type", "SourceDBStatistics");
      props.put("name", getSourceName());
      ObjectName objectName = new ObjectName(ServerContainer.JMX_DOMAIN, props);

      if (mbeanServer.isRegistered(objectName))
      {
        _log.warn("Unregistering old DB Statistics  mbean: " + objectName);
        mbeanServer.unregisterMBean(objectName);
      }
      if (register) {
        mbeanServer.registerMBean(this, objectName);
        _log.info("Registered DB source statistics statistics mbean: " + objectName);
      }
    } catch (Exception ex) {
      if (register) {
        _log.error("Failed to register Mbean:" + getSourceName() , ex);
      } else {
        _log.error("Failed to unregister Mbean:" + getSourceName() , ex);
      }
      return false;
    }
    return true;

  }

  public synchronized boolean registerAsMbean(MBeanServer mbeanServer)
  {
    return registerAsMbean(mbeanServer,true);

  }

  public synchronized boolean unregisterAsMbean(MBeanServer mbeanServer)
  {
    return registerAsMbean(mbeanServer,false);
  }

  @Override
  public synchronized long getTimeSinceLastUpdate()
  {
    return System.currentTimeMillis() - _lastUpdateTimestamp;
  }

}
