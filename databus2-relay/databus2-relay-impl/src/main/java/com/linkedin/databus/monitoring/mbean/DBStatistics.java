package com.linkedin.databus.monitoring.mbean;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.container.netty.ServerContainer;



public class DBStatistics implements DBStatisticsMBean
{

  private final String _name;
  private long _scn;
  private long _lastUpdateTimestamp;

  private final HashMap<String,SourceDBStatistics> _perSourceStats;
  /** Logger for error and debug messages. */
  private final Logger _log = Logger.getLogger(getClass());

  public DBStatistics (String name)
  {
    _name = name;
    _perSourceStats = new HashMap<String,SourceDBStatistics> (100);
    _lastUpdateTimestamp = 0;
    reset();
  }

  @Override
  public String getDBSourceName()
  {
    return _name;
  }

  public synchronized void setMaxDBScn(long s)
  {
    _scn = s;
    _lastUpdateTimestamp = System.currentTimeMillis();
  }

  @Override
  public synchronized long getMaxDBScn()
  {
    return _scn;
  }

  public synchronized void addSrcStats(SourceDBStatistics srcStats)
  {
    _perSourceStats.put(srcStats.getSourceName(),srcStats);
  }

  public synchronized void setSrcMaxScn(String srcName,long maxScn)
  {
    if (_perSourceStats.containsKey(srcName)) {
      SourceDBStatistics stats = _perSourceStats.get(srcName);
      if (null != stats) {
         stats.setMaxScn(maxScn);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public synchronized boolean registerAsMbean(MBeanServer mbeanServer,boolean register) {
    try {
      Hashtable<String,String> props = new Hashtable<String,String>();
      props.put("type", "DBStatistics");
      props.put("name", getDBSourceName());
      ObjectName objectName = new ObjectName(ServerContainer.JMX_DOMAIN, props);

      if (mbeanServer.isRegistered(objectName))
      {
        _log.warn("Unregistering old DB Statistics  mbean: " + objectName);
        mbeanServer.unregisterMBean(objectName);
      }
      if (register) {
        mbeanServer.registerMBean(this, objectName);
        _log.info("Registered DB statistics statistics mbean: " + objectName);
      }
      Iterator it = _perSourceStats.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry pairs = (Map.Entry) it.next();
        SourceDBStatistics stats = (SourceDBStatistics) pairs.getValue();
        if (null != stats)
        {
          if (register) {
            stats.registerAsMbean(mbeanServer);
          } else {
            stats.unregisterAsMbean(mbeanServer);
          }
        }
      }
    } catch (Exception ex) {
      if (register) {
        _log.error("Failed to register Mbean:" + getDBSourceName() , ex);
      } else {
        _log.error("Failed to unregister Mbean:" + getDBSourceName() , ex);
      }
      return false;
    }
    return true;
  }

  public synchronized boolean registerAsMbean(MBeanServer mbeanServer) {

    return registerAsMbean(mbeanServer,true);
  }

  public synchronized boolean unregisterAsMbean(MBeanServer mbeanServer) {

    return registerAsMbean(mbeanServer,false);
  }

  @Override
  public synchronized SourceDBStatisticsMBean getPerSourceStatistics(String name)
  {
    return _perSourceStats.get(name);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public synchronized void reset()
  {
    _scn = -1;
    _lastUpdateTimestamp = 0;
    Iterator it = _perSourceStats.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry pairs = (Map.Entry) it.next();
      SourceDBStatisticsMBean stats = (SourceDBStatisticsMBean) pairs.getValue();
      if (null != stats)
      {
        stats.reset();
      }

    }
  }

  @Override
  public synchronized long getTimeSinceLastUpdate()
  {
    return System.currentTimeMillis() - _lastUpdateTimestamp;
  }

}
