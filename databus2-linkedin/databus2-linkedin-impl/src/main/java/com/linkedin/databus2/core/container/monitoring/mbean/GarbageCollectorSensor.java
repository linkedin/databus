package com.linkedin.databus2.core.container.monitoring.mbean;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.healthcheck.pub.AttributeDataType;
import com.linkedin.healthcheck.pub.AttributeMetricType;
import com.linkedin.healthcheck.pub.SensorAttribute;
import com.linkedin.healthcheck.pub.sensor.AbstractSensor;

public class GarbageCollectorSensor extends AbstractSensor
{
  private HashMap<String, SensorAttribute> _attrMap;
  private SensorAttribute[] _attrs;
  private List<GarbageCollectorMXBean> _gcMBeans;
  private long _lastResetTime;
  private static Set<String> _tags = new HashSet<String>();
  static private HashMap<String,Set<String>> _map = new HashMap<String,Set<String>> ();

  private static final String COLLECTIONS_CNT_SUFFIX = "GcCollections";
  private static final String COLLECTIONS_TIME_SUFFIX = "GcTime";
  private static final long REFRESH_TIME_MS = 60000;

  public GarbageCollectorSensor(String name, String type)
  {
    super(name, type,_tags,_map);
    _tags.add("ingraphs");
    _attrMap = new HashMap<String, SensorAttribute>();
    reset();
  }

  @Override
  public long getLastResetTime()
  {
    return _lastResetTime;
  }

  @Override
  public void reset()
  {
    _gcMBeans = ManagementFactory.getGarbageCollectorMXBeans();
    _attrs = new SensorAttribute[2 * _gcMBeans.size()];
    _attrMap.clear();

    int attrIdx = 0;
    for (GarbageCollectorMXBean gcBean: _gcMBeans)
    {
	
      SensorAttribute collAttr = new SensorAttribute(this, gcBean.getName() + COLLECTIONS_CNT_SUFFIX,
                                                     Long.valueOf(gcBean.getCollectionCount()),
                                                     AttributeDataType.LONG,
                                                     AttributeMetricType.GAUGE,_tags);
      _attrs[attrIdx++] = collAttr;
      _attrMap.put(collAttr.getName(), collAttr);
      SensorAttribute timeAttr = new SensorAttribute(this, gcBean.getName() + COLLECTIONS_TIME_SUFFIX,
                                                     Long.valueOf(gcBean.getCollectionTime()),
                                                     AttributeDataType.LONG,
                                                     AttributeMetricType.GAUGE,_tags);
      _attrs[attrIdx++] = timeAttr;
      _attrMap.put(timeAttr.getName(), timeAttr);
      
    }

    _lastResetTime = System.currentTimeMillis();
    
  }

  @Override
  public SensorAttribute getAttribute(String name)
  {
    resetIfTooOld();
    return _attrMap.get(name);
  }

  @Override
  public SensorAttribute[] getAttributes()
  {
    resetIfTooOld();
    return _attrs;
  }

  private void resetIfTooOld()
  {
    if (System.currentTimeMillis() - _lastResetTime > REFRESH_TIME_MS) reset();
  }

  public Set<String> getAttributeTags(String name)
  {
      return _tags;
  }
  
}
