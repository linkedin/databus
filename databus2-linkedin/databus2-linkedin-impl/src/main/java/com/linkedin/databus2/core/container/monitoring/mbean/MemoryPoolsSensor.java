package com.linkedin.databus2.core.container.monitoring.mbean;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import com.linkedin.healthcheck.pub.AttributeDataType;
import com.linkedin.healthcheck.pub.AttributeMetricType;
import com.linkedin.healthcheck.pub.SensorAttribute;
import com.linkedin.healthcheck.pub.sensor.AbstractSensor;

public class MemoryPoolsSensor extends AbstractSensor
{
  private enum PoolType
  {
    Eden,
    Survivor,
    OldGen,
    PermGen
  }

  private HashMap<String, SensorAttribute> _attrMap;
  private SensorAttribute[] _attrs;
  private long _lastResetTime;

  private static final String CURRENT_USAGE_SUFFIX = "CurUsed";
  private static final String COMMITED_SUFFIX = "Committed";
  private static final String MAX_SUFFIX = "Max";
  private static final String COLLECTION_USAGE_SUFFIX = "CollectionUsed";
  private static final String PEAK_USAGE_SUFFIX = "PeakUsed";

  private static final long REFRESH_TIME_MS = 60000;
  private static Set<String> _tags = new HashSet<String>();
 static private HashMap<String,Set<String>> _map = new HashMap<String,Set<String>> ();


  
  public MemoryPoolsSensor(String name, String type)
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
    _attrMap.clear();

    for (MemoryPoolMXBean poolBean: ManagementFactory.getMemoryPoolMXBeans())
    {
      String beanName = poolBean.getName().toUpperCase();
      if (beanName.contains("EDEN"))
      {
        addAttributeForBean(poolBean, PoolType.Eden);
      }
      else if (beanName.contains("SURVIVOR"))
      {
        addAttributeForBean(poolBean, PoolType.Survivor);
      }
      else if (beanName.contains("OLD GEN"))
      {
        addAttributeForBean(poolBean, PoolType.OldGen);
      }
      else if (beanName.contains("PERM GEN"))
      {
        addAttributeForBean(poolBean, PoolType.PermGen);
      }
    }
    _attrs = new SensorAttribute[_attrMap.size()];

    int i = 0;
    for (Entry<String, SensorAttribute> entry: _attrMap.entrySet()) _attrs[i++] = entry.getValue();

    _lastResetTime = System.currentTimeMillis();
  }

  private void addAttributeForBean(MemoryPoolMXBean poolBean, PoolType poolType)
  {
      
    SensorAttribute curUsageAttr = new SensorAttribute(
        this, poolType + CURRENT_USAGE_SUFFIX, poolBean.getUsage().getUsed(), AttributeDataType.LONG,
        AttributeMetricType.GAUGE,_tags);
    _attrMap.put(curUsageAttr.getName(), curUsageAttr);

    SensorAttribute peakUsageAttr = new SensorAttribute(
        this, poolType + PEAK_USAGE_SUFFIX, poolBean.getPeakUsage().getUsed(), AttributeDataType.LONG,
        AttributeMetricType.GAUGE,_tags);
    _attrMap.put(peakUsageAttr.getName(), peakUsageAttr);

    SensorAttribute collUsageAttr = new SensorAttribute(
        this, poolType + COLLECTION_USAGE_SUFFIX, poolBean.getCollectionUsage().getUsed(),
        AttributeDataType.LONG, AttributeMetricType.GAUGE,_tags);
    _attrMap.put(collUsageAttr.getName(), collUsageAttr);

    SensorAttribute maxAttr = new SensorAttribute(
        this, poolType + MAX_SUFFIX, poolBean.getUsage().getMax(), AttributeDataType.LONG,
        AttributeMetricType.GAUGE,_tags);
    _attrMap.put(maxAttr.getName(), maxAttr);

    SensorAttribute commitedAttr = new SensorAttribute(
        this, poolType + COMMITED_SUFFIX, poolBean.getUsage().getCommitted(), AttributeDataType.LONG,
        AttributeMetricType.GAUGE,_tags);
     _attrMap.put(commitedAttr.getName(), commitedAttr);
    
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

}
