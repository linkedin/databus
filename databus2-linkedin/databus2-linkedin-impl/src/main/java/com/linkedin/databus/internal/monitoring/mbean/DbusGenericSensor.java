package com.linkedin.databus.internal.monitoring.mbean;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.databus.internal.monitoring.mbean.MBeanSensorHelper.SensorValue;
import com.linkedin.healthcheck.pub.SensorAttribute;
import com.linkedin.healthcheck.pub.annotation.AnnotatedSensor;
import com.linkedin.healthcheck.pub.sensor.AbstractSensor;

public class  DbusGenericSensor<T>  extends AbstractSensor
{

  private final T _stats;
  private final HashMap<String,SensorValue> _methods;
  static private Set<String> _tags = new HashSet<String>();
  static private HashMap<String,Set<String>> _map = new HashMap<String,Set<String>> ();

  public  DbusGenericSensor (String name,String type,T stats)
  {
     super("databus2."+name,type,_tags,_map);
    _tags.add("ingraphs");
    _stats = stats;
    _methods = new HashMap<String,SensorValue>();
    MBeanSensorHelper.methodToMap(_stats, _methods);
  }

  @Override
  public long getLastResetTime()
  {
    return System.currentTimeMillis();
  }

  @Override
  public void reset()
  {

  }

  public HashMap<String,SensorValue> getMethods()
  {
	  return _methods;
  }

  /*
  public static void main(String[] args) {
	  DbusHttpTotalStats stats = new DbusHttpTotalStats(123,"abc",true,false,null);
	  DbusGenericSensor<DbusHttpTotalStats> sensor = new DbusGenericSensor<DbusHttpTotalStats>("test","http",stats);
	  SensorAttribute[] sensorAttrs= sensor.getAttributes();
	  HashMap<String,SensorValue> map = sensor.getMethods();
	  for (String k: map.keySet()) {
		  System.out.printf("keys= %s , value=%s\n" , k,map.get(k).toString());
	  }

  }
  */

  @Override
  public SensorAttribute getAttribute(String attrib)
  {
    if (_methods.containsKey(attrib))
    {
      MBeanSensorHelper.invokeMethods(_stats,_methods,attrib);
      SensorValue value = _methods.get(attrib);
     return new SensorAttribute(this,attrib,value.get_value(),value.get_type(),value.get_metricType(), _tags);
    }
    return null;
  }

  @Override
  public SensorAttribute[] getAttributes()
  {
    MBeanSensorHelper.invokeMethods(_stats, _methods, null);
    SensorAttribute[] attribList = new SensorAttribute[_methods.size()];
    int i=0;
    for (String k: _methods.keySet())
    {
    	SensorValue value = _methods.get(k);
    	attribList[i++] =  new SensorAttribute(this,k,value.get_value(),value.get_type(),value.get_metricType(),_tags);
    }
    return attribList;
  }
  
  public Set<String> getAttributeTags(String name)
  {
      return _tags;
  }

}
