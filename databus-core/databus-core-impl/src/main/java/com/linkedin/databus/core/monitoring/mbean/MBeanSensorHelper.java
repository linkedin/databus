package com.linkedin.databus.core.monitoring.mbean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.healthcheck.pub.AttributeDataType;
import com.linkedin.healthcheck.pub.AttributeMetricType;

 public class MBeanSensorHelper
 {

	public static final String MODULE = DbusEventBuffer.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static <T> void methodToMap(T obj, HashMap<String,SensorValue> methodMap)
	{
		Class<? extends Object> type = obj.getClass();
		Method[] methods = type.getMethods();
		for (Method method: methods)
		{

			if ((method.getReturnType()==Long.TYPE || method.getReturnType()==Integer.TYPE) && method.getGenericParameterTypes().length==0)
			{
				String methodName = method.getName();
				if (methodName.startsWith("get"))
				{
					String attribName = methodName.substring(3);
					AttributeMetricType metType = (attribName.contains("Num") || attribName.contains("num") || attribName.contains("count") ||  attribName.contains("Count"))
					? AttributeMetricType.COUNTER : AttributeMetricType.GAUGE;
					AttributeDataType dt = (method.getReturnType()==Integer.TYPE) ? AttributeDataType.INTEGER  : AttributeDataType.LONG;
					methodMap.put(attribName, new  SensorValue(0,dt,metType));
				}
			}
		}

	}


	public static <T> void invokeMethods(T obj, HashMap<String,SensorValue> methodMap,String key)
	{
		Class<? extends Object> type = obj.getClass();
		Method[] methods = type.getMethods();
		for (Method method: methods)
		{
			String methodName = method.getName();
			if (methodName.length()>3)
			{
				//dealing only with methods in methodMap that return long
				String attribName = methodName.substring(3);
				if ((key != null) && (!key.equals(attribName))) {
					continue;
				}
				if (methodName.startsWith("get") && methodMap.containsKey(attribName)) {
					SensorValue value = methodMap.get(attribName);
					try {
						try {
						    if (method.getReturnType()==Integer.TYPE) {
						      value.set_value(method.invoke(obj,(Object[]) null));
						    } else {
						      value.set_value(method.invoke(obj,(Object[]) null));
						    }
						} catch (IllegalAccessException e) {
							LOG.info("Illegal Access Exception! " +type.getCanonicalName() + methodName);
						}
					} catch (IllegalArgumentException e) {
						LOG.info("Illegal Argument Exception! " + type.getCanonicalName() +  methodName);
					} catch (InvocationTargetException e) {
						LOG.info("Invocation Target Exception! " + type.getCanonicalName() +  methodName);
					}
				}
			}
		}

	}

	static public class SensorValue
	{
		private Object _value;
		private AttributeDataType _type;
		private AttributeMetricType _metricType;

		public SensorValue(Object _value, AttributeDataType _type,
				AttributeMetricType _metricType) {
			super();
			this._value = _value;
			this._type = _type;
			this._metricType = _metricType;
		}

		public Object get_value() {
			return _value;
		}
		public void set_value(Object _value) {
			this._value = _value;
		}
		public AttributeDataType get_type() {
			return _type;
		}
		public void set_type(AttributeDataType _type) {
			this._type = _type;
		}
		public AttributeMetricType get_metricType() {
			return _metricType;
		}
		public void set_metricType(AttributeMetricType _metricType) {
			this._metricType = _metricType;
		}

		@Override
    public String toString() {
			return _value + ":" + _type + ":" + _metricType;
		}

	}


}
