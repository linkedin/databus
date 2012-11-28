package com.linkedin.databus.core.util;

import java.beans.PropertyDescriptor;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * A helper class for loading configurations from property files and JSON
 * @author cbotev
 *
 * @param <D>   Config class
 */
public class ConfigLoader<D> extends ReadWriteSyncedObject
{
  public final static String MODULE = ConfigLoader.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  protected final String _propPrefix;
  protected final ConfigBuilder<D> _configBuilder;
  private BeanUtilsBean _beanUtilsBean;

  public ConfigLoader(String propPrefix, ConfigBuilder<D> dynConfigBuilder)
	                  throws InvalidConfigException
  {
	super(true);
	_propPrefix = propPrefix;
	_configBuilder = dynConfigBuilder;
	_beanUtilsBean = new BeanUtilsBean();
  }

  public D loadConfig(Map<?, ?> props) throws InvalidConfigException
  {
	Map<String, Object> realProps = new HashMap<String, Object>();
	D result = null;
	if (_propPrefix != null && _propPrefix.length() > 0)
	{
      int prefixLen = _propPrefix.length();
	  for (Map.Entry<?, ?> entry: props.entrySet())
	  {
	    String keyStr = entry.getKey().toString();
	    if (keyStr.startsWith(_propPrefix))
	    {
	      String realKey = keyStr.substring(prefixLen);
	      realProps.put(realKey, entry.getValue());
	    }
	  }
	}
	else
	{
      for (Map.Entry<?, ?> entry: props.entrySet())
      {
        realProps.put(entry.getKey().toString(), entry.getValue());
      }
	}

	synchronized (_configBuilder)
	{
      for (Map.Entry<String, Object> entry : realProps.entrySet())
	  {
	    setBareSetting(entry.getKey(), entry.getValue());
	  }

   	  result = _configBuilder.build();
	}

	return result;
  }

  private void setBareSetting(String settingName, Object value) throws InvalidConfigException
  {
    //check syntax because beanutils silently ignore settings with typos
    PropertyDescriptor propDesc = null;
    try
    {
       propDesc = _beanUtilsBean.getPropertyUtils().getPropertyDescriptor(_configBuilder, settingName);
    }
    catch (Exception e)
    {
      throw new InvalidConfigException("Error obtaining configuration property. settingName=" + settingName, e);
    }
    if (null == propDesc)
    {
      throw new InvalidConfigException("Unknown configuration property:" + settingName);
    }

    try
    {
      if (value instanceof String)
      {
        value = ((String)value).trim();
      }

      _beanUtilsBean.setProperty(_configBuilder, settingName, value);
    }
    catch (Exception e)
    {
      throw new InvalidConfigException("setting failed: " + settingName, e);
    }
  }

  public D setSetting(String settingName, Object value) throws InvalidConfigException
  {
    synchronized (_configBuilder)
    {
      if (settingName.startsWith(_propPrefix))
      {
        String realKey = settingName.substring(_propPrefix.length());
        setBareSetting(realKey, value);
      }

      D newConfig = _configBuilder.build();
      return newConfig;
    }
  }

  public D loadConfigFromJson(Reader jsonReader) throws InvalidConfigException
  {
    D newConfig = null;
	try
	{
	  ObjectMapper _jsonMapper = new ObjectMapper();
	  Map<?, ?> jsonMap = _jsonMapper.readValue(jsonReader, new TypeReference<Map<?, ?>>(){});
	  synchronized (_configBuilder)
      {
        fillBeanFromMap(_configBuilder, jsonMap);
      }

	  newConfig = _configBuilder.build();
    }
    catch (Exception e)
	{
	  throw new InvalidConfigException("Load from JSON failed", e);
	}

    return newConfig;
  }

	private void fillBeanFromMap(Object bean, Map<?, ?> map) throws IllegalAccessException,
	                                                                InvocationTargetException,
	                                                                NoSuchMethodException
	{
      PropertyUtilsBean propUtils = _beanUtilsBean.getPropertyUtils();

      for (Map.Entry<?, ?> entry : map.entrySet())
	  {
	    String keyStr = entry.getKey().toString();
	    Object value = entry.getValue();
	    if (value instanceof Map<?, ?>)
	    {
	      Object subBean = propUtils.getProperty(bean, keyStr);
	      if (null != subBean) fillBeanFromMap(subBean, (Map<?, ?>)value);
	    }
	    else if (value instanceof List<?>)
	    {
	      fillPropertyFromList(bean, keyStr, (List<?>)value);
	    }
	    else
	    {
	      propUtils.setProperty(bean, keyStr, value);
	    }
	  }
	}

	private void fillPropertyFromList(Object bean, String propName, List<?> values)
	                                  throws IllegalAccessException,
                                             InvocationTargetException,
                                             NoSuchMethodException
	{
      PropertyUtilsBean propUtils = _beanUtilsBean.getPropertyUtils();
      int idx = 0;
      for (Object elem: values)
      {
        if (elem instanceof Map<?,?>)
        {
          Object subBean = propUtils.getIndexedProperty(bean, propName, idx);
          fillBeanFromMap(subBean, (Map<?, ?>)elem);
        }
        else
        {
          propUtils.setIndexedProperty(bean, propName, idx, elem);
        }
        ++idx;
      }
	}

  /**
   * Provides direct access to the config builder.
   */
  public ConfigBuilder<D> getConfigBuilder()
  {
    return _configBuilder;
  }

  public String getPropPrefix()
  {
    return _propPrefix;
  }

}
