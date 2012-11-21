package com.linkedin.databus.core.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Helper functions for dealing with {@link java.util.Properties} .
 *
 * @author cbotev
 *
 */
public class PropertiesHelper
{
  public static final String MODULE = PropertiesHelper.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /**
   * Obtain the value of an optional integer property
   * @param  props          the property collection
   * @param  paramName      the property name
   * @param  defaultValue   default value if absent
   * @return the property value
   */
  public static int getOptionalIntParam(Properties props, String paramName, int defaultValue)
  {
    int result = defaultValue;

    String paramStr = props.getProperty(paramName);
    if (null != paramStr)
    {
      try
      {
        result = Integer.parseInt(paramStr);
      }
      catch (NumberFormatException nfe)
      {
        LOG.error("Invalid value for property " + paramName + ": " + paramStr);
      }
    }

    return result;
  }

  /**
   * Read a required integer property value or throw an exception if no such property is found
   * @param  props          The properties to read from
   * @param  name           The property name
   */
  public static int getRequiredIntParam(Properties props, String name)
                                        throws IllegalArgumentException
  {
    if(props.containsKey(name))
      return getRequiredIntParam(props, name, -1);
    else
      throw new IllegalArgumentException("Missing required property '" + name + "'");
  }

  /**
   * Read an integer from the properties instance
   * @param  props          The properties to read from
   * @param  name           The property name
   * @param  defaultValue   The default value to use if the property is not found
   * @return the integer value
   */
  public static int getRequiredIntParam(Properties props, String name, int defaultValue)
  {
    return getRequiredIntParamInRange(props, name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  /**
   * Read an integer from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param props           The properties to read from
   * @param name            The property name
   * @param defaultValue    The default value to use if the property is not found
   * @param rangeMin        The minimum value of the range in which the value must fall (inclusive)
   * @param rangeMax        The maximum value of the range in which the value must fall (inclusive)
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the integer value
   */
  public static int getRequiredIntParamInRange(Properties props, String name, int defaultValue,
                                               int rangeMin, int rangeMax)
                                               throws IllegalArgumentException
  {
    int value = defaultValue;
    if(props.containsKey(name))
        value = Integer.parseInt(props.getProperty(name));

    if(value < rangeMin || value > rangeMax)
      throw new IllegalArgumentException(name + " has value " + value + " which is not in the range (" + rangeMin + "," + rangeMax + ").");

    return value;
  }

  /**
   * Obtain the value of an optional long property
   * @param  props          the property collection
   * @param  paramName      the property name
   * @param  defaultValue   default value if absent
   * @return the property value
   */
  public static long getOptionalLongParam(Properties props, String paramName, long defaultValue)
  {
    long result = defaultValue;

    String paramStr = props.getProperty(paramName);
    if (null != paramStr)
    {
      try
      {
        result = Long.parseLong(paramStr);
      }
      catch (NumberFormatException nfe)
      {
        LOG.error("Invalid value for property " + paramName + ": " + paramStr);
      }
    }

    return result;
  }

  /**
   * Obtain the value of an optional double property
   * @param  props          the property collection
   * @param  paramName      the property name
   * @param  defaultValue   default value if absent
   * @return the property value
   */
  public static double getOptionalDoubleParam(Properties props, String paramName, double defaultValue)
  {
    double result = defaultValue;

    String paramStr = props.getProperty(paramName);
    if (null != paramStr)
    {
      try
      {
        result = Double.parseDouble(paramStr);
      }
      catch (NumberFormatException nfe)
      {
        LOG.error("Invalid value for property " + paramName + ": " + paramStr);
      }
    }

    return result;
  }

  /**
   * Obtain the value of an optional boolean property
   * @param  props          the property collection
   * @param  paramName      the property name
   * @param  defaultValue   default value if absent
   * @return the property value
   */
  public static boolean getOptionalBooleanParam(Properties props, String paramName,
                                                boolean defaultValue)
  {
    boolean result = defaultValue;

    String paramStr = props.getProperty(paramName);
    if (null != paramStr)
    {
      result = Boolean.parseBoolean(paramStr);
    }

    return result;
  }

  /**
   * Get a string property or throw and exception if no such property is defined.
   */
  public static String getRequiredStringParam(Properties props, String name)
                                              throws IllegalArgumentException
  {
    if(props.containsKey(name))
      return props.getProperty(name);
    else
      throw new IllegalArgumentException("Missing required property '" + name + "'");
  }

  /**
   * Read a properties file from the given path
   * @param filename The path of the file to read
   */
  public static Properties loadProps(String filename) {
    FileInputStream propStream = null;
    try
    {
      propStream = new FileInputStream(filename);
    }
    catch (FileNotFoundException e)
    {
      LOG.error("unable to open properties stream: " + e.getMessage(), e);
      return null;
    }
    Properties props = null;
    try
    {
      props = new Properties();
      props.load(propStream);
    }
    catch (IOException e)
    {
      LOG.error("unable to load properties: " + e.getMessage(), e);
    }
    finally
    {
      if (null != propStream)
      {
        try
        {
          propStream.close();
        }
        catch (Exception e)
        {
          LOG.error("unable to close property file stream:" + e.getMessage(), e);
        }
      }
    }
    return props;
  }

}
