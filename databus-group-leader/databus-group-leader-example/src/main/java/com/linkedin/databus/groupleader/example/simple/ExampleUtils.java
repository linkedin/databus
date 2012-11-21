/**
 * $Id: ExampleUtils.java 155034 2010-12-12 06:43:52Z mstuart $ */
package com.linkedin.databus.groupleader.example.simple;

import org.apache.log4j.Logger;

/**
 *
 * @author Mitch Stuart
 * @version $Revision: 155034 $
 */
public class ExampleUtils
{
  public static String getRequiredStringProperty(String propname, Logger log)
  {
    String val = System.getProperty(propname);

    if (val == null || val.trim().length() == 0)
    {
      throw new IllegalArgumentException("Missing property: " + propname);
    }

    if (log != null)
    {
      log.info("Property " + propname + "=" + val);
    }

    return val;
  }

  public static int getRequiredIntProperty(String propname, Logger log)
  {
    String stringVal = getRequiredStringProperty(propname, log);
    int intVal = Integer.parseInt(stringVal);
    return intVal;
  }


  /**
   * Private constructor - static methods only
   */
  private ExampleUtils()
  {
  }

}
