/**
 * $Id: ExampleUtils.java 155034 2010-12-12 06:43:52Z mstuart $ */
package com.linkedin.databus.groupleader.example.simple;
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
