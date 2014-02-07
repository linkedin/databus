package com.linkedin.databus.core;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DbusPrettyLogUtils
{
  /**
   * 
   * @param msg
   * @param t
   * @param log
   * @param level
   */
  public static void logAtLevel(String msg, Throwable t, Logger log, Level level) {
    switch(level.toInt()) {
    case Level.INFO_INT:
      logExceptionAtInfo(msg, t, log);
      break;
    case Level.WARN_INT:
      logExceptionAtWarn(msg, t, log);
      break;
    case Level.ERROR_INT:
      logExceptionAtError(msg, t, log);
      break;
    default:
      logExceptionAtInfo(msg, t, log);
      break;
    }
  }

  /**
   * A helper method to uniformly print log messages where
   * 1. The user-message is simple string that is printed as-is
   *    ( i.e., no formatted / parameterized strings expected )
   * 2. The exception message and cause are logged at INFO level
   * 3. The exception trace is logged at DEBUG level
   *
   * @param msg The message caller wants logged
   * @param t   The exception raised that the user wants logged
   * @param log The log4j logger with which the user is logging messages
   */
  public static void logExceptionAtInfo(String msg, Throwable t, Logger log)
  {
    // Logger (log) is not expected to be null
    if (null == log)
    {
      return;
    }
    if(t == null)
    {
      log.info(msg);
    }
    else
    {
      log.info(msg + ". Exception message = " + t +
               ". Exception cause = " + t.getCause());
      if (log.isDebugEnabled())
      {
        log.debug(". Exception trace = ", t);
      }
    }
    return;
  }

  /**
   * A helper method to uniformly print log messages where
   * 1. The user-message is simple string that is printed as-is
   *    ( i.e., no formatted / parameterized strings expected )
   * 2. The exception message and cause are logged at WARN level
   * 3. The exception trace is logged at DEBUG level
   *
   * @param msg The message caller wants logged
   * @param t   The exception raised that the user wants logged
   * @param log The log4j logger with which the user is logging messages
   */
  public static void logExceptionAtWarn(String msg, Throwable t, Logger log)
  {
    // Logger (log) is not expected to be null
    if ( null == log)
    {
      return;
    }

    if ( null == t)
    {
      log.warn(msg);
    }
    else
    {
      log.warn(msg + ". Exception message = " + t +
               ". Exception cause = " + t.getCause());
      if (log.isDebugEnabled())
      {
        log.debug(". Exception trace = ", t);
      }
    }
    return;
  }

  /**
   * A helper method to uniformly print log messages where
   * 1. The user-message is simple string that is printed as-is
   *    ( i.e., no formatted / parameterized strings expected )
   * 2. The exception message and cause are logged at DEBUG level
   * 3. The exception trace is logged at DEBUG level
   *
   * @param msg The message caller wants logged
   * @param t   The exception raised that the user wants logged
   * @param log The log4j logger with which the user is logging messages
   */
  public static void logExceptionAtDebug(String msg, Throwable t, Logger log)
  {
    // Logger (log) is not expected to be null
    if (null == log || ! log.isDebugEnabled())
    {
      return;
    }
    if(null == t)
    {
      log.debug(msg);
    }
    else
    {
      log.debug(msg + ". Exception message = " + t +
                ". Exception cause = " + t.getCause());
      log.debug(". Exception trace = ", t);
    }
    return;
  }

  /**
   * A helper method to uniformly print log messages where
   * 1. The user-message is simple string that is printed as-is
   *    ( i.e., no formatted / parameterized strings expected )
   * 2. The exception message and cause are logged at ERROR level
   * 3. The exception trace is logged at DEBUG level
   *
   * @param msg The message caller wants logged
   * @param t   The exception raised that the user wants logged
   * @param log The log4j logger with which the user is logging messages
   */
  public static void logExceptionAtError(String msg, Throwable t, Logger log)
  {
    // Logger (log) is not expected to be null
    if (null == log)
    {
      return;
    }
    if(null == t)
    {
      log.error(msg);
    }
    else
    {
      log.error(msg + ". Exception message = " + t +
                ". Exception cause = " + t.getCause());
      if (log.isDebugEnabled())
      {
        log.debug(". Exception trace = ", t);
      }
    }
    return;
  }

}
