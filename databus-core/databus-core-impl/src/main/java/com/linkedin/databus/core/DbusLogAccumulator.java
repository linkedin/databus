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

import org.apache.log4j.Logger;

/**
 * A template class that allows for accumulating log messages with a limit
 * on the number of messages that can be retained
 *
 * The use-case is mostly on the customer facing client side where a trade-off
 * needs to be made between showing the user only understandable log information
 * vis-a-vis collecting logs useful/necessary for internal debugging
 *
 * The design / implementation is currently TBD, and so current implementation
 * outputs the messages to DEBUG logs.
 */
public class DbusLogAccumulator
{
  /**
   * This API is subject to change. Adding this as a place-holder so as to capture all
   * the messages that are candidates to be accumulated. It currently becomes a debug
   * log
   *
   * TBD : Is there intersection of functionality between DbusPrettyLogUtils.java and
   * this file ? Currently the thinking is no - DbusLogAccumulator would just be responsible
   * for buffering data, but DbusPrettyLogUtils for outputing them to a log
   */
  public static void addLog(String msg, Logger log)
  {
    if (null == log)
    {
      return;
    }
    if (log.isDebugEnabled())
    {
      log.debug(msg);
    }
  }
}
