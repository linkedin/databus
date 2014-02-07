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

import java.util.IllegalFormatException;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * A class that allows for accumulating log messages with a limit
 * on the number of messages that can be retained
 *
 */
public class DbusLogAccumulator
{
  /**
   *  DbusLogAccumulator is responsible for buffering data,
   *  and DbusPrettyLogUtils for outputting it to a log
   */

  public static final int MAX_NUMBER_OF_LOG_MSGS = 256;
  private int _totalNumberOfMsgs; // actual number of messages
  private final CircularFifoBuffer _msgs;

  /**
   * DO NOT USE. needs to be removed
   * this method directly prints out the log message
   * @param s
   * @param log
   */
  @Deprecated
  public static void addLog(String s, Logger log) {
    if (null == log)
      return;

    if(log.isDebugEnabled())
      log.debug(s);
  }

  public DbusLogAccumulator() {
    this(MAX_NUMBER_OF_LOG_MSGS);
  }

  public DbusLogAccumulator(int maxNumberMsgs) {
    _msgs = new CircularFifoBuffer(maxNumberMsgs);
    reset();
  }

  public void reset() {
    _msgs.clear();
    _totalNumberOfMsgs = 0;
  }

  /**
   * Auxiliary message. Avoid using this method if passing a complex string (s+s1+s2+s3...)
   * @param msg as a string
   */
  public void addMessage(String msg) {
    addMessage(new DebugMessage(msg));
  }

  public void addMessage(String format, Object val) {
    addMessage(new DebugMessage(format, val));
  }

  /**
   * Add a new message (without converting it to a String)
   * @param msg as format + args
   */
  public void addMessage(DebugMessage msg) {
    _msgs.add(msg);
    ++ _totalNumberOfMsgs;
  }

  /**
   * Total number of messages (including the overwritten ones)
   * @return total number of messages added to the accumulator
   */
  public int getTotalNumberOfMessages() {
    return _totalNumberOfMsgs;
  }

  /**
   * Number of stored/available messages
   * @return number of stored messages
   */
  public int getNumberOfMessages() {
    return _msgs.size();
  }

  /**
   * Print all the messages
   * @param log
   * @param level
   */
  public void prettyLog(Logger log, Level level) {
    for(Object o : _msgs) {
      DebugMessage dm = (DebugMessage) o;
      String msg = dm.toString();
      Throwable t = dm.getException();
      DbusPrettyLogUtils.logAtLevel(msg, t, log, level);
    }
  }

  /**
   * Each log line is stored as DebugMessage
   * and its content is constructed on demand only
   */
  public static class DebugMessage {
    final private String _format;
    final private Object [] _params;
    private Throwable _exception;

    public DebugMessage(String format, Object... params) {
      _format = format;
      _params = params;
      _exception = null;
    }

    public DebugMessage(String msg) {
      this(null, new Object [] {msg});
    }
    public DebugMessage setException(Throwable e) {
      _exception = e;
      return this;
    }
    public Throwable getException() {
      return _exception;
    }

    @Override
    public String toString() {
      if(_format != null) {
        try {
          return String.format(_format, _params);
        } catch (IllegalFormatException e) {
          return  "failed to format with " + _format + ":" +  e.getMessage();
        }
      }

      if(_params[0] == null)
        return "null";

      return _params[0].toString();
    }
  }
}
