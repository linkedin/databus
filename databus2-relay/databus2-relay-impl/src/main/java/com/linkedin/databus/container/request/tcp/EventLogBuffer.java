package com.linkedin.databus.container.request.tcp;


/**
 * @author: ssubrama
 *
 * This class holds the string to be logged for each event that SendEventsRequest processes.
 */
public class EventLogBuffer
{
  private static final int DEFAULT_CAPACITY = 256;
  private StringBuilder _sb = null;

  public EventLogBuffer()
  {
    _sb = new StringBuilder(DEFAULT_CAPACITY);
  }

  public void append(String s)
  {
    _sb.append(s);
  }

  public String toString()
  {
    return _sb.toString();
  }

  public void reset()
  {
    _sb = new StringBuilder(DEFAULT_CAPACITY);
  }
}
