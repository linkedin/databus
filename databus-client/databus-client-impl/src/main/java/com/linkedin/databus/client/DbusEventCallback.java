package com.linkedin.databus.client;

import com.linkedin.databus.core.DbusEvent;

public interface DbusEventCallback
{
  boolean startEventSequence();
  boolean startSource(short srcId);
  boolean onEvent(DbusEvent e);
  boolean endSource(short srcId);
  boolean endEventSequence();
}
