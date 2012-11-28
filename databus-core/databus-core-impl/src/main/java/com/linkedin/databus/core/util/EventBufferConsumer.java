package com.linkedin.databus.core.util;

public interface EventBufferConsumer
{
  public void onInvalidEvent(long numEventsReadSoFar);

}
