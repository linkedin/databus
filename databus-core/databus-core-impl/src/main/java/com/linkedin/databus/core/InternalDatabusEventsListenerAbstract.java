package com.linkedin.databus.core;

import java.io.IOException;

public abstract class InternalDatabusEventsListenerAbstract implements
    InternalDatabusEventsListener, java.io.Closeable
{

  @Override
  public void onEvent(DataChangeEvent event, long offset, int size)
  {
  }

  @Override
  public void close() throws IOException
  {
  }

}
