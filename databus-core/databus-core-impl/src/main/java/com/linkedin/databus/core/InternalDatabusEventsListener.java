package com.linkedin.databus.core;


/**
 * Internal Interface for listening for databus events as they enter the system.
 * @author sdas
 *
 */
public interface InternalDatabusEventsListener extends java.io.Closeable
{
  /**
   *
   * @param event An event was added to the Buffer
   * @param offset At this offset
   */
  public void onEvent(DataChangeEvent event, long offset, int size);
}
