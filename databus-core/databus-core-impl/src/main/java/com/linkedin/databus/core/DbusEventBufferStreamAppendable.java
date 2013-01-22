package com.linkedin.databus.core;

import java.nio.channels.ReadableByteChannel;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * allows to append events to the buffer from a stream
 *
 */
public interface DbusEventBufferStreamAppendable
{

  /*
   * Read events from a channel and append them to a buffer.
   * @param readChannel : ByteChannel to read events from
   * @param eventListeners : List of listeners interested in the events in the channel
   * @param statsCollector : Stats Collector for this event
   * @return Number of events appended
   */
  int readEvents(ReadableByteChannel readChannel,
          Iterable<InternalDatabusEventsListener> eventListeners,
          DbusEventsStatisticsCollector statsCollector)
          throws InvalidEventException;
}
