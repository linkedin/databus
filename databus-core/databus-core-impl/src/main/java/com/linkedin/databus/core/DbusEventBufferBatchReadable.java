package com.linkedin.databus.core;

import java.nio.channels.WritableByteChannel;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.DbusFilter;

/**
 * allows to read events from the buffer in a batch (of some fixed size)
 */
public interface DbusEventBufferBatchReadable
{
  /**
  *
  * @param checkPoint
  * @param streamFromLatestScn
  * @param batchFetchSize
  * @param writeChannel
  * @param encoding
  * @param filter
  * @return
  * @throws ScnNotFoundException
  */

 public int streamEvents(boolean streamFromLatestScn,
                         int batchFetchSize,
                         WritableByteChannel writeChannel, Encoding encoding,
                         DbusFilter filter)
 throws ScnNotFoundException, DatabusException, OffsetNotFoundException;
}
