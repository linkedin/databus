package com.linkedin.databus.bootstrap.api;

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

/**
 *
 * Contains the result or processing by BootstrapEventWriters. This class provides both
 * (a) the processing result of the last event sent to BootstrapEventWriters(both V2 and
 * V3) (b) and the cumulative stat on the number of events streamed out by the
 * EventWriters since the time of their creation.
 *
 */
public class BootstrapEventProcessResult
{
  /**
   * Return Total number of events which were streamed to the client by
   * BootstrapEventWriter
   */
  public long getNumRowsWritten()
  {
    return _numRowsWritten;
  }

  /**
   * Returns true if the writer could not write the event because the client buffer size
   * exceeded
   */
  public boolean isClientBufferLimitExceeded()
  {
    return _isClientBufferLimitExceeded;
  }

  /**
   * Returns true if the last event has been not been sent because it did not match a
   * filter specification. This also implies that there are no more events in the pipeline
   * to be sent. If some of the events in a batch were dropped by filter, and the last
   * event was sent out, this call will return false This method will return true when @
   * {@link #isError()} also returns true
   */
  public boolean isDropped()
  {
    return _isDropped;
  }

  /**
   * Returns true if the event writer encountered an error while writing the event. The
   * error may be a timeout, or a closing event in the remote channel, or encoding (if
   * any) error, etc. Some events may be sent. Call @{@link #getNumRowsWritten()} to see
   * how many events were sent
   */
  public boolean isError()
  {
    return _isError;
  }

  private final boolean                           _isClientBufferLimitExceeded;
  private final boolean                           _isDropped;
  private final long                              _numRowsWritten;
  private final boolean                           _isError;

  /**
   *
   * Error Case EventProcessing Result factory
   * @param numRowsWritten : Number of Rows written by EventWriters
   * @return BootstrapEventProcessResult
   */
  public static BootstrapEventProcessResult getFailedEventProcessingResult(long numRowsWritten)
  {
    return new BootstrapEventProcessResult(numRowsWritten,false,true,true);
  }

  /**
   *
   * Create a  Bootstrap Event Processing result
   *
   * @param processedRowCount
   *          : Number of rows processed
   * @param isLimitExceeded
   *          : Send buffer limit size exceeded?
   * @param dropped
   *          : Event dropped
   */
  public BootstrapEventProcessResult(long processedRowCount,
                                     boolean isLimitExceeded,
                                     boolean dropped)
  {
    this(processedRowCount, isLimitExceeded, dropped, false);
  }

  /**
   *
   * Create Bootstrap Event Processing result
   *
   * @param processedRowCount
   *          : Number of rows processed
   * @param isLimitExceeded
   *          : Send buffer limit size exceeded?
   * @param dropped
   *          : Event dropped
   * @param error
   *          : Processing failed because of error ?
   */
  public BootstrapEventProcessResult(long processedRowCount,
                                     boolean isLimitExceeded,
                                     boolean dropped,
                                     boolean error)
  {
    _isError = error;
    _isClientBufferLimitExceeded = isLimitExceeded;
    _numRowsWritten = processedRowCount;
    _isDropped = dropped;
  }

  @Override
  public String toString()
  {
    return "BootstrapEventProcessResultImpl [_isClientBufferLimitExceeded="
        + _isClientBufferLimitExceeded + ", _isDropped=" + _isDropped
        + ", _processedRowCount=" + _numRowsWritten + ", _isError=" + _isError + "]";
  }
}
