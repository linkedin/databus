package com.linkedin.databus.core;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * allows to append event to the buffer one by one
 */
public interface DbusEventBufferAppendable
{
  /**
   * Init the accumulator with the starting SCN for this session
   * @param startSCN
   */
  void start(long startSCN);

  /**
   * Notify that the producer will be starting to append events for the next window
   */
  void startEvents();

  /**
   * Append a single event to the Buffer whose sequence id is known in advance.
   * @param key                 the event key
   * @param sequenceId          the event sequence number
   * @param lPartitionId        the event physical partition id
   * @param pPartitionId        the event logical partition id
   * @param timeStamp           the event creation timestamp (in nanoseconds)
   * @param srcId               the event logical source id
   * @param schemaId            the MD5 hash of the event payload schema
   * @param value               the event payload bytes
   * @param enableTracing       a flag if to trace the event flowing through the system
   * @param statsCollector      a statistics collector to update on success (can be null)
   * @return true iff the append succeeded
   */
  boolean appendEvent(DbusEventKey key, long sequenceId, short pPartitionId,
                      short lPartitionId, long timeStamp, short srcId, byte[] schemaId,
                      byte[] value, boolean enableTracing, DbusEventsStatisticsCollector statsCollector);

  /**
   * Append a single event to the Buffer.
   * Safe only for a single-writer thread.
   *
   * @param key                 the event key
   * @param lPartitionId        the event physical partition id
   * @param pPartitionId        the event logical partition id
   * @param timeStamp           the event creation timestamp (in nanoseconds)
   * @param srcId               the event logical source id
   * @param schemaId            the MD5 hash of the event payload schema
   * @param value               the event payload bytes
   * @param enableTracing       a flag if to trace the event flowing through the system
   * @return true iff the append succeeded
   */
  boolean appendEvent(DbusEventKey key, short pPartitionId,
          short lPartitionId, long timeStamp, short srcId, byte[] schemaId,
          byte[] value, boolean enableTracing);

  /**
   * Append a single event.
   * Safe only for a single-writer thread.
   *
   * @param key                 the event key
   * @param lPartitionId        the event physical partition id
   * @param pPartitionId        the event logical partition id
   * @param timeStamp           the event creation timestamp (in nanoseconds)
   * @param srcId               the event logical source id
   * @param schemaId            the MD5 hash of the event payload schema
   * @param value               the event payload bytes
   * @param enableTracing       a flag if to trace the event flowing through the system
   * @param statsCollector      a statistics collector to update on success (can be null)
   * @return true iff the append succeeded
   */
  boolean appendEvent(DbusEventKey key, short pPartitionId,
          short lPartitionId, long timeStamp, short srcId, byte[] schemaId,
          byte[] value, boolean enableTracing,
          DbusEventsStatisticsCollector statsCollector);


  /**
   * Append a single event.
   * Safe only for a single-writer thread.
   * if eventInfo.opCode is set to null - it means use default opCode ('UPSERT' usually).
   *
   * @param key                     the event key
   * @param eventInfo               the event contents
   * @param statsCollector          a statistics collector to update on success (can be null)
   * @return true iff the append succeeded
   */
  boolean appendEvent(DbusEventKey key,
                      DbusEventInfo eventInfo, DbusEventsStatisticsCollector statsCollector);

  /**
   * Rollback last non-ended Event Window
   */
  void rollbackEvents();


  /**
   * Notify the buffer that the current batch of events is complete
   * This method
   * a) appends an End-Of-Period marker to the buffer with the timeStamp provided
   * b) [Optional] Sets the windowScn on all the events in the window
   * c) [Optional] If (b) is done, then updates CRC
   * d) [Optional] Call internal Listeners about all the events in the window
   * e) Moves tail to the end of the window to allow consumption.
   * @param updateWindowScn - update Window Scn with the passed value
   * @param  sequence - sequence id to be used
   * @param updateIndex - update ScnIndex
   * @param callListener - call internal listener callbacks for each event in the window.
   *
   */
  void endEvents(boolean updateWindowScn, long sequence,
          boolean updateIndex, boolean callListener,DbusEventsStatisticsCollector statsCollector);

  /**
   * Notify the buffer that the current batch of events is complete
   * @param  sequence - sequence id to be used
   * @param  timeStampInNanos - timestamp to attach to this event
   */
  void endEvents(long sequence, DbusEventsStatisticsCollector statsCollector);

  /**
   * Check for empty
   * @return true iff the buffer is empry
   */
  boolean empty();

  /**
   * @return minimum scn stored in the accumulator
   */
  long getMinScn();

  /**
   * return last written scn
   * @return last written scn;
   */
  long lastWrittenScn();

  /**
   * set startScn explicitly ; override what was written by start(sinceSCN) ; affects prevScn - DDS-699
   * @param sinceSCN    the new since SCN
   */
  void setStartSCN(long sinceSCN);

  /**
   * Get scn immediately preceding the minScn ;
   * - scn=sinceScn ; offset=-1 is equivalent of flexible checkpoints w.r.t. behaviour of /stream
   * - scn=sinceScn; with offset=0 will yield an scn not found; as data in this scn is no longer contained in the buffer
   *  */
  public long getPrevScn();


}
