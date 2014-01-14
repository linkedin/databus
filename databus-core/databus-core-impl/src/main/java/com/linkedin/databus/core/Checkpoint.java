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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.util.Fnv1aHashImpl;


/**
 * This class represents the state of a consumer consuming events from a Databus server for
 * a single timeline (physical partition). There are two main types of checkpoints:
 *
 * <ul>
 *   <li>Online consumption - used for consuming events a relay
 *   <li>Bootstrap - used for consuming events from a bootstrap server
 * </ul>
 *
 * The type of a checkpoint is determined by the consumptionMode parameter
 * {@link #getConsumptionMode()}.
 *
 * <p><b>Online-Consumption checkpoints</b>
 *
 * Online-consumption checkpoints {@link DbusClientMode#ONLINE_CONSUMPTION} represent the state of
 * a consumer of event stream from a relay. The main properties of the checkpoint are:
 *
 * <ul>
 *  <li> {@code consumption_mode} - must be {@link DbusClientMode#ONLINE_CONSUMPTION}
 *  <li> {@code windowScn}        - the sequence number (SCN) of the current window; -1 denotes
 *                                  "flexible" checkpoint (see below). If the SCN is 0, and tsNescs is greater than 0
 *                                  then the relay may (if capable) stream events that have timestamp greater than
 *                                  or equal to tsNsecs. However, the relay MUST ensure that it does not miss any
 *                                  events that have a timestamp greater than or equal to tsNsecs.
 *                                  TODO: Until we have this capability in the relays we don't have to define the exact behavior
 *  <li> {@code prevScn}          - the sequence number (SCN) of the window before current; -1 means
 *                                  "unknown".
 *  <li> {@code windowOffset}     - the number of events processed from the current window;
 *                                  -1 means the entire window has been processed including the end-of-window event and
 *                                  prevScn must be equal to windowScn.
 *  <li> {@code tsNsecs}          - optional value that is set to the timestamp of the EOP event in the window of
 *                                  events with the highest SCN that has been successfully consumed. If tsNsecs is
 *                                  greater than 0 then the value of windowScn must not be -1 (see discussion on
 *                                  flexible checkpoints below).
 * </ul>
 *
 * <i>Flexible online-consumption checkpoints</i>
 *
 * Used by consumers which do not care from where they start consuming. The relay will make the
 * best effort to serve whatever data it has. Flexible checkpoints can be created using
 * {@link #createFlexibleCheckpoint()} or invoking {@link #setFlexible()} on an existing Checkpoint.
 * If a flexible checkpoint has tsNsecs set, the value of tsNsecs must be -1 (unset value).
 *
 *<p><b>Bootstrap checkpoints</b>
 *
 * These are common fields for snapshot and catchup bootsrap checkpoints (see below).
 *
 * <ul>
 *   <li> {@code bootstrap_since_scn} - the SCN of the last fully-consumed window or 0 for a full bootstrap. Must be >= 0.
 *   <li> {@code bootstrap_start_scn}  - the last SCN written to the snapshot when the snapshot started; must be set
 *                                       before any data is read, i.e. {@code bootstrap_snapshot_source_index} > 0 or
 *                                       {@code snapshot_offset} > 0 or {@code bootstrap_target_scn} != -1
 *   <li> {@code bootstrap_target_scn} - the last SCN written to the log tables when the snapshot of the last source
 *                                       completed. It provides an upper bound of how much dirty data might have been
 *                                       read while snapshotting. It specifies the SCN up to which catch-up should be
 *                                       performed to guarantee consistency of the bootstrap results.
 *   <li> {@code bootstrap_start_tsnsecs}
 *                                     - (optional) the timestamp of the EOP event of the highest window successfully
 *                                       processed by the client before the client fell off the relay. This value
 *                                       is optionally set by the bootstrap client before bootstrapping begins, and
 *                                       is never changed during the entire bootstrap sequence
 *                                       (snapshot and catchup phases).
 * </ul>
 *
 * <p><b>Bootstrap snapshot checkpoints</b>
 *
 * Bootstrap snapshot checkpoints ({@link DbusClientMode#BOOTSTRAP_SNAPSHOT}) represent a consumer in the SNAPSHOT
 * phase of bootstrapping. The main properties of checkpoints are:
 *
 * <ul>
 *   <li> {@code consumption_mode}    - must be {@link DbusClientMode#BOOTSTRAP_SNAPSHOT}
 *   <li> {@code bootstrap_snapshot_source_index} - the index of the current source being snapshotted or the index
 *                                      of the next source if {@code bootstrap_snapshot_offet} == -1
 *   <li> {@code snapshot_source}     - the name of the current source being snapshotted or the name of the next source
 *                                      if {@code bootstrap_snapshot_offet} == -1
 *   <li> {@code bootstrap_snapshot_offset} - number of rows successfully read for the current snapshot source; if -1,
 *                                      all rows have been read
 *   <li> {@code bootstrap_snapshot_file_record_offset} - Applicable for V3 bootstrap only; refers to the offset of the
 *                                       record within the AVRO block
 *   <li> {@code storage_cluster_name} - Applicable for V3 bootstrap only; refers to name of espresso storage cluster
 *
 * </ul>
 *
 * <p><b>Bootstrap catchup checkpoints</b>
 *
 * Bootstrap catchup checkpoints ({@link DbusClientMode#BOOTSTRAP_CATCHUP}) represent a consumer in the CATCHUP
 * phase of bootstrapping. The main properties of checkpoints are:
 *
 * <ul>
 *   <li> {@code consumption_mode}     - must be {@link DbusClientMode#BOOTSTRAP_CATCHUP}
 *   <li> {@code bootstrap_snapshot_source_index} - the index of the last snapshot source
 *   <li> {@code bootstrap_snapshot_offset} - should always be -1
 *   <li> {@code catchup_source}       - the name of the current catch-up source or the name of the next source
 *                                       if {@code windowScn} == -1
 *  <li> {@code windowScn}             - the sequence number (SCN) of the current window being caught-up;
 *  <li> {@code windowOffset}          - the number of events processed from the current window;
 * </ul>
 *
 * @see CheckpointMult for multi-partition checkpoints
 *
 */
public class Checkpoint
	extends InternalDatabusEventsListenerAbstract
	implements Serializable, Cloneable
{
  private static final long serialVersionUID = 1L;
  public static final String  MODULE               = Checkpoint.class.getName();
  public static final Logger  LOG                  = Logger.getLogger(MODULE);

  public static final long UNSET_BOOTSTRAP_START_NSECS = -1;
  public static final long UNSET_TS_NSECS = -1;
  public static final long UNSET_BOOTSTRAP_START_SCN = -1;
  public static final long UNSET_BOOTSTRAP_SINCE_SCN = -1;
  public static final long UNSET_BOOTSTRAP_TARGET_SCN = -1;
  public static final int UNSET_BOOTSTRAP_INDEX = 0;
  public static final long UNSET_ONLINE_PREVSCN = -1;
  /**
   * A checkpoint has the tuple (SCN, Timestamp-of-highest-scn) to indicate the point of successful
   * consumption -- The SCN and timestamp being that of the EOW event consumed successfully.
   * However, it is possible to create a checkpoint (e.g. by the operator as a run-book procedure) that
   * has only a timestamp to indicate the last consumption point, but does not have the corresponding SCN.
   * For now, we restrict these checkpoints to have an SCN of 0 (definitely not -1, since -1 will indicate
   * a 'flexible checkpoint')
   */
  public static final long WINDOW_SCN_FOR_PURE_TIMEBASED_CKPT = 0;
  public static final String NO_SOURCE_NAME = "";

  /** The window offset value for a full-consumed window*/
  public static final Long FULLY_CONSUMED_WINDOW_OFFSET = -1L;
  public static final Long DEFAULT_SNAPSHOT_FILE_RECORD_OFFSET = -1L;


  private static final String TS_NSECS             = "tsNsecs";
  private static final String WINDOW_SCN           = "windowScn";
  // which window scn have we processed completely
  private static final String WINDOW_OFFSET        = "windowOffset";
  // when non-zero: within a window, how many messages have been processed


  /**
   * the last window we have completely processed
   */
  private static final String PREV_SCN           = "prevScn";


  // Bootstrap Checkpoint
  // The checkpoint consists of
  // 1. The phase in the bootstrap (snapshot/catchup)
  // 2. Start SCN - the max. scn of a source (min of bootstrap_applier_state) when bootstrap process is initiated for it
  // 3. Target SCN - the max. scn of a source (bootstrap_producer_state) at the beginning of bootstrap catchup
  // 4. The source we are currently snapshotting
  // 5. The row_offset for the source being snapshotted
  // 6. The source involved for catchup
  // 7. The window scn for the catchup [Similar to regular consumption mode]
  // 8. The window offset for the catchup [Similar to regular consumption mode]
  // 9. Since SCN - the SCN at which bootstrap process is initiated.
  // 10. Bootstrap Server Coordinates
  // 11. (V3 only) Bootstrap server side file record offset.
  //     (i)  snapshot_offset field has the avro block number to seek within the avro file in v3 bootstrap
  //     (ii) snapshot_file_record_offset is used to skip records
  // 12. (V3 only) When storage in on Espresso, this refers to storage cluster name

  private static final String CONSUMPTION_MODE     = "consumption_mode";
  private static final String BOOTSTRAP_START_SCN  = "bootstrap_start_scn";
  private static final String SNAPSHOT_SOURCE      = "snapshot_source";
  private static final String SNAPSHOT_OFFSET      = "snapshot_offset";
  private static final String CATCHUP_SOURCE       = "catchup_source";
  private static final String BOOTSTRAP_TARGET_SCN = "bootstrap_target_scn";
  private static final String BOOTSTRAP_SINCE_SCN  = "bootstrap_since_scn";
  private static final String BOOTSTRAP_SNAPSHOT_SOURCE_INDEX = "bootstrap_snapshot_source_index";
  private static final String BOOTSTRAP_CATCHUP_SOURCE_INDEX = "bootstrap_catchup_source_index";
  public static final String BOOTSTRAP_SERVER_INFO = "bootstrap_server_info";
  public static final String SNAPSHOT_FILE_RECORD_OFFSET = "bootstrap_snapshot_file_record_offset";
  public static final String STORAGE_CLUSTER_NAME = "storage_cluster_name";
  public static final String BOOTSTRAP_START_TSNSECS = "bootstrap_start_tsnsecs";

  private static final ObjectMapper mapper               = new ObjectMapper();
  private final Map<String, Object> internalData;

  private long                currentWindowScn;
  private long                prevWindowScn;
  private long                currentWindowOffset;
  private long                snapShotOffset;
  // TODO ALERT XXX WARNING: Do NOT add any more member variables. See DDSDBUS-3070. It is ok to add to internalData

  @SuppressWarnings("unchecked")
  public Checkpoint(String serializedCheckpoint) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    this();
    internalData.putAll(mapper.readValue(new ByteArrayInputStream(serializedCheckpoint.getBytes(Charset.defaultCharset())),
                                         Map.class));
    // copy from map to local state variables
    mapToInternalState();

  }

  private void mapToInternalState()
  {
    currentWindowScn =  (internalData.get(WINDOW_SCN) != null) ? ((Number) internalData.get(WINDOW_SCN)).longValue() : -1;
    prevWindowScn = (internalData.get(PREV_SCN) != null) ? ((Number) internalData.get(PREV_SCN)).longValue() : -1;
    currentWindowOffset =  (internalData.get(WINDOW_OFFSET) != null) ? ((Number) internalData.get(WINDOW_OFFSET)).longValue() : FULLY_CONSUMED_WINDOW_OFFSET;
    snapShotOffset =  (internalData.get(SNAPSHOT_OFFSET) != null) ? ((Number) internalData.get(SNAPSHOT_OFFSET)).longValue() : -1;
  }

  private void internalStateToMap( )
  {
    internalData.put(WINDOW_SCN, currentWindowScn);
    internalData.put(PREV_SCN, prevWindowScn);
    internalData.put(WINDOW_OFFSET, currentWindowOffset);
    internalData.put(SNAPSHOT_OFFSET, snapShotOffset);
  }

  public Checkpoint()
  {
    internalData = new HashMap<String, Object>();
    init();
  }

  /** Clears the checkpoint. */
  public void init()
  {
	  currentWindowScn = -1L;
	  prevWindowScn = -1L;
	  currentWindowOffset = FULLY_CONSUMED_WINDOW_OFFSET;
	  snapShotOffset = -1;
	  internalData.clear();
	  setConsumptionMode(DbusClientMode.INIT);
  }

  public void setTsNsecs(long nsecs)
  {
    internalData.put(TS_NSECS, Long.valueOf(nsecs));
  }

  public long getTsNsecs()
  {
    return number2Long((Number)internalData.get(TS_NSECS), UNSET_TS_NSECS);
  }

  public void setBootstrapStartNsecs(long nsecs)
  {
    internalData.put(BOOTSTRAP_START_TSNSECS, Long.valueOf(nsecs));
  }

  public long getBootstrapStartNsecs()
  {
    return number2Long((Number)internalData.get(BOOTSTRAP_START_TSNSECS), UNSET_BOOTSTRAP_START_NSECS);
  }

  public void setBootstrapSnapshotSourceIndex(int index)
  {
	  internalData.put(BOOTSTRAP_SNAPSHOT_SOURCE_INDEX, index);
  }

  public void setBootstrapCatchupSourceIndex(int index)
  {
	  internalData.put(BOOTSTRAP_CATCHUP_SOURCE_INDEX, index);
  }

  int nextBootstrapSnapshotSourceIndex()
  {
	  int index = getBootstrapSnapshotSourceIndex();
	  return index + 1;
  }

  int nextBootstrapCatchupSourceIndex()
  {
	  int index = getBootstrapCatchupSourceIndex();
	  return index + 1;
  }

  public void setBootstrapServerInfo(String serverInfoStr)
  {
	  internalData.put(BOOTSTRAP_SERVER_INFO, serverInfoStr);
  }

  public String getBootstrapServerInfo()
  {
	  Object obj = internalData.get(BOOTSTRAP_SERVER_INFO);
	  if ( null == obj)
		  return null;

	  return (String)obj;
  }

  public void setWindowScn(Long windowScn)
  {
    if (DbusClientMode.BOOTSTRAP_CATCHUP == getConsumptionMode() && !isBootstrapTargetScnSet())
    {
      throw new InvalidCheckpointException("target SCN must be set for catchup to proceed", this);
    }
     currentWindowScn = windowScn;
  }

  public void setPrevScn(Long windowScn)
  {
     prevWindowScn = windowScn;
  }

  public long getPrevScn()
  {
	  return prevWindowScn;
  }

  public void setWindowOffset(long windowOffset)
  {
    if (DbusClientMode.BOOTSTRAP_CATCHUP == getConsumptionMode() && !isBootstrapTargetScnSet())
    {
      throw new InvalidCheckpointException("target SCN must be set for catchup to proceed", this);
    }
    currentWindowOffset = windowOffset;
  }

  @Deprecated
  /** @deprecated Please use {@link #setWindowOffset(long)} */
  public void setWindowOffset(Integer windowOffset)
  {
    currentWindowOffset = windowOffset.longValue();
  }

  public void setConsumptionMode(DbusClientMode mode)
  {
    internalData.put(CONSUMPTION_MODE, mode.toString());
  }

  public void setBootstrapStartScn(Long bootstrapStartScn)
  {
    if (isBootstrapStartScnSet() && bootstrapStartScn.longValue() != UNSET_BOOTSTRAP_START_SCN)
    {
      throw new InvalidCheckpointException("bootstrap_start_scn is already set", this);
    }
    if (bootstrapStartScn.longValue() != UNSET_BOOTSTRAP_START_SCN &&
        DbusClientMode.BOOTSTRAP_SNAPSHOT != getConsumptionMode())
    {
      throw new InvalidCheckpointException("not in bootstrap snapshot mode", this);
    }
    if (bootstrapStartScn.longValue() != UNSET_BOOTSTRAP_START_SCN &&
        bootstrapStartScn.longValue() < 0)
    {
      throw new InvalidCheckpointException("invalid bootstra_start_scn value:" + bootstrapStartScn, this);
    }
    internalData.put(BOOTSTRAP_START_SCN, bootstrapStartScn);
  }

  public void setSnapshotSource(int sourceIndex, String sourceName)
  {
    internalData.put(SNAPSHOT_SOURCE, sourceName);
    setBootstrapSnapshotSourceIndex(sourceIndex);
  }

  public void setSnapshotOffset(long snapshotOffset)
  {
    if (snapshotOffset != 0 && !isBootstrapStartScnSet())
    {
      throw new InvalidCheckpointException("cannot snapshot without bootstrap_start_scn", this);
    }
    internalData.put(SNAPSHOT_OFFSET, Long.valueOf(snapshotOffset));
    this.snapShotOffset = snapshotOffset;
  }

  protected void clearSnapshotOffset()
  {
    internalData.put(SNAPSHOT_OFFSET, FULLY_CONSUMED_WINDOW_OFFSET);
    this.snapShotOffset = FULLY_CONSUMED_WINDOW_OFFSET;
  }

  @Deprecated
  /** @deprecated Please use #setSnapshotOffset(long) */
  public void setSnapshotOffset(Integer snapshotOffset)
  {
    internalData.put(SNAPSHOT_OFFSET, Long.valueOf(snapshotOffset));
  }

  protected void setCatchupSource(int sourceIndex, String sourceName)
  {
    setBootstrapCatchupSourceIndex(sourceIndex);
    internalData.put(CATCHUP_SOURCE, sourceName);
  }

  public void setCatchupOffset(Integer catchupOffset)
  {
    setWindowOffset(catchupOffset.longValue());
    // There is no separate field called CATCHUP_OFFSET in checkpoint.
    // WINDOW_OFFSET is used to store the catchupOffset
    internalData.put(WINDOW_OFFSET, catchupOffset);
  }

  public void setBootstrapTargetScn(Long targetScn)
  {
    if (UNSET_BOOTSTRAP_TARGET_SCN != targetScn.longValue())
    {
      if (targetScn < getBootstrapStartScn())
      {
        throw new InvalidCheckpointException("bootstrap_target_scn cannot be smaller than bootstrap_start_scn", this);
      }
      if (!isSnapShotSourceCompleted())
      {
        throw new InvalidCheckpointException("snapshot should be complete before setting bootstrap_target_scn", this);
      }
    }
    internalData.put(BOOTSTRAP_TARGET_SCN, targetScn);
  }

  public void setBootstrapSinceScn(Long sinceScn)
  {
    internalData.put(BOOTSTRAP_SINCE_SCN, sinceScn);
  }

  public long getWindowScn()
  {
    return currentWindowScn;

  }

  public Long getWindowOffset()
  {
    return currentWindowOffset;

  }

  public DbusClientMode getConsumptionMode()
  {
    return (DbusClientMode.valueOf((String) internalData.get(CONSUMPTION_MODE)));
  }

  public String getSnapshotSource()
  {
    return (String) internalData.get(SNAPSHOT_SOURCE);
  }

  public long getSnapshotFileRecordOffset()
  {
    return number2Long((Number)internalData.get(SNAPSHOT_FILE_RECORD_OFFSET),
        DEFAULT_SNAPSHOT_FILE_RECORD_OFFSET);
  }

  public void setSnapshotFileRecordOffset(long snapshotFileRecordOffset)
  {
    internalData.put(SNAPSHOT_FILE_RECORD_OFFSET, snapshotFileRecordOffset);
  }

  public String getStorageClusterName()
  {
    return (String) internalData.get(STORAGE_CLUSTER_NAME);
  }

  public void setStorageClusterName(String storageClusterName)
  {
    internalData.put(STORAGE_CLUSTER_NAME, storageClusterName);
  }

  private static Long number2Long(Number n, Long nullValue)
  {
    return (null == n) ? nullValue :  (n instanceof Long) ? (Long)n : n.longValue();
  }

  private static Integer number2Integer(Number n, Integer nullValue)
  {
    return (null == n) ? nullValue :  (n instanceof Integer) ? (Integer)n : n.intValue();
  }

  public Long getSnapshotOffset()
  {
    return number2Long((Number)internalData.get(SNAPSHOT_OFFSET), FULLY_CONSUMED_WINDOW_OFFSET);
  }

  public String getCatchupSource()
  {
    return (String) internalData.get(CATCHUP_SOURCE);
  }

  public Long getBootstrapStartScn()
  {
      Number n = ((Number) internalData.get(BOOTSTRAP_START_SCN));
      return number2Long(n, UNSET_BOOTSTRAP_START_SCN);
  }

  public Long getBootstrapTargetScn()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_TARGET_SCN));
	  return number2Long(n, UNSET_BOOTSTRAP_TARGET_SCN);
  }

  public Integer getBootstrapSnapshotSourceIndex()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_SNAPSHOT_SOURCE_INDEX));
	  return number2Integer(n, UNSET_BOOTSTRAP_INDEX);
  }

  public Integer getBootstrapCatchupSourceIndex()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_CATCHUP_SOURCE_INDEX));
      return number2Integer(n, UNSET_BOOTSTRAP_INDEX);
  }

  public Long getBootstrapSinceScn()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_SINCE_SCN));
      return number2Long(n, UNSET_BOOTSTRAP_SINCE_SCN);
  }

  // TODO Deprecate and remove this method. See DDSDBUS-3070.
  // See toString()
  public void serialize(OutputStream outStream) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    internalStateToMap();
    mapper.writeValue(outStream, internalData);

  }

  // This is the method used by databus components to "serialize" a checkpoint for on-the-wire
  // transmission.
  @Override
  public String toString()
  {

    internalStateToMap();
    try
    {
      return (mapper.writeValueAsString(internalData));
    }
    catch (JsonGenerationException e)
    {
      LOG.error("JSON generation error: " + e.getMessage(), e);
      return ("JsonGenerationException while printing Checkpoint.");
    }
    catch (JsonMappingException e)
    {
      LOG.error("JSON mapping error: " + e.getMessage(), e);
      return ("JsonMappingException while printing Checkpoint.");
    }
    catch (IOException e)
    {
      LOG.error("JSON IO error: " + e.getMessage(), e);
      return ("IOException while printing Checkpoint.");
    }
  }

  public void startEvent()
  {

  }

  @Override
  public void onEvent(DbusEvent e, long offset, int size)
  {
    // Checkpoint doesn't use the offset in the buffer for anything (yet)
    onEvent(e);
  }

  public void onEvent(DbusEvent e)
  {
    if (e.isEndOfPeriodMarker())
    {
      prevWindowScn = e.sequence();
      endEvents(e.sequence(), e.timestampInNanos());
    }
    else if (e.isCheckpointMessage())
    {
      Checkpoint ckpt = null;
      try
      {
        ByteBuffer tmpBuffer = e.value();
        byte[] valueBytes = new byte[tmpBuffer.limit()];
        tmpBuffer.get(valueBytes);
        ckpt = new Checkpoint(new String(valueBytes, "UTF-8"));

        switch (this.getConsumptionMode())
        {
          case BOOTSTRAP_SNAPSHOT:
               copyBootstrapSnapshotCheckpoint(ckpt);
               break;
          case BOOTSTRAP_CATCHUP:
               copyBootstrapCatchupCheckpoint(ckpt);
               break;
          case ONLINE_CONSUMPTION:
               copyOnlineCheckpoint(ckpt);
               break;
          default:
             throw new RuntimeException("Invalid checkpoint message received: " + this);
        }
      }
      catch (Exception exception)
      {
        LOG.error("Exception encountered while reading checkpiont from bootstrap service",
                  exception);
      }
      finally
      {
        if (null != ckpt) ckpt.close();
      }
    }
    else // regular dbusEvent
    {
      if (currentWindowScn == e.sequence())
      {
        ++currentWindowOffset;
      }
      else
      {
        currentWindowScn = e.sequence();
        currentWindowOffset = 1L;
      }
    }

    if (LOG.isDebugEnabled())
      LOG.info("CurrentWindowSCN : " + currentWindowScn
          + ", currentWindowOffset :" + currentWindowOffset
          + ", PrevSCN :" + prevWindowScn);
  }

  /** Copy data about bootstrap catchup consumption from another checkpoint */
  protected void copyBootstrapCatchupCheckpoint(Checkpoint ckpt)
  {
    setWindowScn(ckpt.getWindowScn());
    setWindowOffset(ckpt.getWindowOffset());
    //setCatchupSource(ckpt.getCatchupSource());
    //setBootstrapCatchupSourceIndex(ckpt.getBootstrapCatchupSourceIndex());

    // Update file record offset. The storage_cluster_name is not updated, as it
    // is meant to be an invariant, once set
    setSnapshotFileRecordOffset(ckpt.getSnapshotFileRecordOffset());
  }

  /** Copy data about bootstrap snapshot consumption from another checkpoint
   *  TODO : This seems to be used only on the eventBuffer and on client side,
   *  the lastCheckpoint is saved and then this method is invoked on itself.
   *  This seems to be a no-op
   */
  protected void copyBootstrapSnapshotCheckpoint(Checkpoint ckpt)
  {
    setSnapshotOffset(ckpt.getSnapshotOffset());
    setSnapshotSource(ckpt.getBootstrapSnapshotSourceIndex(), ckpt.getSnapshotSource());
    //setBootstrapSnapshotSourceIndex(ckpt.getBootstrapSnapshotSourceIndex());

    // Update file record offset. The storage_cluster_name is not updated, as it
    // is meant to be an invariant, once set
    setSnapshotFileRecordOffset(ckpt.getSnapshotFileRecordOffset());
  }

  /** Copy data about online consumption from another checkpoint */
  private void copyOnlineCheckpoint(Checkpoint fromCkpt)
  {
    setWindowScn(fromCkpt.getWindowScn());
    setWindowOffset(fromCkpt.getWindowOffset());
  }

  private void endEvents(long endWindowScn, long nsecs)
  {
    setFullyConsumed(endWindowScn);
    setTsNsecs(nsecs);
  }

  private void setFullyConsumed(long endWindowScn)
  {
    currentWindowOffset = FULLY_CONSUMED_WINDOW_OFFSET;
    this.clearWindowOffset();
    this.setWindowScn(endWindowScn);
  }

  public void onSnapshotEvent(long snapshotOffset)
  {
    snapShotOffset = snapshotOffset;
  }

  public void onCatchupEvent(long eventWindowScn, long catchupOffset)
  {
    currentWindowScn = eventWindowScn;
    currentWindowOffset = catchupOffset;
  }

  public void startSnapShotSource()
  {
    setSnapshotOffset(0);
  }

  public void endSnapShotSource()
  {
    this.setSnapshotOffset(-1);
  }

  public boolean isSnapShotSourceCompleted()
  {
    return ((this.getSnapshotOffset() == -1) ? true : false);
  }

  public void startCatchupSource()
  {
    setWindowOffset(0);
    setWindowScn(getBootstrapStartScn());
  }

  public void endCatchupSource()
  {
    setFullyConsumed(currentWindowScn);
    this.setWindowOffset(FULLY_CONSUMED_WINDOW_OFFSET);
  }


  public boolean isCatchupSourceCompleted()
  {
    return (this.getWindowOffset() == FULLY_CONSUMED_WINDOW_OFFSET);
  }

  public void bootstrapCheckPoint()
  {
    if (this.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP)
    {
      this.setWindowOffset(currentWindowOffset);
      this.setWindowScn(currentWindowScn);
    }
    else if (this.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
    {
      this.setSnapshotOffset(snapShotOffset);
    }
  }

  private void clearWindowOffset()
  {
    internalData.remove(WINDOW_OFFSET);
  }

  public void checkPoint()
  {
    if (currentWindowScn >= 0)
    {
      this.setWindowScn(currentWindowScn);
    }
    if (currentWindowOffset >= 0)
    {
      this.setWindowOffset(currentWindowOffset);
    }
  }

  public boolean isPartialWindow()
  {
    return currentWindowOffset >= 0;
  }

  /** @deprecated Please use {@link Checkpoint#init()}*/
  @Deprecated
  public void setInit()
  {
    setConsumptionMode(DbusClientMode.INIT);
  }

  /** Checks if the checkpoint is in initialized state, i.e. empty. */
  public boolean getInit()
  {
    return (getConsumptionMode() == DbusClientMode.INIT);
  }

  /** Converts a checkpoint to a flexible online-consumption checkpoint. */
  public void setFlexible()
  {
    setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    setWindowScn(-1L);
    setTsNsecs(UNSET_TS_NSECS);
  }

  public boolean getFlexible()
  {
    if ((getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION)
        && (getWindowScn() < 0) && getTsNsecs() == UNSET_TS_NSECS)
    {
      return true;
    }
    else
    {
      return false;
    }
  }

  public void clearBootstrapStartTsNsecs()
  {
    setBootstrapStartNsecs(UNSET_BOOTSTRAP_START_NSECS);
  }

  public void clearBootstrapSinceScn()
  {
    setBootstrapSinceScn(Long.valueOf(UNSET_BOOTSTRAP_SINCE_SCN));
  }

  public void clearBootstrapStartScn()
  {
    setBootstrapStartScn(Long.valueOf(UNSET_BOOTSTRAP_START_SCN));
  }

  public void clearBootstrapTargetScn()
  {
    setBootstrapTargetScn(Long.valueOf(UNSET_BOOTSTRAP_TARGET_SCN));
  }

  public boolean isBootstrapStartScnSet()
  {
    return (null != getBootstrapStartScn() &&
            UNSET_BOOTSTRAP_START_SCN != getBootstrapStartScn().longValue());
  }

  public boolean isBootstrapTargetScnSet()
  {
    return (null != getBootstrapTargetScn() &&
           UNSET_BOOTSTRAP_TARGET_SCN != getBootstrapTargetScn().longValue());
  }

  public boolean isBootstrapSinceScnSet()
  {
    return (null != getBootstrapSinceScn() &&
            UNSET_BOOTSTRAP_SINCE_SCN != getBootstrapSinceScn().longValue());
  }

  /*
   * reset bootstrap specific values in the checkpoint
   */
  public void resetBootstrap()
  {
	  clearBootstrapSinceScn();
	  clearSnapshotOffset();
	  setWindowOffset(FULLY_CONSUMED_WINDOW_OFFSET);
	  clearBootstrapStartScn();
	  clearBootstrapTargetScn();
	  setBootstrapSnapshotSourceIndex(UNSET_BOOTSTRAP_INDEX);
	  setBootstrapCatchupSourceIndex(UNSET_BOOTSTRAP_INDEX);
	  setBootstrapServerInfo(null);
	  setSnapshotFileRecordOffset(DEFAULT_SNAPSHOT_FILE_RECORD_OFFSET);
	  setStorageClusterName("");
    clearBootstrapStartTsNsecs();
  }

  /**
   * Resets the bootstrap checkpoint to consume events from a new bootstrap server
   * This method must be invoked on the client whenever a connection is made to a
   * bootstrap server that is different from the one serving so far.
   */
  protected void resetForServerChange()
  {
    setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    setSnapshotOffset(0L);
    setWindowOffset(FULLY_CONSUMED_WINDOW_OFFSET);
    setWindowScn(getBootstrapSinceScn());
    clearBootstrapStartScn();
    clearBootstrapTargetScn();
    setBootstrapSnapshotSourceIndex(UNSET_BOOTSTRAP_INDEX);
    setBootstrapCatchupSourceIndex(UNSET_BOOTSTRAP_INDEX);
    setBootstrapServerInfo(null);
    setSnapshotFileRecordOffset(DEFAULT_SNAPSHOT_FILE_RECORD_OFFSET);
    setStorageClusterName("");
  }

  /** Remove IOException javac warnings */
  @Override
  public void close()
  {
  }

  @Override
  public Checkpoint clone()
  {
    Checkpoint ckpt = new Checkpoint();
    ckpt.currentWindowOffset = currentWindowOffset;
    ckpt.currentWindowScn = currentWindowScn;
    ckpt.prevWindowScn = prevWindowScn;
    ckpt.snapShotOffset = snapShotOffset;
    for (Map.Entry<String, Object> srcEntry: internalData.entrySet())
    {
      ckpt.internalData.put(srcEntry.getKey(), srcEntry.getValue());
    }

    return ckpt;
  }

  /* Helper factory methods */

  /**
   * Creates a time-based checkpoint.
   *
   * A very nice API to have for the clients, when we provide the use case for a registration to
   * start receiving relay events X hours before registration time (i,e. neither from the beginning of
   * buffer, nor from latest point).
  public static Checkpoint createTimeBasedCheckpoint(long nsecs)
  throws DatabusRuntimeException
  {
    if (nsecs <= UNSET_TS_NSECS)
    {
      throw new DatabusRuntimeException("Invalid value for timestamp:" + nsecs);
    }
    Checkpoint cp = new Checkpoint();
    cp.setTsNsecs(nsecs);
    cp.setWindowScn(WINDOW_SCN_FOR_PURE_TIMEBASED_CKPT);
    return cp;
  }
   */

  /**
   * Creates a flexible online-consumption checkpoint.
   * @return the new checkpoint
   */
  public static Checkpoint createFlexibleCheckpoint()
  {
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    return cp;
  }

  /**
   * Creates a simple online-consumption checkpoint for a given SCN.
   * @param lastConsumedScn    the sequence number of the last fully consumed window
   * @return the new checkpoint
   */
  public static Checkpoint createOnlineConsumptionCheckpoint(long lastConsumedScn)
  {
    if (lastConsumedScn < 0)
    {
      throw new InvalidCheckpointException("scn must be non-negative: " + lastConsumedScn, null);
    }

    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp.setWindowScn(lastConsumedScn);
    cp.setPrevScn(lastConsumedScn);
    cp.setWindowOffset(FULLY_CONSUMED_WINDOW_OFFSET);

    return cp;
  }

  /**
   * Creates an online checkpoint with timestamp and SCN. See DDSDBUS-3332
   * @param lastConsumedScn the sequence number of the last fully consumed window
   * @param tsNanos the timestamp, if available, of the last fully consumed window.
   */
  public static Checkpoint createOnlineConsumptionCheckpoint(long lastConsumedScn, long tsNanos)
  {
    Checkpoint cp = createOnlineConsumptionCheckpoint(lastConsumedScn);
    cp.setTsNsecs(tsNanos);
    return cp;
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other) return false;
    if (this == other) return true;
    if (!(other instanceof Checkpoint)) return false;
    Checkpoint otherCp = (Checkpoint)other;

    boolean success = (currentWindowScn == otherCp.currentWindowScn &&
                       prevWindowScn == otherCp.prevWindowScn &&
                       currentWindowOffset == otherCp.currentWindowOffset &&
                       snapShotOffset == otherCp.getSnapshotOffset());
    if (success)
    {
      //Unfortunately, we cannot use the the Map.equals() method.
      //If a checkpoint is deserialized from a string, the ObjectMapper may create Integer objects for some fields while
      //the other checkpoint may have Longs. For java, Integer(-1) != Long(-1). Go figure.
      for (Map.Entry<String, Object> e: internalData.entrySet())
      {
        String k = e.getKey();
        Object v = e.getValue();
        Object otherV = otherCp.internalData.get(k);
        if (v instanceof Number)
        {
          success = (otherV instanceof Number) && (((Number) v).longValue() == ((Number)otherV).longValue());
        }
        else
        {
          success = v.equals(otherV);
        }
        if (!success) break;
      }
    }
    return success;
  }

  @Override
  public int hashCode()
  {
    long lhash = Fnv1aHashImpl.init32();
    final DbusClientMode mode = getConsumptionMode();
    lhash = Fnv1aHashImpl.addInt32(lhash, mode.ordinal());
    lhash = Fnv1aHashImpl.addLong32(lhash, currentWindowScn);
    lhash = Fnv1aHashImpl.addLong32(lhash, prevWindowScn);
    lhash = Fnv1aHashImpl.addLong32(lhash, currentWindowOffset);
    lhash = Fnv1aHashImpl.addLong32(lhash, getTsNsecs());
    if (DbusClientMode.BOOTSTRAP_CATCHUP == mode || DbusClientMode.BOOTSTRAP_SNAPSHOT == mode)
    {
      lhash = Fnv1aHashImpl.addLong32(lhash, snapShotOffset);
      lhash = Fnv1aHashImpl.addLong32(lhash, getBootstrapSinceScn().longValue());
      lhash = Fnv1aHashImpl.addLong32(lhash, getBootstrapStartScn().longValue());
      lhash = Fnv1aHashImpl.addLong32(lhash, getBootstrapTargetScn().longValue());
      lhash = Fnv1aHashImpl.addLong32(lhash, getBootstrapCatchupSourceIndex());
      lhash = Fnv1aHashImpl.addLong32(lhash, getBootstrapSnapshotSourceIndex());
      lhash = Fnv1aHashImpl.addLong32(lhash, getSnapshotFileRecordOffset());
      lhash = Fnv1aHashImpl.addLong32(lhash, getBootstrapStartNsecs());
    }

    return Fnv1aHashImpl.getHash32(lhash);
  }

  /**
   * Checks invariants for a checkpoint.
   * @return true; this is so one can write "assert assertCheckpoint()" if they want control if the assert is to be run
   * @throws InvalidCheckpointException if the validation fails
   */
  public boolean assertCheckpoint()
  {
    switch (getConsumptionMode())
    {
    case INIT: return true;
    case ONLINE_CONSUMPTION: return assertOnlineCheckpoint();
    case BOOTSTRAP_SNAPSHOT: return assertSnapshotCheckpoint();
    case BOOTSTRAP_CATCHUP: return assertCatchupCheckpoint();
    default:
      throw new InvalidCheckpointException("unknown checkpoint type", this);
    }
  }

  private boolean assertCatchupCheckpoint()
  {
    assertCatchupSourceIndex();
    if (! isBootstrapSinceScnSet())
    {
      throw new InvalidCheckpointException("bootstrap_since_scn must be set", this);
    }
    if (! isBootstrapStartScnSet())
    {
      throw new InvalidCheckpointException("bootstrap_start_scn must be set", this);
    }
    if (! isBootstrapTargetScnSet())
    {
      throw new InvalidCheckpointException("bootstrap_target_scn must be set", this);
    }
    if (! isSnapShotSourceCompleted())
    {
      throw new InvalidCheckpointException("bootstrap_snapshot_offset must be -1 for CATCHUP checkpoints", this);
    }
    if (getBootstrapTargetScn() < getBootstrapStartScn())
    {
      throw new InvalidCheckpointException("bootstrap_target_scn < getbootstrap_start_scn", this);
    }
    // If offset is set, then clusterName cannot be empty
    if (getSnapshotFileRecordOffset() != DEFAULT_SNAPSHOT_FILE_RECORD_OFFSET)
    {
      if (getStorageClusterName().isEmpty())
      {
        throw new InvalidCheckpointException("snapshot file record offset cannot be set when storage cluster name is empty", this);
      }
    }

    return true;
  }

  private boolean assertSnapshotCheckpoint()
  {
    if (0 != getBootstrapCatchupSourceIndex())
    {
      throw new InvalidCheckpointException("bootstrap_catchup_source_index must be 0", this);
    }
    if (! isBootstrapSinceScnSet())
    {
      throw new InvalidCheckpointException("bootstrap_since_scn must be set", this);
    }
    if (! isBootstrapStartScnSet())
    {
      //we allow bootstrap_start_scn not to be set only in the beginning of the bootstrap before any
      //data has been read
      if (0 != getBootstrapSnapshotSourceIndex())
      {
        throw new InvalidCheckpointException("bootstrap_snapshot_source_index must be 0 when bootstrap_start_scn is not set",
                                             this);
      }
      if (0 != getSnapshotOffset())
      {
        throw new InvalidCheckpointException("snapshot_offset must be 0 when bootstrap_start_scn is not set", this);
      }
      if (isBootstrapTargetScnSet())
      {
        throw new InvalidCheckpointException("bootstrap_target_scn cannot be set when bootstrap_start_scn is not set",
                                             this);
      }
    }
    // If offset is set, then clusterName cannot be empty
    if (getSnapshotFileRecordOffset() != DEFAULT_SNAPSHOT_FILE_RECORD_OFFSET)
    {
      if (getStorageClusterName().isEmpty())
      {
        throw new InvalidCheckpointException("snapshot file record offset cannot be set when storage cluster name is empty", this);
      }
    }

    return true;
  }

  private boolean assertOnlineCheckpoint()
  {
    if (getFlexible())
    {
      long tsNsecs = getTsNsecs();
      // tsNsecs should be unset.
      if (tsNsecs != UNSET_TS_NSECS)
      {
        throw new InvalidCheckpointException("unexpected tsNsecs:" + tsNsecs, this);
      }
      return true;
    }
    if (getWindowScn() < 0)
    {
      throw new InvalidCheckpointException("unexpected windowScn: " + getWindowScn(), this);
    }
    final long ofs = getWindowOffset();
    if (ofs < 0 && FULLY_CONSUMED_WINDOW_OFFSET != ofs)
    {
      throw new InvalidCheckpointException("unexpected windowOfs: " + getWindowOffset(), this);
    }
    if (FULLY_CONSUMED_WINDOW_OFFSET == ofs && UNSET_ONLINE_PREVSCN != getPrevScn() && getPrevScn() != getWindowScn())
    {
      throw new InvalidCheckpointException("prevScn != windowScn for a fully consumed window ", this);
    }
    if (getPrevScn() > getWindowScn())
    {
      throw new InvalidCheckpointException("prevScn > windowScn", this);
    }
    return true;
  }

  private void assertCatchupSourceIndex()
  {
    final int catchupSourceIndex = getBootstrapCatchupSourceIndex();
    final int snapshotSourceIndex = getBootstrapSnapshotSourceIndex();
    if (0 > catchupSourceIndex || catchupSourceIndex > snapshotSourceIndex)
    {
      throw new InvalidCheckpointException("invalid catchup source index for using sources ", this);
    }
  }

}
