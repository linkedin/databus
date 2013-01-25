package com.linkedin.databus.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

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
 * <b>Online-Consumption checkpoints<b>
 *
 * Online-consumption checkpoints ({@link DbusClientMode#ONLINE_CONSUMPTION} represent the state of
 * a consumer of event stream from a relay. The main properties of the checkpoint are:
 *
 * <ul>
 *  <li> {@code windowScn} - the sequence number (SCN) of the current window; -1 denotes
 *  "flexible" checkpoint (see below).
 *  <li> {@code prevScn} - the sequence number (SCN) of the window before current; -1 means
 *  "unknown".
 *  <li> {@code windowOffset} - the number of events processed from the current window;
 *  -1 means the entire window has been processed including the end-of-window event.
 * </ul>
 *
 * <i>Flexible online-consumption checkpoints<i>
 *
 * Used by consumers which do not care from where they start consuming. The relay will make the
 * best effort to serve whatever data it has. Flexible checkpoints can be created using
 * {@link #createFlexibleCheckpoint()} or invoking {@link #setFlexible()} on an existing Checkpoint.
 *
 * <b>Bootstrap checkpoints</b>
 *
 * @see CheckpointMult for multi-partition checkpoints
 *
 */
public class Checkpoint
	extends InternalDatabusEventsListenerAbstract
	implements Serializable
{
  private static final long serialVersionUID = 1L;
  public static final String  MODULE               = Checkpoint.class.getName();
  public static final Logger  LOG                  = Logger.getLogger(MODULE);

  public static final long INVALID_BOOTSTRAP_START_SCN = -1;
  public static final long INVALID_BOOTSTRAP_SINCE_SCN = 0;
  public static final long INVALID_BOOTSTRAP_TARGET_SCN = -1;
  public static final int INVALID_BOOTSTRAP_INDEX = 0;


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
  // 10. Boostrap Server Coordinates

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

  private static final String FLEXIBLE             = "flexible";

  private static ObjectMapper mapper               = new ObjectMapper();
  private final Map<String, Object> internalData;

  private long                currentWindowScn;
  private long                prevWindowScn;
  private long                currentWindowOffset;
  private long                snapShotOffset;


  private DbusClientMode      mode;

  @SuppressWarnings("unchecked")
  public Checkpoint(String serializedCheckpoint) throws JsonParseException,
      JsonMappingException,
      IOException
  {

    internalData =
        mapper.readValue(new ByteArrayInputStream(serializedCheckpoint.getBytes()),
                         Map.class);
    // copy from map to local state variables
    mapToInternalState();

  }

  private void mapToInternalState()
  {
    currentWindowScn =  (internalData.get(WINDOW_SCN) != null) ? ((Number) internalData.get(WINDOW_SCN)).longValue() : -1;
    prevWindowScn = (internalData.get(PREV_SCN) != null) ? ((Number) internalData.get(PREV_SCN)).longValue() : -1;
    currentWindowOffset =  (internalData.get(WINDOW_OFFSET) != null) ? ((Number) internalData.get(WINDOW_OFFSET)).longValue() : -1;
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
	  currentWindowOffset = -1;
	  snapShotOffset = -1;
	  internalData.clear();
	  setConsumptionMode(DbusClientMode.INIT);
  }

  public void setBootstrapSnapshotSourceIndex(int index)
  {
	  internalData.put(BOOTSTRAP_SNAPSHOT_SOURCE_INDEX, index);
  }

  public void setBootstrapCatchupSourceIndex(int index)
  {
	  internalData.put(BOOTSTRAP_CATCHUP_SOURCE_INDEX, index);
  }

  public void incrementBootstrapSnapshotSourceIndex()
  {
	  int index = getBootstrapSnapshotSourceIndex();
	  internalData.put(BOOTSTRAP_SNAPSHOT_SOURCE_INDEX, index + 1);
  }

  public void incrementBootstrapCatchupSourceIndex()
  {
	  int index = getBootstrapCatchupSourceIndex();
	  internalData.put(BOOTSTRAP_CATCHUP_SOURCE_INDEX, index + 1);
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
    currentWindowOffset = windowOffset;
  }

  public void setConsumptionMode(DbusClientMode mode)
  {
    internalData.put(CONSUMPTION_MODE, mode.toString());
  }

  public void setBootstrapStartScn(Long bootstrapStartScn)
  {
    internalData.put(BOOTSTRAP_START_SCN, bootstrapStartScn);
  }

  public void setSnapshotSource(String snapshotSource)
  {
    internalData.put(SNAPSHOT_SOURCE, snapshotSource);
  }

  public void setSnapshotOffset(long snapshotOffset)
  {
    internalData.put(SNAPSHOT_OFFSET, Long.valueOf(snapshotOffset));
  }

  public void setCatchupSource(String catchupSource)
  {
    internalData.put(CATCHUP_SOURCE, catchupSource);
  }

  public void setCatchupOffset(Integer catchupOffset)
  {
    setWindowOffset(catchupOffset);
  }

  public void setBootstrapTargetScn(Long targetScn)
  {
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

  public Long getBootstrapStartScn()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_START_SCN));

	  if ( null == n)
		  return INVALID_BOOTSTRAP_START_SCN;

	  return n.longValue();
  }

  public String getSnapshotSource()
  {
    return (String) internalData.get(SNAPSHOT_SOURCE);
  }

  public Long getSnapshotOffset()
  {
    return ((Number)internalData.get(SNAPSHOT_OFFSET)).longValue();
  }

  public String getCatchupSource()
  {
    return (String) internalData.get(CATCHUP_SOURCE);
  }

  public Long getBootstrapTargetScn()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_TARGET_SCN));

	  if ( null == n )
		 return INVALID_BOOTSTRAP_TARGET_SCN;

	  return n.longValue();
  }

  public Integer getBootstrapSnapshotSourceIndex()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_SNAPSHOT_SOURCE_INDEX));

	  if ( null == n )
		 return INVALID_BOOTSTRAP_INDEX;

	  return n.intValue();
  }

  public Integer getBootstrapCatchupSourceIndex()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_CATCHUP_SOURCE_INDEX));

	  if ( null == n )
		 return INVALID_BOOTSTRAP_INDEX;

	  return n.intValue();
  }

  public Long getBootstrapSinceScn()
  {
	  Number n = ((Number) internalData.get(BOOTSTRAP_SINCE_SCN));

	  if ( null == n )
		 return INVALID_BOOTSTRAP_SINCE_SCN;

	  return n.longValue();
  }

  public void serialize(OutputStream outStream) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    internalStateToMap();
    mapper.writeValue(outStream, internalData);

  }

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
  public void onEvent(DataChangeEvent e, long offset, int size)
  {
    // Checkpoint doesn't use the offset in the buffer for anything (yet)
    onEvent(e);
  }

  public void onEvent(DataChangeEvent e)
  {
    if (e.isEndOfPeriodMarker())
    {
      prevWindowScn = e.sequence();
      endEvents(e.sequence());
    }
    else if (e.isCheckpointMessage())
    {
      try
      {
        ByteBuffer tmpBuffer = e.value();
        byte[] valueBytes = new byte[tmpBuffer.limit()];
        tmpBuffer.get(valueBytes);
        Checkpoint ckpt = new Checkpoint(new String(valueBytes));

        if (this.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
        {
          onSnapshotEvent(ckpt.getSnapshotOffset());
        }
        else if (this.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP)
        {
          onCatchupEvent(ckpt.getWindowScn(), ckpt.getWindowOffset());
        }
        else
        {
          throw new RuntimeException("Invalid checkpoint message received: " + this);
        }
      }
      catch (Exception exception)
      {
        LOG.error("Exception encountered while reading checkpiont from bootstrap service",
                  exception);
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
        currentWindowOffset = 1;
      }
    }

    if (LOG.isDebugEnabled())
    	LOG.info("CurrentWindowSCN : " + currentWindowScn
    			  + ", currentWindowOffset :" + currentWindowOffset
    			  + ", PrevSCN :" + prevWindowScn);
  }

  public void endEvents(long endWindowScn)
  {
    currentWindowOffset = -1;
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
    snapShotOffset = 0;
    this.setSnapshotOffset(snapShotOffset);
  }

  public void endSnapShotSource()
  {
    snapShotOffset = -1;
    this.setSnapshotOffset(snapShotOffset);
  }

  public boolean isSnapShotSourceCompleted()
  {
    return ((this.getSnapshotOffset() == -1) ? true : false);
  }

  public void startCatchupSource()
  {
    currentWindowOffset = 0;
    currentWindowScn = 0;
    this.setWindowOffset(currentWindowOffset);
  }

  public void endCatchupSource()
  {
    endEvents(currentWindowScn);
    this.setWindowOffset(-1);
  }


  public boolean isCatchupCompleted()
  {
    return ((this.getWindowOffset() == -1) ? true : false);
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
  }

  public boolean getFlexible()
  {
    if ((getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION)
        && (getWindowScn() < 0))
    {
      return true;
    }
    else
    {
      return false;
    }
  }

  public void clearBootstrapSinceScn()
  {
    setBootstrapSinceScn(Long.valueOf(INVALID_BOOTSTRAP_SINCE_SCN));
  }

  public void clearBootstrapStartScn()
  {
    setBootstrapStartScn(Long.valueOf(INVALID_BOOTSTRAP_START_SCN));
  }

  public void clearBootstrapTargetScn()
  {
    setBootstrapTargetScn(Long.valueOf(INVALID_BOOTSTRAP_TARGET_SCN));
  }

  public boolean isBootstrapStartScnSet()
  {
    return (INVALID_BOOTSTRAP_START_SCN != getBootstrapStartScn().longValue());
  }

  public boolean isBootstrapSinceScnSet()
  {
    return (INVALID_BOOTSTRAP_SINCE_SCN != getBootstrapSinceScn().longValue());
  }

  /*
   * reset bootstrap specific values in the chckpoint
   */
  public void resetBootstrap()
  {
	  clearBootstrapSinceScn();
	  resetBSServerGeneratedState();
  }

  /**
   * Reset all state info originated from BSServer
   */
  public void resetBSServerGeneratedState()
  {
	  clearBootstrapStartScn();
	  clearBootstrapTargetScn();
	  setBootstrapSnapshotSourceIndex(INVALID_BOOTSTRAP_INDEX);
	  setBootstrapCatchupSourceIndex(INVALID_BOOTSTRAP_INDEX);
	  setBootstrapServerInfo(null);
  }

  /** Remove IOException javac warnings */
  @Override
  public void close()
  {
  }

  /* Helper factory methods */

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
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp.setWindowScn(lastConsumedScn);
    cp.setWindowOffset(-1);

    return cp;
  }
}
