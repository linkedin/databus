package com.linkedin.databus.core;

import java.nio.ByteBuffer;

/**
 * Read-only interface for Databus events.
 **/
public interface DbusEvent
{
  /**
   * Denotes the end of the range of srcid values reserved for private use:
   * (Short.MIN_VALUE, PRIVATE_RANGE_MAX_SRCID]
   */
  public static final short EOPMarkerSrcId = -2;
  public static final short CHECKPOINT_SRCID = -3;
  // -4 - -50 are reserved for errors
  public static final short PRIVATE_RANGE_MAX_ERROR_SRCID = -4;
  public static final short BOOTSTRAPTOOOLD_ERROR_SRCID = -5;
  public static final short PULLER_RETRIES_EXPIRED = -6;
  public static final short DISPATCHER_RETRIES_EXPIRED = -7;
  public static final short PRIVATE_RANGE_MIN_ERROR_SRCID = -50;
  public static final short PRIVATE_RANGE_MAX_SRCID = -20000;

  public static final short SCN_REGRESS = -51;
  // TODO Move these enums to Readable or Writable interface. They are here to get things compiled
  /**
   *
   * @author snagaraj
   *  used to determine status of event when it is read;
   */
  public enum HeaderScanStatus {
    OK,
    ERR,
    PARTIAL,
  }

  /**
   *
   * @author snagaraj
   * used to signal status of event when it is read;
   */
  public enum EventScanStatus {
    OK,
    ERR,
    PARTIAL,
  }

  public boolean isExtReplicatedEvent();

  /** Returns true iff the event points to a valid Databus event */
  public boolean isValid();

  //Event type tests

  /** Checks if the event is a control message */
  public boolean isControlMessage();

  /** Checks if the event is a private control message */
  public boolean isPrivateControlMessage();

  /** Checks if the event is a checkpoint message */
  public boolean isCheckpointMessage();

  /** Returns true iff the key of the event is a numeric (long) */
  public boolean isKeyNumber();

  /** Returns true iff the key of th event is a string (byte sequence) */
  public boolean isKeyString();

  /** Checks if the event is a SCNRegressMessage */
  public boolean isSCNRegressMessage();
  

  // Attribute tests

  /** Returns true if tracing has been enabled for the event */
  public boolean isTraceEnabled();

  /** Checks if the event denotes the end of an event window */
  public boolean isEndOfPeriodMarker();

  // Event fields

  /** Returns the opcode of the data event; null for non-data events */
  public DbusOpcode getOpcode();

  /** Returns the creation timestamp of the event in nanoseconds from Unix epoch */
  public long timestampInNanos();

  /** Returns the total size of the event binary serialization */
  public int size();

  /** Returns the sequence number of the event */
  public long sequence();

  /** Returns key value for events with numeric keys; undefined for events with string keys. */
  public long key();

  /** Returns the length of the event key */
  public int keyLength();

  /** Returns the key value for events with string keys; undefined for events with numeric keys. */
  public byte[] keyBytes();

  /** Returns the Databus source id of the event */
  public short srcId();

  /** Returns the physical partition id for the event */
  public short physicalPartitionId();

  /** Returns the logical partition id for the event */
  public short logicalPartitionId();

  /**
   * Returns a byte array with the hash id of the event serialization schema.
   * <p> NOTE: this will most likely lead to a memory allocation. The preferred way to access the
   * schema id is through {@link DbusEvent#schemaId(byte[]). </p>
   * */
  public byte[] schemaId();

  /**
   * Stores the hash id of the event serialization schema in an existing byte array.
   * <p>NOTE: The byte array should be at least 16 bytes long. </p>
   * */
  public void schemaId(byte[] md5);

  /** Returns the length of the data event value (data payload) */
  public int valueLength();

  /** Obtains the data payload of the event */
  public ByteBuffer value();
}
