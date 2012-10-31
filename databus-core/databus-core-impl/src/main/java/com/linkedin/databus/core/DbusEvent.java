package com.linkedin.databus.core;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xeril.util.base64.Base64;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ByteBufferCRC32;
import com.linkedin.databus.core.util.Utils;

/**
 * This class represents a Databus event stored in a {@link java.nio.ByteBuffer}.
 *
 * <h3>Binary Serialization Format </h3>
 *
 * The table below summarizes the serialization format of Databus events as stored into memory and
 * sent over the wire.
 *
 * <table border="1" >
 * <tr> <th>Field</th> <th> Offset </th> <th>Type</th> <th>Size</th> <th>Description</th> </tr>
 * <tr>
 *    <th colspan="5">Header (61 bytes for events with long keys, 57 bytes + key length)</th>
 * </tr>
 * <tr> <td>MagicByte</td> <td>0</td> <td>byte</td> <td>1</td> <td> A special value denoting the
 * beginning of a message </td> </tr>
 * <tr> <td>HeaderCrc</td> <td>1</td> <td>int</td> <td>4</td> <td>CRC computed over the header</td> </tr>
 * <tr> <td>Length</td> <td>5</td> <td>int</td> <td>4</td> <td>Total length of the event in bytes
 * (fixed-length header + variable-length payload) </td> </tr>
 * <tr> <td>Attributes</td> <td>9</td> <td>short</td> <td>2</td> <td>Event attributes bitmap (see
 * below)</td> </tr>
 * <tr> <td>Sequence</td> <td>11</td> <td>long</td> <td>8</td> <td>Sequence number for the event
 * window in which this event was generated</td> </tr>
 * <tr> <td>PhysicalPartitionId</td> <td>19</td> <td>short</td> <td>2</td> <td>physical partition-id
 * -> represents a sequence generator</td> </tr>
 * <tr> <td>LogicalPartitionId</td> <td>21</td> <td>short</td> <td>2</td> <td>logical partition-id
 * -> represents a logical partition of the physical stream</td> </tr>
 * <tr> <td>NanoTimestamp</td> <td>23</td> <td>long</td> <td>8</td> <td>Time (in nanoseconds) at
 * which the event was generated</td> </tr>
 * <tr> <td>srcId</td> <td>31</td> <td>short</td> <td>2</td> <td>Databus source id for the event</td> </tr>
 * <tr> <td>SchemaId*</td> <td>33</td> <td>byte[]</td> <td>16</td> <td>hash for the schema used to
 * generate the event</td> </tr>
 * <tr> <td>ValueCrc</td> <td>49</td> <td>int</td> <td>4</td> <td>a CRC computed over the
 * variable-length payload of the event</td> </tr>
 * <tr> <td>Key</td> <td>53</td> <td>long</td> <td>8</td> <td>key value for events with long keys
 * <tr> <td>KeySize</td> <td>53</td> <td>int</td> <td>4</td> <td>key length for byte[]-valued keys </td> </tr>
 * <tr>
 *    <th colspan="5">Variable-size payload</th>
 * </tr>
 * <tr> <td>Key</td> <td>57</td> <td>byte[]</td> <td> 4 or KeySize</td>
 * <td>32 LSB from key value for long-typed keys or key value for byte[] keys</td> </tr>
 * <tr> <td>Value</td> <td>61 or 57 + KeySize</td> <td>byte[]</td> <td>Length - Offset(Value)</td>
 * <td>Serialized event payload</td> </tr>
 * </table>
 * <p>
 * &#42 For REPL_DBUS we are using schemaid field to pass Schema Version. So in this case SchemaId
 * row of the table should be replaced with: <p>
 *  <table border="1">
 * <tr><td>SchemaVersion </td> <td>33</td> <td>short</td> <td>2</td> <td> schema version for the event</td></tr>
 * <tr><td>SchemaId(not valid)</td> <td>35</td> <td>byte[]</td> <td>14</td> <td>hash for the schema used to
 * generate the event</td> </tr>
 * </table>
 *
 * <h3>JSON serialization format </h3>
 *
 * The table below summarizes the JSON serialization format.
 *
 * <table border="1">
 * <tr> <th>Attribute</th> <th>Type</th> <th>Description</th> <th>Optional</th> </tr>
 * <tr> <td>opcode</td> <td>String</td> <td>UPSERT or DELETE</td> <td>Yes</td> </tr>
 * <tr> <td>keyBytes</td> <td>String</td> <td>Base-64 encoding of the key byte sequence for string
 * keys</td> <td rowspan="2">One of the two needs to be present</td> </tr>
 * <tr> <td>key</td> <td>Long</td> <td>key value for numeric keys</td> </tr>
 * <tr> <td>sequence</td> <td>Long</td> <td>event sequence number</td> <td>No</td> </tr>
 * <tr> <td>logicalPartitionId</td> <td>Short</td> <td>logical partition id</td> <td>No</td> </tr>
 * <tr> <td>physicalPartitionId</td> <td>Short</td> <td>physical partition id</td> <td>No</td> </tr>
 * <tr> <td>timestampInNanos</td> <td>Long</td> <td>creation timestamp in nanoseconds since the Unix
 * Epoch</td> <td>No</td> </tr>
 * <tr> <td>srcId</td> <td>Short</td> <td>source id</td> <td>No</td> </tr>
 * <tr> <td>schemaId</td> <td>String</td> <td>Base-64 encoding of the event serialization schema
 * hash id</td> <td>No</td> </tr>
 * <tr> <td>valueEnc</td> <td>String</td> <td>value encoding format: JSON or JSON_PLAIN</td>
 * <td>No</td> </tr>
 * <tr> <td>endOfPeriod</td> <td>Boolean</td> <td>true iff the event denotes end of event window</td>
 * <td>Yes; default is false</td> </tr>
 * <tr> <td>value</td> <td>String</td> <td>Literal value string for JSON_PLAIN encoding or Base-64
 * encoding of the value byte sequence for JSON encoding</td> <td>Yes; default is false</td> </tr>
 * </table>
 *
 * <h3>Event attributes</h3>
 *
 * The table below summarizes the Databus event attribute bits
 *
 * <table border="1">
 * <tr> <th>Attribute</th> <th>Bit N</th> <th>Description</th> </tr>
 * <tr> <td>OpCode0</td> <td>0</td> <td>Bit 0 of event opcode</td> </tr>
 * <tr> <td>OpCode1</td> <td>1</td> <td>bit 1 of event opcode</td> </tr>
 * <tr> <td>Trace</td> <td>2</td> <td>The event is a trace event</td> </tr>
 * <tr> <td>ByteKey</td> <td>3</td> <td>The event has a byte[] key</td> </tr>
 * <tr> <td>EoP</td> <td>4</td> <td>The event is the last event in a event window</td> </tr>
 * </table>
 *
 * <h3>Event opcodes</h3>
 *
 * Currently, Databus supports two choices of event opcodes
 *
 * <ul>
 * <li>1 - UPSERT</li>
 * <li>2 - DELETE</li>
 * </ul>
 *
 * <h3>Databus source ids</h3>
 *
 * The possible values for Databus source ids are partitioned into several ranges. In general, all
 * positive source ids are used to uniquely represent a Databus source. The source ids are used in
 * Databus data messages. All non-positive values are reserved for Databus system use. These source
 * ids are used in Databus control messages.
 *
 * <ul>
 * <li> [1, {@link java.lang.Short.MAX_VALUE}] - data source ids</li>
 * <li> [{@link java.lang.Short.MIN_VALUE}, 0] - system source ids
 *   <ul>
 *   <li> [{@link #PRIVATE_RANGE_MAX_SRCID} + 1, 0] - global system source ids. These control
 *   messages will be transmitted to other Databus components over the network
 *     <ul>
 *     <li>-3 - Checkpoint event</li>
 *     </ul>
 *   </li>
 *   <li> [{@link java.lang.Short.MIN_VALUE}, {@link #PRIVATE_RANGE_MAX_SRCID}] - private system
 *   source ids. These messages are used for internal communication inside a Databus component and
 *   are not transmitted to other Databus components.
 *   </ul>
 * </li>
 * </ul>
 *
 * </table>
 */
public class DbusEvent
implements DataChangeEvent, Cloneable
{
  public static final String MODULE = DbusEvent.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

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
  
  public static volatile ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

  /** Serialization Format is :
   * MagicByte (1 byte)
   * HeaderCrc (4 bytes) // Crc to protect the header from being corrupted
   * Length (4 bytes)
   * Attributes (2 byte)  // Key-type, Trace marker, Event-opcode
   * Sequence (8 bytes)  // Sequence number for the event window in which this event was generated
   * Physical PartitionId (2 byte) // Short physical partition-id -> represents a sequence generator
   * Logical PartitionId (2 byte) // Short logical partition-id -> represents a logical partition of the physical stream
   * NanoTimestamp (8 bytes)  // Time (in nanoseconds) at which the event was generated
   * SrcId (short) // SourceId for the event
   * SchemaId (short) // 16-byte hash for the schema used to generate the event
   * ValueCrc (4 bytes)   // Crc to protect the value from being corrupted
   * Key (8 bytes long key)  or KeySize (4 bytes for byte[] key)
   * Key Bytes (k bytes for byte[] key)
   * Value (N bytes)      // Serialized Event
   */

  public static final Byte CurrentMagicValue = 0;
  private static final int MagicOffset = 0;
  public static final int MagicLength = 1;
  private static final int HeaderCrcOffset = MagicOffset + MagicLength;
  private static final int HeaderCrcLength = 4;
  private static final int HeaderCrcDefault = 0;
  //used by the SendEvents command to determine the size of the incoming event
  public static final int LengthOffset = HeaderCrcOffset + HeaderCrcLength;
  public static final int LengthLength = 4;
  private static final int AttributesOffset = LengthOffset + LengthLength;
  private static final int AttributesLength = 2;
  public static final int SequenceOffset = AttributesOffset + AttributesLength;
  public static final int SequenceLength = 8;
  public static final int PhysicalPartitionIdOffset = SequenceOffset + SequenceLength;
  public static final int PhysicalPartitionIdLength = 2;
  public static final int LogicalPartitionIdOffset = PhysicalPartitionIdOffset + PhysicalPartitionIdLength;
  public static final int LogicalPartitionIdLength = 2;
  public static final int TimestampOffset = LogicalPartitionIdOffset + LogicalPartitionIdLength;
  public static final int TimestampLength = 8;
  public static final int SrcIdOffset = TimestampOffset + TimestampLength;
  public static final int SrcIdLength = 2;
  private static final int SchemaIdOffset = SrcIdOffset + SrcIdLength;
  private static final int SchemaIdLength = 16;
  private static final int ValueCrcOffset = SchemaIdOffset + SchemaIdLength;
  private static final int ValueCrcLength = 4;
  private static final int LongKeyOffset = ValueCrcOffset + ValueCrcLength;
  private static final int LongKeyLength = 8;
  private static final int LongKeyValueOffset = LongKeyOffset + LongKeyLength;
  private static final int StringKeyLengthOffset = ValueCrcOffset + ValueCrcLength;
  private static final int StringKeyLengthLength = 4;
  private static final int StringKeyOffset = StringKeyLengthOffset + StringKeyLengthLength;


  private static int KeyLength(byte[] key) { return (key.length + StringKeyLengthLength); } // length of the string + 4 bytes to write the length}
  private static int LongKeyValueOffset() { return (LongKeyOffset + LongKeyLength); }

  private static int StringValueOffset(int keyLength) { return (StringKeyOffset + keyLength); }
  private static int LongKeyHeaderSize = LongKeyValueOffset - LengthOffset;
  private static int StringKeyHeaderSize = StringKeyOffset - LengthOffset;    // everything until the key length field is part of the header, for byte[] keys, we crc the key and value blobs together as part of the value crc

  // Attribute masks follow
  private static final int EVENT_OPCODE_MASK = 0x03;
  private static final int TRACE_FLAG_MASK = 0x04;
  private static final int KEY_TYPE_MASK = 0x08;
  private static final int MinHeaderSize = LongKeyOffset  ;
  private static final int MaxHeaderSize = Math.max(LongKeyValueOffset,StringKeyOffset);

  private int serializedKeyLength() { if (isKeyString()) { return (StringKeyLengthLength + buf.getInt(position+StringKeyLengthOffset)); } else { return LongKeyLength; } }

  private boolean inited;


  private static final TypeReference<Map<String, Object>> JSON_GENERIC_MAP_TYPEREF = new TypeReference<Map<String, Object>>(){};

  private static byte[] emptyValue = "".getBytes();
  public static byte[] emptymd5 = new byte[16];
  static final DbusEventKey EOPMarkerKey = new DbusEventKey(0L);
  public static final byte[] EOPMarkerValue = emptyValue;
  static final byte[] EmptyAttributes = new byte[DbusEvent.AttributesLength];
  static final byte[] UpsertLongKeyAttributes = new byte[DbusEvent.AttributesLength];
  static final byte[] UpsertStringKeyAttributes = new byte[DbusEvent.AttributesLength];


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

  static
  {
    for (int i=0; i< DbusEvent.AttributesLength;++i)
    {
      EmptyAttributes[i] = 0;
      UpsertLongKeyAttributes[i] = 0;
      UpsertStringKeyAttributes[i] = 0;
    }

    setOpcode(UpsertLongKeyAttributes, DbusOpcode.UPSERT);
    setOpcode(UpsertStringKeyAttributes, DbusOpcode.UPSERT);
    setKeyTypeString(UpsertStringKeyAttributes);
  }

  public static DbusEvent createErrorEvent(DbusErrorEvent errorEvent)
  {
    byte[] serializedErrorEvent = errorEvent.toString().getBytes();
    ByteBuffer tmpBuffer = ByteBuffer.allocate(serializedErrorEvent.length + LongKeyValueOffset).order(DbusEvent.byteOrder);
    byte[] attributes = EmptyAttributes;
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
                                                0L,
                                                (short)-1,
                                                (short) 0,
                                                System.nanoTime(),
                                                errorEvent.getErrorId(),
                                                emptymd5,
                                                serializedErrorEvent,
                                                false, //enable tracing
                                                true // autocommit
                                                );
    serializeFullEvent(0L, tmpBuffer, eventInfo, attributes);
    DbusEvent event = new DbusEvent(tmpBuffer, 0);
    return event;
  }

  public static DbusErrorEvent getErrorEventFromDbusEvent(DbusEvent event)
  {
    if (!event.isErrorEvent())
    {
      throw new RuntimeException("Event is expected to be an error event: " + event);
    }

    ByteBuffer valueBuffer = event.value();
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);
    try
    {
      DbusErrorEvent errorEvent = DbusErrorEvent.createDbusErrorEvent(new String(valueBytes));
      return errorEvent;
    }
    catch (JsonParseException e)
    {
      throw new RuntimeException(e);
    }
    catch (JsonMappingException e)
    {
      throw new RuntimeException(e);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }

  }

  
  /**
   * Utility method to create a DbusEvent with a SCNRegress message in it.
   * @param checkpoint
   * @return
   */
  public static DbusEvent createSCNRegressEvent(SCNRegressMessage message)
  {
	String regressMsg =   SCNRegressMessage.toJSONString(message);
	
	if ( null == regressMsg)
		return null;
	
    byte[] serializedMsg = regressMsg.getBytes();
    ByteBuffer tmpBuffer = ByteBuffer.allocate(serializedMsg.length + LongKeyValueOffset).order(DbusEvent.byteOrder);
    byte[] attributes = EmptyAttributes;
    DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                message.getCheckpoint().getWindowScn(),
                                                (short)-1,
                                                (short) 0,
                                                System.nanoTime(),
                                                SCN_REGRESS,
                                                emptymd5,
                                                serializedMsg,
                                                false, //enable tracing
                                                true // autocommit
                                                );
    serializeFullEvent(0L, tmpBuffer, eventInfo, attributes);
    DbusEvent event = new DbusEvent(tmpBuffer, 0);
    return event;
  }
  
  
  /**
   * Utility method to extract a SCN regress message from a DbusEvent
   * Note: Ensure that this is a SCNRegressMessage event before calling this method.
   * @param event
   * @return
   */
  public static SCNRegressMessage getSCNRegressFromEvent(DbusEvent event)
  {
    assert (event.isSCNRegressMessage());
    ByteBuffer valueBuffer = event.value();
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);    
    SCNRegressMessage newRegressMsg = SCNRegressMessage.getRegressMessage(new String(valueBytes));
    return newRegressMsg;
  }
  
  /**
   * Utility method to create a DbusEvent with an embedded checkpoint in it.
   * @param checkpoint
   * @return
   */
  public static DbusEvent createCheckpointEvent(Checkpoint checkpoint)
  {
    byte[] serializedCheckpoint = checkpoint.toString().getBytes();
    ByteBuffer tmpBuffer = ByteBuffer.allocate(serializedCheckpoint.length + LongKeyValueOffset).order(DbusEvent.byteOrder);
    byte[] attributes = EmptyAttributes;
    DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                0L,
                                                (short)-1,
                                                (short) 0,
                                                System.nanoTime(),
                                                CHECKPOINT_SRCID,
                                                emptymd5,
                                                serializedCheckpoint,
                                                false, //enable tracing
                                                true // autocommit
                                                );
    serializeFullEvent(0L, tmpBuffer, eventInfo, attributes);
    DbusEvent event = new DbusEvent(tmpBuffer, 0);
    return event;
  }

  /**
   * Utility method to extract a checkpoint from a DbusEvent
   * Note: Ensure that this is a Checkpoint event before calling this method.
   * @param event
   * @return
   */
  public static Checkpoint getCheckpointFromEvent(DbusEvent event)
  {
    assert (event.isCheckpointMessage());
    ByteBuffer valueBuffer = event.value();
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);
    try
    {
      Checkpoint newCheckpoint = new Checkpoint(new String(valueBytes));
      return newCheckpoint;
    }
    catch (JsonParseException e)
    {
      throw new RuntimeException(e);
    }
    catch (JsonMappingException e)
    {
      throw new RuntimeException(e);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }

  }

  /**
   * Serializes an End-Of-Period Marker onto the ByteBuffer passed in.
   * @param sequence - The sequence to store on the EOP marker
   * @param timeStamp - The timestamp to use for the EOP marker
   * @param serializationBuffer - The ByteBuffer to serialize the event in. The buffer must have enough space to accommodate
   *                              the event. (76 bytes)
   * @return the number of bytes written
   */
  //public static int serializeEndOfPeriodMarker(long sequence, long timeStamp, ByteBuffer serializationBuffer,
  //short pPartitionId, short lPartitionId)
  public static int serializeEndOfPeriodMarker(ByteBuffer serializationBuffer, DbusEventInfo eventInfo)
  {
    byte[] attributes = EmptyAttributes;
    return serializeFullEvent(EOPMarkerKey.getLongKey(), serializationBuffer, eventInfo, attributes);
  }

  /**
   * non-threadsafe : serializationBuffer needs to be protected if multiple threads are writing to it concurrently
   * @param key
   * @param logicalPartitionId
   * @param timeStampInNanos
   * @param srcId
   * @param schemaId
   * @param value
   * @param enableTracing
   * @param serializationByteBuffer
   */
  public static int serializeEvent(DbusEventKey key,
                                   short pPartitionId,
                                   short lPartitionId,
                                   long timeStampInNanos,
                                   short srcId,
                                   byte[] schemaId,
                                   byte[] value,
                                   boolean enableTracing,
                                   ByteBuffer serializationBuffer)
  throws KeyTypeNotImplementedException  {
    DbusEventInfo dbusEventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
                                                    0L,
                                                    pPartitionId,
                                                    lPartitionId,
                                                    timeStampInNanos,
                                                    srcId,
                                                    schemaId,
                                                    value,
                                                    enableTracing,
                                                    false /*autocommit*/);

    return serializeEvent(key, serializationBuffer, dbusEventInfo);
  }

  public static int serializeEvent(DbusEventKey key,
                                   ByteBuffer serializationBuffer,
                                   DbusEventInfo dbusEventInfo)
  throws KeyTypeNotImplementedException {

    switch (key.getKeyType())
    {
    case LONG:
      return serializeLongKeyEvent(key.getLongKey(), serializationBuffer, dbusEventInfo);
    case STRING:
      return serializeStringKeyEvent(key.getStringKeyInBytes(), serializationBuffer, dbusEventInfo);
    default:
      throw new KeyTypeNotImplementedException();
    }
                                   }

  public static int serializeFullEvent(DbusEventKey key,
                                       ByteBuffer serializationBuffer,
                                       DbusEventInfo eventInfo)
  {
    int numBytes = 0;
    switch (key.getKeyType())
    {
    case LONG:
        numBytes = serializeLongKeyEvent(key.getLongKey(), serializationBuffer,eventInfo);
        break;
    case STRING:
        numBytes = serializeStringKeyEvent(key.getStringKeyInBytes(), serializationBuffer, eventInfo);
        break;
    }

    return numBytes;
  }

  private static int serializeLongKeyEvent(long key,
                                           ByteBuffer serializationBuffer,
                                           DbusEventInfo eventInfo)  {
    byte[] attributes = UpsertLongKeyAttributes.clone();
    if(eventInfo.getOpCode() != null)
      setOpcode(attributes, eventInfo.getOpCode());
    if (eventInfo.isEnableTracing())
    {
      setTraceFlag(attributes);
    }
    return (serializeFullEvent(key,
                               serializationBuffer,
                               eventInfo,
                               attributes));
  }


  private static int serializeFullEvent(long key,
                                        ByteBuffer serializationBuffer,
                                        DbusEventInfo eventInfo,
                                        byte[] attributes)
  {

    int startPosition = serializationBuffer.position();
    serializationBuffer.put(CurrentMagicValue)
    .putInt(HeaderCrcDefault)
    .putInt(LongKeyValueOffset + eventInfo.getValue().length)
    .put(attributes)
    .putLong(eventInfo.getSequenceId())
    .putShort(eventInfo.getpPartitionId())
    .putShort(eventInfo.getlPartitionId())
    .putLong(eventInfo.getTimeStampInNanos())
    .putShort(eventInfo.getSrcId())
    .put(eventInfo.getSchemaId(),0,16)
    .putInt(HeaderCrcDefault)
    .putLong(key)
    .put(eventInfo.getValue());
    int stopPosition = serializationBuffer.position();

    long valueCrc = ByteBufferCRC32.getChecksum(serializationBuffer,
                                                startPosition+LongKeyValueOffset,
                                                eventInfo.getValue().length);
    Utils.putUnsignedInt(serializationBuffer, startPosition+ValueCrcOffset, valueCrc);

    if (eventInfo.isAutocommit())
    {
      //TODO (DDSDBUS-60): Medium : can avoid new here
      DbusEvent e = new DbusEvent(serializationBuffer, startPosition);
      e.applyCrc();
    }

    serializationBuffer.position(stopPosition);
    return (stopPosition - startPosition);
  }


  private static int serializeStringKeyEvent(byte[] key,
                                             ByteBuffer serializationBuffer,
                                             DbusEventInfo eventInfo)
  {
    int startPosition = serializationBuffer.position();
    byte[] attributes = UpsertStringKeyAttributes.clone();
    if(eventInfo.getOpCode() != null)
      setOpcode(attributes, eventInfo.getOpCode());
    if (eventInfo.isEnableTracing()) {
      setTraceFlag(attributes);
    }
    serializationBuffer.put(CurrentMagicValue)
    .putInt(HeaderCrcDefault)
    .putInt(StringValueOffset(key.length) + eventInfo.getValue().length)
    .put(attributes)
    .putLong(eventInfo.getSequenceId())
    .putShort(eventInfo.getpPartitionId())
    .putShort(eventInfo.getlPartitionId())
    .putLong(eventInfo.getTimeStampInNanos())
    .putShort(eventInfo.getSrcId())
    .put(eventInfo.getSchemaId(),0,16)
    .putInt(HeaderCrcDefault)
    .putInt(key.length)
    .put(key)
    .put(eventInfo.getValue());
    int stopPosition = serializationBuffer.position();
    long valueCrc = ByteBufferCRC32.getChecksum(serializationBuffer, startPosition+StringKeyOffset,
                                                key.length+eventInfo.getValue().length);
    Utils.putUnsignedInt(serializationBuffer, startPosition + ValueCrcOffset, valueCrc);
    if (eventInfo.isAutocommit())
    {
      //TODO (DDSDBUS-61): Medium : can avoid new here
      DbusEvent e = new DbusEvent(serializationBuffer, startPosition);
      e.applyCrc();
    }

    serializationBuffer.position(stopPosition);
    return (stopPosition - startPosition);
  }


  private boolean isAttributeBitSet(int mask) {
    return ((buf.getShort(position+AttributesOffset) & mask) == mask);

  }

  private static void setOpcode(byte[] attribute, DbusOpcode opcode)
  {
    switch (opcode)
    {
    case UPSERT:
        setAttributeBit(attribute, 0x01);
        break;
    case DELETE:
        setAttributeBit(attribute, 0x02);
        break;
    default:
        throw new RuntimeException("Unknown DbusOpcode " + opcode.name());
    }

  }

  @Override
  public DbusOpcode getOpcode()
  {
    if (isAttributeBitSet(1))
    {
      return DbusOpcode.UPSERT;
    }

    if (isAttributeBitSet(2))
    {
      return DbusOpcode.DELETE;
    }
    return null;
  }

  private static void setAttributeBit(byte[] attribute, int mask) {
    for (int i=0; i < DbusEvent.AttributesLength; ++i)
    {
      attribute[i] = (byte) (attribute[i]&0xff | mask);
    }
  }

  public static void setTraceFlag(byte[] attribute) {
    setAttributeBit(attribute, TRACE_FLAG_MASK);
  }

  @Override
  public boolean isTraceEnabled() {
    return isAttributeBitSet(TRACE_FLAG_MASK);
  }

  private static void setKeyTypeString(byte[] attributes) {
    setAttributeBit(attributes, KEY_TYPE_MASK);
  }

  @Override
  public boolean isKeyString() {
    return isAttributeBitSet(KEY_TYPE_MASK);
  }

  @Override
  public boolean isKeyNumber() {
    return !isAttributeBitSet(KEY_TYPE_MASK);
  }

  @Override
  public boolean isControlMessage() {
    return isControlSrcId(srcId());
  }

  public static boolean isControlSrcId(short srcId)
  {
    return srcId < 0;
  }

  @Override
  public boolean isEndOfPeriodMarker() {
    return (srcId() == EOPMarkerSrcId);
  }

  @Override
  public boolean isPrivateControlMessage()
  {
    return (srcId() <= PRIVATE_RANGE_MAX_SRCID);
  }

  @Override
  public boolean isCheckpointMessage()
  {
    return (srcId() == CHECKPOINT_SRCID);
  }
  
  @Override
  public boolean isSCNRegressMessage()
  {
    return (srcId() == SCN_REGRESS);
  }


  public boolean isErrorEvent()
  {
    return (srcId() > PRIVATE_RANGE_MIN_ERROR_SRCID &&
            srcId() < PRIVATE_RANGE_MAX_ERROR_SRCID);
  }

  public DbusEvent(ByteBuffer buf, int position) {
    reset(buf, position);
  }

  public DbusEvent()
  {
    // empty constructor that doesn't do anything useful.
    inited = false;
  }

  private int position;
  private ByteBuffer buf;


  public void reset(ByteBuffer buf, int position) {
    inited = true;
    this.buf = buf;
    this.position = position;
  }


  public static int length(DbusEventKey key, byte[] value) throws KeyTypeNotImplementedException {

    switch(key.getKeyType())
    {
    case LONG: {
      return (LongKeyOffset + LongKeyLength + value.length);
    }
    case STRING: {
      return (StringKeyOffset + key.getStringKeyInBytes().length + value.length);
    }
    default: {
      throw new KeyTypeNotImplementedException();
    }
    }

  }



  public void unsetInited()
  {
    inited = false;
  }

  public void applyCrc()  {
    //Re-compute the header crc
    int headerLength = headerLength();
    long headerCrc = ByteBufferCRC32.getChecksum(buf, position+LengthOffset, headerLength);
    Utils.putUnsignedInt(buf, position + HeaderCrcOffset, headerCrc);
  }

  public int headerLength()  {
    if (!isKeyString()) {
      return LongKeyHeaderSize;
    }
    else {
      return StringKeyHeaderSize;
    }
  }

  public int payloadLength() {
    int overhead = LengthOffset;
    return (size() - headerLength() - overhead);
  }

  public void setSequence(long sequence) {
    buf.putLong(position +SequenceOffset, sequence);
  }

  @Override
  public long sequence()
  {
    return (buf.getLong(position + SequenceOffset));
  }

  @Override
  public int keyLength()
  {
    if (isKeyString())
    {
      return (StringKeyLengthLength + buf.getInt(position+StringKeyLengthOffset));
    }
    else
    {
      return LongKeyLength;
    }
  }


  @Override
  public int valueLength()
  {
    return (size() - (LongKeyOffset + keyLength()));
  }


  @Override
  public boolean isValid()
  {
    return isValid(true);
  }


  public boolean isPartial() {
	  return isPartial(true);
  }

  public HeaderScanStatus scanHeader() {
	  return scanHeader(true);
  }

  public EventScanStatus scanEvent() {
	  return scanEvent(true);
  }


  /**
   *
   * @param logErrors
   * @return PARTIAL if the event appears to be a partial event; ERR if the header is corrupt; OK if the event header is intact and the event appears to be complete
   */

  protected HeaderScanStatus scanHeader(boolean logErrors) {
	  if (!magic().equals(CurrentMagicValue)) {
	      if (logErrors) {
	        LOG.error("No Magic Byte in header!" + magic());
	      }
		  return HeaderScanStatus.ERR;
	  }

	  if (isHeaderPartial(logErrors)) {
		  return HeaderScanStatus.PARTIAL;
	  }

	  int size = size();
	  if (size < MinHeaderSize) {
		  //size can never be smaller than min header size
	      if (logErrors) {
	        LOG.error("Event size too small ; size=" + size + " Header size= " + MinHeaderSize);
	      }
		  return HeaderScanStatus.ERR;
	  }
	  int headerLength = headerLength();
	  long calculatedHeaderCrc = ByteBufferCRC32.getChecksum(buf, position+LengthOffset, headerLength);
	  if(calculatedHeaderCrc != this.headerCrc())
	  {
		  if (logErrors)
		  {
		      LOG.error("Header CRC mismatch: ");
			  LOG.error("headerCrc() = "+ this.headerCrc());
			  LOG.error("calculatedCrc = "+ calculatedHeaderCrc);
		  }
		  return HeaderScanStatus.ERR;
	  }
	  return HeaderScanStatus.OK;
  }


  /**
   *
   * @param logErrors
   * @return one of ERR/ OK / PARTIAL
   */
  protected EventScanStatus scanEvent(boolean logErrors) {
	  HeaderScanStatus h = scanHeader(logErrors);

	  if (h != HeaderScanStatus.OK)  {
	      if (logErrors){
	        LOG.error("HeaderScan error=" + h);
	      }
		  return (h == HeaderScanStatus.ERR? EventScanStatus.ERR : EventScanStatus.PARTIAL);
	  }

	  //check if event is partial
	  if (isPartial(logErrors)) {
		  return EventScanStatus.PARTIAL;
	  }

	  int payloadLength = payloadLength();
	  long calculatedValueCrc = ByteBufferCRC32.getChecksum(buf,
			  isKeyNumber()?position+LongKeyValueOffset:position+StringKeyOffset,
					  payloadLength);

	  if (calculatedValueCrc != this.valueCrc())
	  {
		  if (logErrors)
		  {
			  LOG.error("valueCrc() = "+ this.valueCrc());
			  LOG.error("crc.getValue() = "+ calculatedValueCrc);
			  LOG.error("crc-ed block size = "+ payloadLength);
			  LOG.error("event sequence = " + this.sequence());
		  }
		  return EventScanStatus.ERR;
	  }

	  return EventScanStatus.OK;
  }

  /**
   *
   * @param logErrors
   * @return true if the event appears to be partially read ; does not perform any header checks
   */
  protected boolean isPartial (boolean logErrors) {
	  int size = size();
	  if (size > (this.buf.limit()-position)) {
		  if (logErrors)
			  LOG.error("size() = " + size + "buf_position=" + position +  " limit = " + buf.limit() +
			  		" (this.buf.limit()-position) = "+ (this.buf.limit()-position));
		  return true;
	  }
	  return false;
  }

  protected boolean isHeaderPartial(boolean logErrors) {
    int limit = this.buf.limit();
    int bufHeaderLen = limit - position; // what we have in buffer

    // make sure we can at least read attributes
    if (bufHeaderLen < (AttributesOffset + AttributesLength))
    {
      return true;
    }

    int headerLen = headerLength() + LengthOffset; // headerLength() ignores first LengthOffset bytes

    // we need to figure out if we got enough to calc the CRC
    // to calc CRC we are using from LengthOffset => KeyOffset
    // headerLen = the required min len of the header
    // bufHeaderLen - what we have in the buffer
    if (bufHeaderLen < headerLen) {
      if (logErrors)
        LOG.error("Partial Header: bufHeaderLen=" + bufHeaderLen + " is less then headerLen=" + headerLen);
      return true;
    }
    return false;
  }

  /**
   * Checks if the event header - containing length is vsalid
   * @param logErrors
   * @return true iff header is devoid of errors; the length field can be trusted.
   */
  protected boolean isHeaderValid(boolean logErrors) {
	  return scanHeader(logErrors) == HeaderScanStatus.OK;
  }



  /**
   *
   * @param logErrors : whether to emit LOG.error messages for invalid results
   * @return true if event is not partial and event is valid; Note that a partial event is deemed invalid;
   */
  protected boolean isValid(boolean logErrors) {
	  return scanEvent(logErrors) == EventScanStatus.OK;
  }




  @Override
  public String toString()
  {
	if ( null == buf)
	{
		return "buf=null";
	}

	boolean valid = true;

	try
	{
		valid = isValid(true);
	} catch (Exception ex) {
		LOG.error("DbusEvent.toString() : Got Exception while trying to validate the event ", ex);
		valid = false;
	}

	if ( !valid )
	{
		return "Position=" + position + ",Buf =" + buf + ", Validity = false";
	}

    StringBuilder sb = new StringBuilder(200);
    sb.append("Position=")
    .append(position)
    .append(";Magic = ")
    .append(magic())
    .append("isEndOfPeriodMarker=")
    .append(isEndOfPeriodMarker())
    .append(";HeaderCrc = ")
    .append(headerCrc())
    .append(";Length = ")
    .append(size())
    .append(";Key = ");
    if (isKeyString())
    {
      sb.append(new String(this.keyBytes()));
    }
    else
    {
      sb.append(key());
    }

    sb.append(";Sequence = ")
    .append(sequence())
    .append(";LogicalPartitionId = ")
    .append(logicalPartitionId())
    .append(";PhysicalPartitionId = ")
    .append(physicalPartitionId())
    .append(";Timestamp = ")
    .append(timestampInNanos())
    .append(";SrcId = ")
    .append(srcId())
    .append(";SchemaId = ")
    .append(Hex.encodeHex(schemaId()))
    .append(";Value Crc =")
    .append(valueCrc());
    //Do not output value - as it's too long.
    /*
    .append(";Value = ");

    try {
      sb.append(Utils.byteBufferToString(value(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      sb.append(value().toString());
    }
    */
    return sb.toString();

  }


  public Byte magic()
  {
    return (this.buf.get(position+MagicOffset));
  }

  @Override
  public int size()
  {
    //    int size = this.buf.getInt(position+LengthOffset);
    return (this.buf.getInt(position+LengthOffset)); // length bytes
  }

  /**
   * Setter for size
   * @param sz
   */
  public void setSize(int sz) {

	  this.buf.putInt(position + LengthOffset, sz);
  }

  public long headerCrc()
  {
    return Utils.getUnsignedInt(buf, position+HeaderCrcOffset);
  }

  public void setHeaderCrc(long crc) {
	  Utils.putUnsignedInt(buf, position+HeaderCrcOffset, crc);
  }


  @Override
  public long key()
  {
    assert isKeyNumber();
    return this.buf.getLong(position+LongKeyOffset);
  }

  @Override
  public byte[] keyBytes()
  {
    assert isKeyString();
    int keyLength = buf.getInt(position + LongKeyOffset);
    byte[] dst = new byte[keyLength];
    for (int i=0; i< keyLength; ++i)
    {
      dst[i] = buf.get(position+LongKeyOffset+4+i);
    }
    return dst;
  }

  @Override
  public short physicalPartitionId()
  {
    return this.buf.getShort(position + PhysicalPartitionIdOffset);
  }

  @Override
  public short logicalPartitionId()
  {
    return this.buf.getShort(position + LogicalPartitionIdOffset);
  }

  /**public void setScn(long scn)
  {
    buf.putLong(position+ScnOffset, scn);
  }
  **/

  /**
  @Override
  public long scn()
  {
    return this.buf.getLong(position+ScnOffset);
  }
  **/

  @Override

  public long timestampInNanos()
  {
    return this.buf.getLong(position+TimestampOffset);
  }

  // two temp method to pass schema version from RPL_DBUS
  public void setSchemaVersion(short schemaVersion)
  {
    buf.putShort(position+SchemaIdOffset, schemaVersion);
  }
  public short schemaVersion()
  {
    return this.buf.getShort(position+SchemaIdOffset);
  }

  public void setSrcId(short srcId)
  {
    buf.putShort(position+SrcIdOffset, srcId);
  }

  @Override
  public short srcId()
  {
    return this.buf.getShort(position+SrcIdOffset);
  }

  /** put a byte[] schemaId into the buffer .
   * Make sure CRC is recomputed after that */
  public void setSchemaId(byte[] schemaId) {
    for(int i=0; i<16; i++) {
      buf.put(position+SchemaIdOffset+i, schemaId[i]);
    }
  }


  @Override
  public byte[] schemaId()
  {
    byte[] md5 = new byte[16];
    for (int i = 0; i < 16; i++)
    {
      md5[i] = buf.get(position+SchemaIdOffset+i);
    }
    return md5;
  }

  @Override
  public void schemaId(byte[] md5)
  {
    for (int i = 0; i < 16; i++)
    {
      md5[i] = buf.get(position+SchemaIdOffset+i);
    }

  }

  public long valueCrc() {
    return Utils.getUnsignedInt(buf, position+ValueCrcOffset);
  }

  public void setValueCrc(long crc) {
	  Utils.putUnsignedInt(buf, position+ValueCrcOffset, crc);
  }

  @Override
  public ByteBuffer value() {
    ByteBuffer value = this.buf.asReadOnlyBuffer().order(DbusEvent.byteOrder);
    value.position(position+LongKeyOffset + keyLength());
    value = value.slice().order(DbusEvent.byteOrder);
    int valueSize = valueLength();
    value.limit(valueSize);
    value.rewind();
    return value;
  }

  public void setValue(byte[] bytes) {
    int offset = position+LongKeyOffset+serializedKeyLength();
    for (int i=0;i< bytes.length;++i) {
      this.buf.put(offset+i,bytes[i]);
    }
  }


  public ByteBuffer getRawBytes()
  {
    ByteBuffer buffer = this.buf.asReadOnlyBuffer().order(DbusEvent.byteOrder);
    buffer.position(position);
    buffer.limit(position + size());
    return buffer;
  }




  public DbusEvent createCopy() {
	  ByteBuffer cloned = ByteBuffer.allocate(size()).order(DbusEvent.byteOrder);
	  cloned.put(getRawBytes());
	  DbusEvent c = new DbusEvent(cloned,0);
	  return c;
  }

/**
 * Serializes the event to a channel using the specified encoding
 * @param  writeChannel           the channel to write to
 * @param  encoding               the serialization encoding
 * @return the number of bytes written to the channel
 */
  public int writeTo(WritableByteChannel writeChannel, Encoding encoding)
  {
    int bytesWritten = 0;
    switch (encoding)
    {
    case BINARY:
    {
      //TODO (DDSDBUS-62): writeBuffer can be cached
      ByteBuffer writeBuffer = this.buf.duplicate().order(DbusEvent.byteOrder);
      writeBuffer.position(position);
      writeBuffer.limit(position+size());
      try
      {
        bytesWritten = writeChannel.write(writeBuffer);
      }
      catch (IOException e)
      {
        // TODO (medium) (DDSDBUS-63) Handle IOException correctly
        LOG.error("binary write error: " + e.getMessage(), e);
      }
      break;
    }
    case JSON_PLAIN_VALUE:
    case JSON: {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      JsonFactory f = new JsonFactory();
      try {
        JsonGenerator g = f.createJsonGenerator(baos, JsonEncoding.UTF8);
        g.writeStartObject();

        DbusOpcode opcode = getOpcode();
        if (null != opcode)
        {
          g.writeStringField("opcode", opcode.toString());
        }

        if (isKeyString())
        {
          g.writeStringField("keyBytes", Base64.encodeBytes(keyBytes()));
        }
        else
        {
          g.writeNumberField("key", key());
        }
        g.writeNumberField("sequence", sequence());
        g.writeNumberField("logicalPartitionId", logicalPartitionId());
        g.writeNumberField("physicalPartitionId", physicalPartitionId());
        g.writeNumberField("timestampInNanos", timestampInNanos());
        g.writeNumberField("srcId", srcId());
        g.writeStringField("schemaId", Base64.encodeBytes(schemaId()));
        g.writeStringField("valueEnc", encoding.toString());
        if (isEndOfPeriodMarker())
        {
          g.writeBooleanField("endOfPeriod", true);
        }

        if (isTraceEnabled())
        {
          g.writeBooleanField("traceEnabled", true);
        }

        if (encoding.equals(Encoding.JSON))
        {
          g.writeStringField("value", Base64.encodeBytes(Utils.byteBufferToBytes(value())));
        }
        else
        {
          g.writeStringField("value", Utils.byteBufferToString(value()));
        }
        g.writeEndObject();
        g.flush();
        baos.write("\n".getBytes());
      } catch (IOException e) {
        LOG.error("JSON write error: " + e.getMessage(), e);
      }
      ByteBuffer writeBuffer = ByteBuffer.wrap(baos.toByteArray()).order(DbusEvent.byteOrder);
      try {
        bytesWritten = writeChannel.write(writeBuffer);
      } catch (IOException e) {
        LOG.error("JSON write error: " + e.getMessage(), e);
      }
      break;
    }
    }

    return bytesWritten;

  }

  /**
   * Appends a single event to the buffer. The event is
   * @param jsonString
   * @param eventBuffer
   * @return
   * @throws IOException
   * @throws JsonParseException
   * @throws InvalidEventException
   * @throws KeyTypeNotImplementedException
   */
  public static int appendToEventBuffer(String jsonString, DbusEventBufferAppendable eventBuffer,
                                            DbusEventsStatisticsCollector statsCollector,
                                            boolean startWindow)
                                            throws IOException, JsonParseException,
                                                   InvalidEventException,
                                                   KeyTypeNotImplementedException
  {
    int numEvents = appendToEventBuffer(jsonString, eventBuffer, null, statsCollector, startWindow);
    return numEvents;
  }

  public static int appendToEventBuffer(BufferedReader jsonStream, DbusEventBufferAppendable eventBuffer,
                                            DbusEventsStatisticsCollector statsCollector,
                                            boolean startWindow)
                                            throws IOException, JsonParseException,
                                                   InvalidEventException
  {
    ObjectMapper objectMapper = new ObjectMapper();
    String jsonString;
    int numEvents = 0;
    boolean endOfPeriod = startWindow;
    try
    {
      boolean success = true;
      while (success && null != (jsonString = jsonStream.readLine()))
      {
        int appendResult = appendToEventBuffer(jsonString, eventBuffer, objectMapper, statsCollector,
                                               endOfPeriod);
        success = (appendResult > 0);
        endOfPeriod = (2 == appendResult);
        ++numEvents;
      }
    }
    catch (KeyTypeNotImplementedException e)
    {
      LOG.error("append error: " + e.getMessage(), e);
      numEvents = -1;
    }

    return numEvents;
  }

  /**
   * Appends a single JSON-serialized event to the buffer
   * @return 0 if add failed, 1 if a regular event has been added, 2 if EOP has been added
   * */
  static int appendToEventBuffer(String jsonString, DbusEventBufferAppendable eventBuffer,
                                     ObjectMapper objectMapper,
                                     DbusEventsStatisticsCollector statsCollector,
                                     boolean startWindow)
                                     throws IOException, JsonParseException, InvalidEventException,
                                            KeyTypeNotImplementedException
  {
    if (null == objectMapper)
    {
      objectMapper = new ObjectMapper();
    }

    Map<String, Object> jsonObj = objectMapper.readValue(jsonString, JSON_GENERIC_MAP_TYPEREF);
    Object tmpObject;

    tmpObject = jsonObj.get("timestampInNanos");
    if (null == tmpObject || ! (tmpObject instanceof Number))
    {
      throw new InvalidEventException("timestampInNanos expected");
    }
    long timestamp = ((Number)tmpObject).longValue();

    tmpObject = jsonObj.get("sequence");
    if (null == tmpObject || ! (tmpObject instanceof Number))
    {
      throw new InvalidEventException("sequence expected");
    }
    long windowScn = ((Number)tmpObject).longValue();



    boolean endOfPeriod = false;
    boolean result = false;
    tmpObject = jsonObj.get("endOfPeriod");
    if (null != tmpObject)
    {
      if (! (tmpObject instanceof String) && ! (tmpObject instanceof Boolean))
      {
        throw new InvalidEventException("invalid endOfPeriod");
      }

      if (((tmpObject instanceof Boolean) && ((Boolean)tmpObject).booleanValue()) ||
          ((tmpObject instanceof String) && Boolean.parseBoolean((String)tmpObject)))
      {
        eventBuffer.endEvents(windowScn,statsCollector);

        endOfPeriod = true;
        result = true;
      }
    }

    if (!endOfPeriod)
    {
      tmpObject = jsonObj.get("key");
      if (null == tmpObject || ! (tmpObject instanceof Number))
      {
        throw new InvalidEventException("key expected");
      }
      DbusEventKey key = new DbusEventKey(((Number)tmpObject).longValue());

      tmpObject = jsonObj.get("logicalPartitionId");
      if (null == tmpObject || ! (tmpObject instanceof Number))
      {
        throw new InvalidEventException("logicalPartitionId expected");
      }
      short lPartitionId = ((Number)tmpObject).shortValue();

      tmpObject = jsonObj.get("physicalPartitionId");
      if (null == tmpObject || ! (tmpObject instanceof Number))
      {
        throw new InvalidEventException("logicalPartitionId expected");
      }
      short pPartitionId = ((Number)tmpObject).shortValue();

      tmpObject = jsonObj.get("srcId");
      if (null == tmpObject || ! (tmpObject instanceof Number))
      {
        throw new InvalidEventException("srcId expected");
      }
      short srcId = ((Number)tmpObject).shortValue();

      tmpObject = jsonObj.get("schemaId");
      if (null == tmpObject || ! (tmpObject instanceof String))
      {
        throw new InvalidEventException("schemaId expected");
      }
      String base64String = (String)tmpObject;
      byte[] schemaId = Base64.decode(base64String);

      tmpObject = jsonObj.get("valueEnc");
      if (null == tmpObject || ! (tmpObject instanceof String))
      {
        throw new InvalidEventException("valueEnc expected");
      }
      String valueEncString = (String)tmpObject;

      tmpObject = jsonObj.get("value");
      if (null == tmpObject || ! (tmpObject instanceof String))
      {
        throw new InvalidEventException("value expected");
      }
      base64String = (String)tmpObject;
      byte[] value;
      if (valueEncString.equals(Encoding.JSON.toString()))
      {
        value = Base64.decode(base64String);
      }
      else if (valueEncString.equals(Encoding.JSON_PLAIN_VALUE.toString()))
      {
        value = base64String.getBytes();
      }
      else
      {
        throw new InvalidEventException("Unknown value encoding: " + valueEncString);
      }

      if (startWindow)
      {
        eventBuffer.startEvents();
      }

      boolean traceEnabled = false;
      tmpObject = jsonObj.get("traceEnabled");
      if (null != tmpObject)
      {
        if(! (tmpObject instanceof Boolean)) throw new InvalidEventException("traceEnabled must be boolean");
        traceEnabled = (Boolean)tmpObject;
      }

      result = eventBuffer.appendEvent(key, pPartitionId, lPartitionId, timestamp, srcId, schemaId, value,
                                       traceEnabled, statsCollector);
    }

    if (result)
    {
      return endOfPeriod ? 2 : 1;
    }
    else
    {
      return 0;
    }
  }

  public boolean inited()
  {
    return inited;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
    {
      return false;
    }

    if (obj instanceof DbusEvent)
    {
      DbusEvent objEvent = (DbusEvent) obj;
      return (this.headerCrc() == objEvent.headerCrc()
            && this.valueCrc() == objEvent.valueCrc()
            );
    }
    else
    {
      return false;
    }

  }

  @Override
  public int hashCode()
  {
    return ((int)this.headerCrc() ^ (int)this.valueCrc());
  }


  /**
   * Creates a copy of the current event.
   *
   * <p> <b>Note: This method should be used with extreme care as the event serialization pointed
   * by the object can be overwritten. It should be used only in buffers with BLOCK_ON_WRITE policy.
   * Further, the object should not be used after {@link DbusEventBuffer.DbusEventIterator#remove()}
   * </b></p>
   * @param  reuse       an existing object to reuse; if null, a new object will be created
   * @return the event copy
   */
  public DbusEvent clone(DbusEvent reuse)
  {
    if (null == reuse)
    {
      reuse = new DbusEvent(buf, position);
    }
    else
    {
      reuse.reset(buf, position);
    }

    return reuse;
  }
}
