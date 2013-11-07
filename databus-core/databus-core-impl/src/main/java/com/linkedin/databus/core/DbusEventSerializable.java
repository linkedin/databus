package com.linkedin.databus.core;


import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.Base64;
import com.linkedin.databus.core.util.Utils;

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

public abstract class DbusEventSerializable extends DbusEventInternalWritable
{
  private static final TypeReference<Map<String, Object>> JSON_GENERIC_MAP_TYPEREF = new TypeReference<Map<String, Object>>(){};
  public static final String MODULE = DbusEventSerializable.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  // Note that _buf and _position are reflectorized in DbusEventCorrupter,
  // so changes here (renames, moves) must be reflected there.
  protected ByteBuffer _buf;
  protected int _position;
  protected boolean _inited;

  public static int appendToEventBuffer(BufferedReader jsonStream, DbusEventBufferAppendable eventBuffer,
                                        DbusEventsStatisticsCollector statsCollector,
                                        boolean startWindow)
  throws IOException, JsonParseException, InvalidEventException
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
        int appendResult = appendToEventBuffer(jsonString, eventBuffer, objectMapper, statsCollector, endOfPeriod);
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
   * Appends a single event to the buffer. The event is expected in a json format in 'jsonString'
   * @throws java.io.IOException
   * @throws org.codehaus.jackson.JsonParseException
   * @throws com.linkedin.databus.core.InvalidEventException
   * @throws com.linkedin.databus.core.KeyTypeNotImplementedException
   */
  public static int appendToEventBuffer(String jsonString, DbusEventBufferAppendable eventBuffer,
                                        DbusEventsStatisticsCollector statsCollector,
                                        boolean startWindow)
  throws IOException, JsonParseException, InvalidEventException, KeyTypeNotImplementedException
  {
    int numEvents = appendToEventBuffer(jsonString, eventBuffer, null, statsCollector, startWindow);
    return numEvents;
  }

  /**
   * Appends a single JSON-serialized event to the buffer
   * TODO This method supports only numeric keys. Are other types of keys even needed? (RB 172113)
   * It appears that this method is used only in pre-loading oracle events into relay. For espresso
   * we do not use this path.
   * @return 0 if add failed, 1 if a regular event has been added, 2 if EOP has been added
   * */
  private static int appendToEventBuffer(String jsonString, DbusEventBufferAppendable eventBuffer,
                                         ObjectMapper objectMapper,
                                         DbusEventsStatisticsCollector statsCollector,
                                         boolean startWindow)
  throws IOException, JsonParseException, InvalidEventException, KeyTypeNotImplementedException
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

  public static DbusErrorEvent getErrorEventFromDbusEvent(DbusEventInternalReadable event)
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

  @Override
  public boolean isErrorEvent()
  {
    return (srcId() > PRIVATE_RANGE_MIN_ERROR_SRCID &&
        srcId() < PRIVATE_RANGE_MAX_ERROR_SRCID);
  }

  @Override
  public boolean isCheckpointMessage()
  {
    return (srcId() == CHECKPOINT_SRCID);
  }

  @Override
  public boolean isPrivateControlMessage()
  {
    return (srcId() <= PRIVATE_RANGE_MAX_SRCID);
  }

  @Override
  public boolean isControlMessage()
  {
    return isControlSrcId();
  }

  @Override
  public boolean isControlSrcId()
  {
    return DbusEventUtils.isControlSrcId(getSourceId());
  }

  @Override
  public boolean isEndOfPeriodMarker()
  {
    return (srcId() == EOPMarkerSrcId);
  }

  @Override
  public boolean isSCNRegressMessage()
  {
    return (srcId() == SCN_REGRESS);
  }

  @Override
  public HeaderScanStatus scanHeader()
  {
    return scanHeader(true);
  }

  @Override
  public EventScanStatus scanEvent()
  {
    return scanEvent(true);
  }

  @Override
  public boolean isValid()
  {
    return isValid(true);
  }

  /**
   * @param logErrors  whether to emit LOG.error messages for invalid results
   * @return true if event is not partial and event is valid; note that a partial event is deemed invalid
   *
   * TODO:  should this also (or instead) check _inited?
   */
  @Override
  public boolean isValid(boolean logErrors)
  {
    return (_buf != null) && (scanEvent(logErrors) == EventScanStatus.OK);
  }

  @Override
  public ByteBuffer getRawBytes()
  {
    ByteBuffer buffer = _buf.asReadOnlyBuffer().order(_buf.order());
    buffer.position(_position);
    buffer.limit(_position + size());
    return buffer;
  }

  /**
   * Serializes the event to a channel using the specified encoding
   * @param  writeChannel           the channel to write to
   * @param  encoding               the serialization encoding
   * @return the number of bytes written to the channel
   */
  @Override
  public int writeTo(WritableByteChannel writeChannel, Encoding encoding)
  {
    int bytesWritten = 0;
    switch (encoding)
    {
    case BINARY:
    {
      // write a copy of the event
      ByteBuffer writeBuffer = _buf.duplicate().order(_buf.order());
      writeBuffer.position(_position);
      writeBuffer.limit(_position+size());
      try
      {
        bytesWritten = writeChannel.write(writeBuffer);
      }
      catch (IOException e)
      {
        LOG.error("binary write error: " + e.getMessage(), e);
      }
      break;
    }
    case JSON_PLAIN_VALUE:
    case JSON:
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      JsonFactory f = new JsonFactory();
      try {
        JsonGenerator g = f.createJsonGenerator(baos, JsonEncoding.UTF8);
        g.writeStartObject();
        int version = getVersion();
        if (version == DbusEventFactory.DBUS_EVENT_V1)
        {
          writeJSON_V1(g,encoding);
        } else {
          writeJSON_V2(g,encoding);
        }

        g.writeEndObject();
        g.flush();
        baos.write("\n".getBytes());
      } catch (IOException e) {
        LOG.error("JSON write error: " + e.getMessage(), e);
      }
      ByteBuffer writeBuffer = ByteBuffer.wrap(baos.toByteArray()).order(_buf.order());
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

  private void writeJSON_V1 (JsonGenerator g, Encoding e) throws IOException {
    if (null != getOpcode())
    {
      g.writeStringField("opcode", getOpcode().toString());
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
    g.writeStringField("valueEnc", e.toString());
    g.writeBooleanField("isReplicated", isExtReplicatedEvent());
    if (isEndOfPeriodMarker())
    {
      g.writeBooleanField("endOfPeriod", true);
    }

    if (isTraceEnabled())
    {
      g.writeBooleanField("traceEnabled", true);
    }

    if (e.equals(Encoding.JSON))
    {
      g.writeStringField("value", Base64.encodeBytes(Utils.byteBufferToBytes(value())));
    }
    else
    {
      g.writeStringField("value", Utils.byteBufferToString(value()));
    }
  }

  private void writeJSON_V2 (JsonGenerator g, Encoding e) throws IOException {
    DbusEventPart metadata = getPayloadMetadataPart();
    DbusEventPart payload = getPayloadPart();
    g.writeNumberField("version", getVersion());
    g.writeNumberField("magicByte", getMagic());
    g.writeNumberField("headerCrc", headerCrc());
    g.writeNumberField("bodyCrc", bodyCrc());
    if ( getOpcode() != null)
    {
      g.writeStringField("opcode", getOpcode().toString());
    } else {
      g.writeNullField("opcode");
    }
    if ( isKeyNumber()) {
      g.writeStringField ("keyType", "Number");
      g.writeNumberField("key", key());
    } else if (isKeyString()) {
      g.writeStringField ("keyType", "String");
      g.writeStringField("keyBytes", Base64.encodeBytes(keyBytes()));
    } else if (isKeySchema()) {
      g.writeStringField("keyType","Schema");
      g.writeStringField("key", getKeyPart().toString());
    } else {
      throw new UnsupportedOperationException("Key Type Not implemented");
    }
    g.writeStringField("valueEnc", e.toString());
    g.writeBooleanField("isReplicated", isExtReplicatedEvent());
    g.writeBooleanField("traceOn", isTraceEnabled());
    g.writeBooleanField("hasPayloadMetadata", (metadata != null));
    g.writeBooleanField("hasPayload", (payload != null));
    g.writeNumberField("timestampInNanos", timestampInNanos());
    g.writeNumberField("srcId", getSourceId());
    g.writeNumberField("logicalPartitionId", getPartitionId());
    g.writeNumberField("physicalPartitionId", getPartitionId());
    g.writeNumberField("sequence", sequence());
    if (metadata != null)
    {
      metadata.printString("metadata", g, e);
    }
    if (payload != null)
    {
      payload.printString("payload", g, e);
    }
  }

  protected void resetInternal(ByteBuffer buf, int position)
  {
    verifyByteOrderConsistency(buf, "DbusEventSerializable.resetInternal()");
    _inited = true;
    _buf = buf;
    _position = position;
  }

  protected void verifyByteOrderConsistency(ByteBuffer buf, String where)
  {
    if (_inited && (buf.order() != _buf.order()))
    {
      throw new DatabusRuntimeException("ByteBuffer byte-order mismatch [" +
                                        (where != null? where : "verifyByteOrderConsistency()") + "]");
    }
  }

}
