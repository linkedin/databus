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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.databus.core.util.TimeUtils;


public class TestDbusEventV2
{
  public static final Logger LOG = Logger.getLogger(TestDbusEventV2.class.getName());
  private static final long seq = 123L;
  private static final short partitionId = 23;
  private static final long timeStamp = 234L;
  private static final short srcId = 345;
  private static final byte[] payloadSchemaMd5 = new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
  private static final byte[] crcSignature = new byte[] {45,46,47,48};
  private static final byte[] payload = "Some payload".getBytes(Charset.forName("UTF-8"));
  private static final byte[] metadataBytes = "metadata".getBytes(Charset.forName("UTF-8"));
  private static final short payloadSchemaVersion = 9;
  private static final short metadataSchemaVersion = 7;
  private static final String strKey = "key";
  private static final long longKey = 456L;
  private static final int maxEventLen = 1000;
  private static final short keySchemaVersion = 5;
  private static final byte[] keySchemaCrc = new byte[] {21, 72, 23, 90};
  private static final byte[] schemaKey = new byte[] {1, 3, 5, 7, 9, 11, 13, 17};

  @Test
  public void testDbusEventV2SerializationDeserialization() throws Exception
  {
    DbusOpcode[] opcodes = new DbusOpcode[] {DbusOpcode.UPSERT, DbusOpcode.DELETE, null};
    DbusEventKey longDek = new DbusEventKey(longKey);
    DbusEventKey strDek = new DbusEventKey(strKey.getBytes(Charset.forName("UTF-8")));
    DbusEventPart schemaKeyPart = new DbusEventPart(DbusEvent.SchemaDigestType.CRC32,
                                                    keySchemaCrc,
                                                    keySchemaVersion,
                                                    ByteBuffer.wrap(schemaKey)
                                                    );
    DbusEventKey schemaDek = new DbusEventKey(schemaKeyPart);
    DbusEventKey[] keys = new DbusEventKey[]{longDek, strDek, schemaDek};
    ByteOrder[] orders = new ByteOrder[] {ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN};
    int[] offsets = new int[]{0, 5};
    DbusEventPart md5Metadata = new DbusEventPart(DbusEvent.SchemaDigestType.MD5,
                                                  payloadSchemaMd5,
                                                  metadataSchemaVersion,
                                                  ByteBuffer.wrap(metadataBytes));
    DbusEventPart crcMetadata = new DbusEventPart(DbusEvent.SchemaDigestType.CRC32,
                                                  crcSignature,
                                                  metadataSchemaVersion,
                                                  ByteBuffer.wrap(metadataBytes));
    DbusEventPart[] metadatas = new DbusEventPart[] {null, md5Metadata, crcMetadata};

    for (DbusOpcode opcode : opcodes)
    {
      for (DbusEventPart metadata : metadatas)
      {
        DbusEventInfo evInfo = new DbusEventInfo(opcode,
                                                 seq,
                                                 partitionId,
                                                 partitionId,
                                                 timeStamp,
                                                 srcId,
                                                 payloadSchemaMd5,
                                                 payload,
                                                 false,  // enabletracing
                                                 true,  // auto-commit
                                                 DbusEventFactory.DBUS_EVENT_V2,
                                                 payloadSchemaVersion,
                                                 metadata // DbusEventPart for metadataBytes
                                                 );
        for (DbusEventKey evKey : keys)
        {
          for (int offset : offsets)
          {
            // offset is the offset into the byte buffer for serialization and de-serialization.
            // The idea is that we should be able to serialize into a non-zero offset, as well as
            // de-serialize an event that starts on a non-zero offset in a ByteBuffer.
            // We should also be able to serialize or de-serialize correctly whatever the ByteOrder of the
            // buffer.
            for (ByteOrder byteOrder : orders)
            {
              // Replicated event
              evInfo.setReplicated(true);
              testSerDeser(evKey, evInfo, offset, byteOrder);
              // Not a Replicated event
              evInfo.setReplicated(false);
              testSerDeser(evKey, evInfo, offset, byteOrder);
            }
          }
        }
      }
    }
  }

  private void testSerDeser(DbusEventKey key, DbusEventInfo evInfo, int bufferOffset, ByteOrder byteOrder) throws Exception
  {
    LOG.info("Trying keytype=" + key.getKeyType() + ",opCode=" + evInfo.getOpCode() + ",bufferOffset=" + bufferOffset + ",byteOrder=" + byteOrder);
    ByteBuffer buf = ByteBuffer.allocate(maxEventLen).order(byteOrder);
    if (bufferOffset > 0)
    {
      byte[] junk = new byte[bufferOffset];
      buf.put(junk);
    }
    final int startPos = bufferOffset;
    int eventLen = DbusEventFactory.serializeEvent(key, buf, evInfo);

    // We should not need to flip() or set the limit of the buffer since the serializer or de-serializer should
    // ignore any bytes after the serialized event.
    DbusEventInternalReadable evt = new DbusEventV2();  // TODO:  switch to DbusEventV2Factory createReadOnlyDbusEvent()
    evt = evt.reset(buf, bufferOffset);
    ByteBuffer bb = evt.getRawBytes();
    byte[] rawEvent = new byte[bb.remaining()];
    bb.get(rawEvent);
    if (evInfo.getOpCode() == null)
    {
      Assert.assertEquals(evt.getOpcode(), DbusOpcode.UPSERT);
    }
    else
    {
      Assert.assertEquals(evInfo.getOpCode(), evt.getOpcode());
    }
    Assert.assertEquals(evt.scanEvent(), DbusEventInternalReadable.EventScanStatus.OK);
    Assert.assertEquals(evt.getSourceId(), evInfo.getSrcId());
    Assert.assertEquals(evt.getPartitionId(), evInfo.getpPartitionId());
    Assert.assertEquals(evt.timestampInNanos(), evInfo.getTimeStampInNanos());
    Assert.assertEquals(evt.isTraceEnabled(), evInfo.isEnableTracing());
    Assert.assertEquals(evt.sequence(), evInfo.getSequenceId());
    Assert.assertTrue(Arrays.equals(evInfo.getSchemaId(), evt.schemaId()));
    int expectedLength = DbusEventFactory.computeEventLength(key, evInfo);
    Assert.assertEquals(eventLen, evt.size());
    Assert.assertEquals(eventLen, expectedLength);
    byte[] value = new byte[evt.payloadLength()];
    evt.value().get(value);
    if (key.getKeyType() == DbusEventKey.KeyType.LONG)
    {
      Assert.assertEquals(key.getLongKey().longValue(), evt.key());
      Assert.assertEquals(key.getLongKey().longValue(), evt.getDbusEventKey().getLongKey().longValue());
    }
    else if (key.getKeyType() == DbusEventKey.KeyType.STRING)
    {
      Assert.assertTrue(Arrays.equals(key.getStringKeyInBytes(), evt.keyBytes()));
      Assert.assertTrue(Arrays.equals(key.getStringKeyInBytes(), evt.getDbusEventKey().getStringKeyInBytes()));
    }
    else if (key.getKeyType() == DbusEventKey.KeyType.SCHEMA)
    {
      Assert.assertEquals(key.getSchemaKey(), evt.getKeyPart());
      Assert.assertEquals(key.getSchemaKey(), evt.getDbusEventKey().getSchemaKey());
    }
    else
    {
      Assert.fail("Unknown key type");
    }
    Assert.assertTrue(Arrays.equals(evInfo.getValueBytes(), value));
    //Assert.assertTrue(Arrays.equals(evInfo.getValue(), value));
    DbusEventPart metadataPart = evt.getPayloadMetadataPart();
    if (metadataPart == null)
    {
      Assert.assertNull(evInfo.getMetadata());
    }
    else
    {
      Assert.assertEquals(metadataPart.getSchemaVersion(), metadataSchemaVersion);
      Assert.assertEquals(evInfo.getMetadata().getSchemaDigestType(), metadataPart.getSchemaDigestType());
      if (metadataPart.getSchemaDigestType() == DbusEvent.SchemaDigestType.CRC32)
      {
        Assert.assertEquals(DbusEvent.CRC32_DIGEST_LEN, metadataPart.getSchemaDigest().length);
        Assert.assertTrue(Arrays.equals(metadataPart.getSchemaDigest(), crcSignature));
      }
      else if (metadataPart.getSchemaDigestType() == DbusEvent.SchemaDigestType.MD5)
      {
        Assert.assertEquals(DbusEvent.MD5_DIGEST_LEN, metadataPart.getSchemaDigest().length);
        Assert.assertTrue(Arrays.equals(metadataPart.getSchemaDigest(), payloadSchemaMd5));
      }
      else
      {
        Assert.fail("Unknown schema digest type:" + metadataPart.getSchemaDigestType());
      }

      ByteBuffer metadataBB = metadataPart.getData();
      Assert.assertEquals(metadataBytes.length, metadataBB.remaining());
      for (int i = 0; i < metadataBytes.length; i++)
      {
        Assert.assertEquals(metadataBytes[i], metadataBB.get());
      }
    }
  }

  @Test
  public void testDbusEventPartSerializationDeserialization() throws Exception
  {
    DbusEventPart part;
    byte[] dataBytes = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer data = ByteBuffer.wrap(dataBytes);

    boolean excepted = false;
    try
    {
      part = new DbusEventPart(DbusEvent.SchemaDigestType.MD5, new byte[3], payloadSchemaVersion, data);
    }
    catch(DatabusRuntimeException e)
    {
      excepted = true;
    }
    Assert.assertTrue(excepted);

    excepted = false;
    try
    {
      part = new DbusEventPart(DbusEvent.SchemaDigestType.CRC32, new byte[8], payloadSchemaVersion, data);
    }
    catch(DatabusRuntimeException e)
    {
      excepted = true;
    }
    Assert.assertTrue(excepted);
    final int[] offsets = new int[] {0,6};

    for (int offset : offsets)
    {
      part = new DbusEventPart(DbusEvent.SchemaDigestType.MD5, payloadSchemaMd5, payloadSchemaVersion, data);
      ByteBuffer encoded = ByteBuffer.allocate(100);
      encoded.position(offset);
      part.encode(encoded);
      encoded.position(offset);
      DbusEventPart decodedPart = DbusEventPart.decode(encoded);
      Assert.assertEquals(decodedPart.getSchemaDigestType(), DbusEvent.SchemaDigestType.MD5);
      Assert.assertEquals(decodedPart.getSchemaVersion(), payloadSchemaVersion);
      Assert.assertEquals(dataBytes.length, decodedPart.getData().limit() - decodedPart.getData().position());
      byte[] dst = new byte[dataBytes.length];
      decodedPart.getData().get(dst);
      Assert.assertTrue(Arrays.equals(dst, dataBytes));
    }

    for (int offset : offsets)
    {
      part = new DbusEventPart(DbusEvent.SchemaDigestType.CRC32, crcSignature, payloadSchemaVersion, data);
      ByteBuffer encoded = ByteBuffer.allocate(100);
      encoded.position(offset);
      part.encode(encoded);
      encoded.position(offset);
      DbusEventPart decodedPart = DbusEventPart.decode(encoded);
      Assert.assertEquals(decodedPart.getSchemaDigestType(), DbusEvent.SchemaDigestType.CRC32);
      Assert.assertEquals(decodedPart.getSchemaVersion(), payloadSchemaVersion);
      Assert.assertEquals(dataBytes.length, decodedPart.getData().limit() - decodedPart.getData().position());
      byte[] dst = new byte[dataBytes.length];
      decodedPart.getData().get(dst);
      Assert.assertTrue(Arrays.equals(dst, dataBytes));
    }
  }

  @Test
  public void testEopEvent() throws Exception
  {
    long seq = 9832465L;
    short partitionId = 33;
    ByteOrder[] byteOrders = {ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN};
    int[] startPositions = {0, 23};
    long now = TimeUtils.currentNanoTime();

    for (ByteOrder byteOrder : byteOrders)
    {
      DbusEventFactory eventFactory = new DbusEventV2Factory(byteOrder);
      for (int startPos : startPositions)
      {
        LOG.info(("Trying byteOrder " + byteOrder + ", startPosition " + startPos));
        ByteBuffer bb = ByteBuffer.allocate(1000).order(byteOrder);
        for (byte i = 0; i < startPos; i++)
        {
          bb.put(i);
        }
        DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                    seq,
                                                    partitionId,
                                                    partitionId,
                                                    now,
                                                    DbusEventInternalWritable.EOPMarkerSrcId,
                                                    DbusEventInternalWritable.emptyMd5,
                                                    DbusEventInternalWritable.EOPMarkerValue,
                                                    false, //enable tracing
                                                    true, // autocommit
                                                    DbusEventFactory.DBUS_EVENT_V2,
                                                    (short)0,  // payload schema version, 0 since there is no payload
                                                    null  // DbusEventPart for metadataBytes
                                                    );
        final int evtLen = eventFactory.serializeLongKeyEndOfPeriodMarker(bb,eventInfo);

        DbusEvent e = eventFactory.createReadOnlyDbusEventFromBuffer(bb, startPos);
        Assert.assertTrue(e.isEndOfPeriodMarker());
        Assert.assertNull(e.getOpcode());
        Assert.assertEquals(seq, e.sequence());
        Assert.assertEquals(partitionId, e.getPartitionId());
        Assert.assertTrue(now <= e.timestampInNanos());
        Assert.assertNull(e.getPayloadPart());
        Assert.assertEquals(evtLen, e.size());
        Assert.assertEquals(DbusEventFactory.computeEventLength(DbusEventInternalWritable.EOPMarkerKey, eventInfo),
                            evtLen, "Mismatch between computed length and serialized length");
        // TODO Check the ext repl bit after DDSDBUS-2296 is fixed.
      }
    }
  }

  @Test
  public void testNullDataEventPart() throws Exception
  {
    ByteBuffer bb = ByteBuffer.allocate(100).order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(0);
    bb.putShort((short)0);
    for (int i = 0; i < DbusEventInternalWritable.emptyMd5.length; i++)
    {
      bb.put(DbusEventInternalWritable.emptyMd5[i]);
    }
    bb.position(0);

    DbusEventPart dbPart = DbusEventPart.decode(bb);

    ByteBuffer data = dbPart.getData();
    Assert.assertEquals(data.remaining(), 0);
  }

  @Test
  public void testDbusEventWithNullPayload() throws Exception
  {
    DbusEventPart crcMetadata = new DbusEventPart(DbusEvent.SchemaDigestType.CRC32,
                                                  crcSignature,
                                                  metadataSchemaVersion,
                                                  ByteBuffer.wrap(metadataBytes));
    DbusEventInfo evInfo = new DbusEventInfo(DbusOpcode.DELETE,
                                             seq,
                                             partitionId,
                                             partitionId,
                                             timeStamp,
                                             srcId,
                                             payloadSchemaMd5,
                                             null,
                                             false,  // enabletracing
                                             true,  // auto-commit
                                             DbusEventFactory.DBUS_EVENT_V2,
                                             payloadSchemaVersion,
                                             crcMetadata
                                             );

    DbusEventKey key = new DbusEventKey(strKey.getBytes(Charset.forName("UTF-8")));
    ByteBuffer buf = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
    int eventLen = DbusEventFactory.serializeEvent(key, buf, evInfo);

    // Now decode the event
    DbusEventFactory eventFactory = new DbusEventV2Factory(ByteOrder.LITTLE_ENDIAN);
    DbusEventInternalReadable evt = eventFactory.createReadOnlyDbusEventFromBuffer(buf, 0);
    ByteBuffer newbuf = evt.getRawBytes();
    byte[] rawEvent = new byte[newbuf.remaining()];
    newbuf.get(rawEvent);

    Assert.assertEquals(evt.getOpcode(), DbusOpcode.DELETE);
    Assert.assertEquals(evt.scanEvent(), DbusEventInternalReadable.EventScanStatus.OK);
    Assert.assertEquals(evt.getSourceId(), evInfo.getSrcId());
    Assert.assertEquals(evt.getPartitionId(), evInfo.getpPartitionId());
    Assert.assertEquals(evt.timestampInNanos(), evInfo.getTimeStampInNanos());
    Assert.assertEquals(evt.isTraceEnabled(), evInfo.isEnableTracing());
    Assert.assertEquals(evt.sequence(), evInfo.getSequenceId());
    Assert.assertTrue(Arrays.equals(evInfo.getSchemaId(), evt.schemaId()));
    int expectedLength = DbusEventFactory.computeEventLength(key, evInfo);
    Assert.assertEquals(eventLen, evt.size());
    Assert.assertEquals(eventLen, expectedLength);
    byte[] value = new byte[evt.payloadLength()];
    Assert.assertEquals(0, value.length);
    // Make sure that the calls to get value work even though the value length is zero.
    evt.value().get(value);
    // Metadata assertions
    Assert.assertTrue(Arrays.equals(key.getStringKeyInBytes(), evt.keyBytes()));
    Assert.assertEquals(crcMetadata.getSchemaVersion(), metadataSchemaVersion);
    Assert.assertEquals(evInfo.getMetadata().getSchemaDigestType(), crcMetadata.getSchemaDigestType());
    Assert.assertEquals(DbusEvent.CRC32_DIGEST_LEN, crcMetadata.getSchemaDigest().length);
    Assert.assertTrue(Arrays.equals(crcMetadata.getSchemaDigest(), crcSignature));
    ByteBuffer metadataBB = crcMetadata.getData();
    Assert.assertEquals(metadataBytes.length, metadataBB.remaining());
    for (int i = 0; i < metadataBytes.length; i++)
    {
      Assert.assertEquals(metadataBytes[i], metadataBB.get());
    }

    // Make sure we can toString an event correctly.
    LOG.debug("Null payload event: " + evt.toString());
  }

  @Test
  public void testCreateErrorEvent() throws Exception
  {
    DbusEventFactory eventV1Factory = new DbusEventV1Factory();
    DbusEventFactory eventV2Factory = new DbusEventV2Factory();
    DatabusRuntimeException error = new DatabusRuntimeException(new RuntimeException("Some exception"));
    final short errorId = DbusEventInternalWritable.PULLER_RETRIES_EXPIRED;
    DbusErrorEvent errEvent = new DbusErrorEvent(error, errorId);

    DbusEventInternalReadable e1 = eventV1Factory.createErrorEvent(errEvent);
    DbusEventInternalReadable e2 = eventV2Factory.createErrorEvent(errEvent);

    Assert.assertTrue(e1.isErrorEvent());
    Assert.assertTrue(e2.isErrorEvent());
    verifyControlEvent(e1, e2, 0L, errorId);

    DbusEventSerializable.getErrorEventFromDbusEvent(e1);
    DbusEventSerializable.getErrorEventFromDbusEvent(e2);
  }

  @Test
  public void testCreateCheckpointEvent() throws Exception
  {
    DbusEventFactory eventV1Factory = new DbusEventV1Factory();
    DbusEventFactory eventV2Factory = new DbusEventV2Factory();
    Checkpoint cp = makeCheckpoint();
    DbusEventInternalReadable e1 = eventV1Factory.createCheckpointEvent(cp);
    DbusEventInternalReadable e2 = eventV2Factory.createCheckpointEvent(cp);

    Assert.assertTrue(e1.isCheckpointMessage());
    Assert.assertTrue(e2.isCheckpointMessage());
    verifyControlEvent(e1, e2, 0L, DbusEventInternalWritable.CHECKPOINT_SRCID);

    Checkpoint cp1 = DbusEventUtils.getCheckpointFromEvent(e1);
    Checkpoint cp2 = DbusEventUtils.getCheckpointFromEvent(e2);
    Assert.assertEquals(cp1, cp2);
  }

  @Test
  public void testCreateSCNRegressEvent() throws Exception
  {
    DbusEventFactory eventV1Factory = new DbusEventV1Factory();
    DbusEventFactory eventV2Factory = new DbusEventV2Factory();
    Checkpoint cp = makeCheckpoint();
    SCNRegressMessage msg = new SCNRegressMessage(cp);
    DbusEvent e1 = eventV1Factory.createSCNRegressEvent(msg);
    DbusEvent e2 = eventV2Factory.createSCNRegressEvent(msg);

    Assert.assertTrue(e1.isSCNRegressMessage());
    Assert.assertTrue(e2.isSCNRegressMessage());

    verifyControlEvent((DbusEventInternalReadable)e1, (DbusEventInternalReadable)e2, cp.getWindowScn(), DbusEventInternalWritable.SCN_REGRESS);

    DbusEventUtils.getSCNRegressFromEvent(e1);
    DbusEventUtils.getSCNRegressFromEvent(e2);
  }

  private Checkpoint makeCheckpoint() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1}");
    return cp;
  }

  private void verifyControlEvent(DbusEventInternalReadable e1, DbusEventInternalReadable e2, long seq, short srcId)
  {
    Assert.assertEquals(DbusEventInternalReadable.EventScanStatus.OK, e1.scanEvent());
    Assert.assertEquals(srcId, e1.getSourceId());
    Assert.assertTrue(e1.isControlSrcId());
    Assert.assertFalse(e1.isEndOfPeriodMarker());
    Assert.assertNull(e1.getOpcode());
    Assert.assertEquals(DbusEventInternalWritable.ZeroLongKey, e1.key());
    Assert.assertEquals(seq, e1.sequence());
    Assert.assertEquals(false, e1.isExtReplicatedEvent());

    Assert.assertEquals(DbusEventInternalReadable.EventScanStatus.OK, e2.scanEvent());
    Assert.assertEquals(srcId, e2.getSourceId());
    Assert.assertTrue(e2.isControlSrcId());
    Assert.assertFalse(e2.isEndOfPeriodMarker());
    Assert.assertNull(e2.getOpcode());
    Assert.assertEquals(DbusEventInternalWritable.ZeroLongKey, e2.key());
    Assert.assertEquals(seq, e2.sequence());
    Assert.assertEquals(false, e2.isExtReplicatedEvent());
  }
}
