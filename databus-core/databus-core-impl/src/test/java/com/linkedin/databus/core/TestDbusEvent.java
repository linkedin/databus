package com.linkedin.databus.core;
/*
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
 */


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus.core.DbusEventKey.KeyType;
import com.linkedin.databus.core.test.DbusEventCorrupter;
import com.linkedin.databus.core.test.DbusEventCorrupter.EventCorruptionType;
import com.linkedin.databus.core.test.DbusEventFactoryForTesting;
import com.linkedin.databus.core.util.Base64;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.Utils;


public class TestDbusEvent
{
  private static final long key = 12345L;
  private static final long timeStamp = 3456L;
  private static final short partitionId = 30;
  private static final short srcId = 15;
  private static final byte[] schemaId = "abcdefghijklmnop".getBytes();

  private static final String DATA_ROOT_DIR_PROP_NAME = "test.datadir";
  private static final String DATA_DIR_NAME = "./test_data";
  private static final String OLD_JAVA_EVENT_FILE = "DbusEventUpsertByOldJava.evt";
  private static final DbusEventFactory _eventV1Factory = new DbusEventV1Factory();
  public static final Logger LOG = Logger.getLogger(TestDbusEvent.class.getName());


  @Test
  public void testOldEvent() throws Exception
  {
    String datadirName = System.getProperty(DATA_ROOT_DIR_PROP_NAME, DATA_DIR_NAME);

    // Test an event that has been written by the previous Java implementation.
    FileInputStream fis = null;
    File oldJavaEventFile = new File(datadirName, OLD_JAVA_EVENT_FILE);
    try
    {
      fis = new FileInputStream(new File(datadirName, OLD_JAVA_EVENT_FILE));
    }
    catch (Exception e)
    {
      fail("Exception opening file input stream:" + oldJavaEventFile.getAbsolutePath());
    }
    // We know that this event is encoded in Big-endian format.
    ByteBuffer buf = ByteBuffer.allocate(1000).order(ByteOrder.BIG_ENDIAN);
    fis.getChannel().read(buf);
    buf.flip();
    DbusEventInternalReadable e = _eventV1Factory.createReadOnlyDbusEventFromBuffer(buf, 0);
    Assert.assertTrue(e.isValid());
    // This is an UPSERT event with key = 12345L, timestamp at 3456L, partitionId at 30,
    // srcId at 15, and the schemaId as "abcdefghijklmnop".getBytes();
    // Specifically, make sure that the only bit set is for the opcode.
    assertEquals(e.getOpcode(), DbusOpcode.UPSERT);
    // Previously we used to set the same bit in both bytes, so isExtReplicatedEvent() will be true for all
    // old java generated events.
    assertTrue("Replication bit not set in saved event", e.isExtReplicatedEvent());
    assertFalse("Not data event", e.isControlMessage());
    assertFalse("Trace is enabled", e.isTraceEnabled());
    assertTrue("key is not numeric", e.isKeyNumber());
    assertEquals(12345L, e.key());
    assertEquals(3456L, e.timestampInNanos());
    assertEquals(0, e.physicalPartitionId());
    assertEquals(30, e.logicalPartitionId());
    assertEquals(15, e.srcId());
    assertEquals("abcdefghijklmnop".getBytes(), e.schemaId());
  }

  //@Test
  public void testLength()
  {
    fail("Not yet implemented");
  }

  @Test
  /**
   * validate conversion from an event V2 to V1
   */
  public void testConvertV2toV1()
  {
    // long key
    DbusEventKey longKey = new DbusEventKey(54321L);
    convertV2toV1LongKey(longKey);

    // string key
    DbusEventKey byteKey = new DbusEventKey(new String("whateverkey").getBytes());
    convertV2toV1LongKey(byteKey);

  }

  @Test
  /**
   * validate conversion from an event V2 to V1
   */
  public void testConvertV2toV1EOP()
  {
    // long key
    DbusEventKey longKey = new DbusEventKey(0L);
    convertV2toV1EOP(longKey);

    // string key
    DbusEventKey byteKey = new DbusEventKey(new byte[0]);
    convertV2toV1EOP(byteKey);
  }

  public void convertV2toV1EOP(DbusEventKey dbusKey)
  {
    final DbusEventFactory eventV2Factory = new DbusEventV2Factory();  // both versions share same _byteOrder var
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(eventV2Factory.getByteOrder());

    try
    {
      DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                  0L,
                                                  partitionId,
                                                  partitionId,
                                                  System.nanoTime(),
                                                  DbusEventInternalWritable.EOPMarkerSrcId,
                                                  DbusEventInternalWritable.emptyMd5,
                                                  new byte[0],
                                                  false, //enable tracing
                                                  true, // autocommit
                                                  DbusEventFactory.DBUS_EVENT_V2,
                                                  (short)0,   // payload schema version
                                                  null        // Metadata
                                                  );
      // create v2 event
      DbusEventFactory.serializeEvent(dbusKey, serializationBuffer, eventInfo);
    }
    catch (KeyTypeNotImplementedException e2)
    {
      fail(e2.getLocalizedMessage());
    }
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue( "event V2 is invalid", e.isValid());

    // now let's convert the event
    DbusEventV2 eV2 = (DbusEventV2)e;
    DbusEventV1 eV1 = null;
    try {
      eV1 = (DbusEventV1)eV2.convertToV1();
    }  catch (KeyTypeNotImplementedException e1)  {
      fail(e1.getLocalizedMessage());
    }
    LOG.info("ev1 =" + eV1);

    // let's compare the fields
    assertTrue("event v1 is invalid", eV1.isValid(true));
    assertEquals(eV1.getVersion(), DbusEventFactory.DBUS_EVENT_V1);
    assertEquals(eV2.getVersion(), DbusEventFactory.DBUS_EVENT_V2);
    if(dbusKey.getKeyType() == KeyType.LONG)
      assertEquals(eV2.key(), eV1.key());
    else if(dbusKey.getKeyType() == KeyType.STRING) {
      assertTrue(Arrays.equals(eV2.keyBytes(), eV1.keyBytes()));
      assertEquals(eV2.keyBytesLength(), eV1.keyBytesLength());
    }
    assertEquals(eV2.getPartitionId(), eV1.getPartitionId());
    assertEquals(eV2.srcId(), eV1.srcId());
    assertEquals(eV2.isTraceEnabled(), eV1.isTraceEnabled());
    assertEquals(eV2.timestampInNanos(), eV1.timestampInNanos());
    assertEquals(eV1.valueLength(), 0);
    assertEquals(eV2.bodyCrc(), eV1.bodyCrc());

    // and these fields should be the same
    assertFalse(eV2.headerCrc() == eV1.headerCrc());
    assertFalse(eV2.size() == eV1.size());
  }

  public void convertV2toV1LongKey(DbusEventKey dbusKey)
  {
    final DbusEventFactory eventV2Factory = new DbusEventV2Factory();  // both versions share same _byteOrder var
    String randomValue = RngUtils.randomString(100);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(eventV2Factory.getByteOrder());

    try
    {
      DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                  schemaId, randomValue.getBytes(), false, false);

      // create v2 event
      eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V2);
      DbusEventFactory.serializeEvent(dbusKey, serializationBuffer, eventInfo);
    }
    catch (KeyTypeNotImplementedException e2)
    {
      fail(e2.getLocalizedMessage());
    }
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue( "event V2 is invalid", e.isValid());

    // now let's convert the event
    DbusEventV2 eV2 = (DbusEventV2)e;
    DbusEventV1 eV1 = null;
    try {
      eV1 = (DbusEventV1)eV2.convertToV1();
    }  catch (KeyTypeNotImplementedException e1)  {
      fail(e1.getLocalizedMessage());
    }
    LOG.info("ev1 =" + eV1);

    // let's compare the fields
    assertTrue("event v1 is invalid", eV1.isValid(true));
    assertEquals(eV1.getVersion(), DbusEventFactory.DBUS_EVENT_V1);
    assertEquals(eV2.getVersion(), DbusEventFactory.DBUS_EVENT_V2);
    if(dbusKey.getKeyType() == KeyType.LONG)
      assertEquals(eV2.key(), eV1.key());
    else if(dbusKey.getKeyType() == KeyType.STRING) {
      assertTrue(Arrays.equals(eV2.keyBytes(), eV1.keyBytes()));
      assertEquals(eV2.keyBytesLength(), eV1.keyBytesLength());
    }
    assertEquals(eV2.getPartitionId(), eV1.getPartitionId());
    assertEquals(eV2.srcId(), eV1.srcId());
    assertEquals(eV2.schemaId(), eV1.schemaId());
    assertEquals(eV2.isTraceEnabled(), eV1.isTraceEnabled());
    assertEquals(eV2.timestampInNanos(), eV1.timestampInNanos());
    assertEquals(eV2.valueLength(), eV1.valueLength());
    assertTrue(Arrays.equals(Utils.byteBufferToBytes(eV2.value()), Utils.byteBufferToBytes(eV1.value())));
    // and these fields should differ
    assertFalse(eV2.bodyCrc() == eV1.bodyCrc());
    assertFalse(eV2.headerCrc() == eV1.headerCrc());
    assertFalse(eV2.size() == eV1.size());
  }


  @Test
  public void testIsValid()
  {
    String randomValue = RngUtils.randomString(100);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());

    try
    {
      DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                  schemaId, randomValue.getBytes(), false, false);
      eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
      DbusEventFactory.serializeEvent(new DbusEventKey(key), serializationBuffer, eventInfo);
    }
    catch (KeyTypeNotImplementedException e2)
    {
      fail(e2.getLocalizedMessage());
    }
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());

    try
    {
      //start modifying events and check if isValid is still true;
      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.LENGTH,e);
      assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.ERR);
      assertFalse(e.isValid());
      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.LENGTH,e);
      assertTrue(e.isValid());

      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.HEADERCRC,e);
      assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.ERR);
      assertFalse(e.isValid());
      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.HEADERCRC,e);
      assertTrue(e.isValid());

      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOADCRC,e);
      //header crc uses payload crc as well!
      assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.ERR);
      assertFalse(e.isValid());
      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOADCRC,e);
      assertTrue(e.isValid());

      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOAD,e);
      assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.OK);
      assertTrue(e.scanEvent()== DbusEventInternalReadable.EventScanStatus.ERR);
      assertFalse(e.isValid());
      DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOAD,e);
      assertTrue(e.isValid());
    }
    catch (InvalidEventException e1)
    {
      fail("Not supposed to throw exception");
    }

    //now check partial events;
    int origLimit = serializationBuffer.limit();
    serializationBuffer.limit(30);
    assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.PARTIAL);
    assertTrue(e.isPartial());
    assertFalse(e.isValid());
    serializationBuffer.limit(origLimit);
    assertTrue(e.isValid());

    //another partial event - this time the value;
    serializationBuffer.limit(90);
    assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.OK);
    assertTrue(e.scanEvent()== DbusEventInternalReadable.EventScanStatus.PARTIAL);
    assertTrue(e.isPartial());
    assertFalse(e.isValid());
    serializationBuffer.limit(origLimit);
    assertTrue(e.isValid());

    int headerSize = 56;
    int preambleSize = 5;
    serializationBuffer.limit(headerSize);
    assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.PARTIAL);
    assertTrue(e.isPartial());
    //version byte + header crc;

    serializationBuffer.limit(headerSize+preambleSize);
    assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.OK);
    assertTrue(e.isPartial());
    serializationBuffer.limit(origLimit);
    assertTrue(e.isValid());

    //edge case
    serializationBuffer.limit(headerSize+preambleSize-1);
    assertTrue(e.scanHeader()== DbusEventInternalReadable.HeaderScanStatus.PARTIAL);
    assertTrue(e.isPartial());
    serializationBuffer.limit(origLimit);
    assertTrue(e.isValid());
  }

  DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                                         int maxReadBufferSize, AllocationPolicy allocationPolicy, QueuePolicy policy)
  throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxEventBufferSize);
    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setAverageEventSize(maxReadBufferSize);
    config.setAllocationPolicy(allocationPolicy.name());
    config.setQueuePolicy(policy.toString());
    return config.build();
  }

  @Test
  public void testSerialize() throws KeyTypeNotImplementedException
  {
    String randomValue = RngUtils.randomString(20);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());

    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), serializationBuffer, eventInfo);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());
    assertEquals(key,e.key());
    assertTrue(e.logicalPartitionId() == partitionId);
    assertTrue(e.srcId() == srcId);
    assertFalse("Trace disabled", e.isTraceEnabled());
    assertTrue(e.timestampInNanos() == timeStamp);
    assertTrue(Utils.byteBufferToString(e.value()).equals(randomValue));
    DbusEventInternalReadable f = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer,
                                                                                    serializationBuffer.position());
    assertFalse(f.isValid());
  }

  @Test
  public void testEspressoSerialize()
  throws Exception
  {
    // A sample espresso event key whose length changes when converted into String and then getBytes().
    final byte[] keyBytes = {-127, 0, 0, 0};
    String value = "Some data";
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo =  new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                 schemaId, value.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(keyBytes), serializationBuffer, eventInfo);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());
    assertEquals(keyBytes,e.keyBytes());
    assertTrue(e.logicalPartitionId() == partitionId);
    assertTrue(e.srcId() == srcId);
    assertFalse("Trace disabled", e.isTraceEnabled());
    assertTrue(e.timestampInNanos() == timeStamp);
    assertTrue(Utils.byteBufferToString(e.value()).equals(value));
//  f.reset(serializationBuffer, serializationBuffer.position());
    DbusEventInternalReadable f = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer,
                                                                                    serializationBuffer.position());
    assertFalse(f.isValid());
  }

  @Test
  public void testSerializeWithTraceFlag() throws KeyTypeNotImplementedException
  {
    String randomValue = RngUtils.randomString(20);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), true, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), serializationBuffer, eventInfo);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    //System.out.println(e);
    assertTrue(e.isValid());
    assertTrue(e.key() == key);
    assertTrue(e.logicalPartitionId() == partitionId);
    assertTrue(e.srcId() == srcId);
    assertTrue("Trace enabled", e.isTraceEnabled());
    assertTrue(e.timestampInNanos() == timeStamp);
    assertTrue(Utils.byteBufferToString(e.value()).equals(randomValue));
    DbusEventInternalReadable f = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer,
                                                                                    serializationBuffer.position());
    assertFalse(f.isValid());
  }

  @Test
  public void testSerializeStringKey() throws KeyTypeNotImplementedException
  {
    int length = RngUtils.randomPositiveInt() % 30;
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    String randomKey = RngUtils.randomString(length);
    String randomValue = RngUtils.randomString(20);
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(randomKey.getBytes()), serializationBuffer, eventInfo);

    validateEvent(randomKey, randomValue, serializationBuffer);

    // now test the empty key (0 len)
    String emptyKey = "";
    String emptyVal = "";
    serializationBuffer.clear();

    DbusEventV1.serializeEvent(new DbusEventKey(emptyKey.getBytes()), (short)0, partitionId, timeStamp, srcId,
                                                schemaId, emptyVal.getBytes(), false, serializationBuffer);
    validateEvent(emptyKey, emptyVal, serializationBuffer);
  }

  private void validateEvent( String randomKey, String randomValue, ByteBuffer serializationBuffer)
  {
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      e1.printStackTrace();
      fail("Not supposed to throw exception");
    }

    //System.out.println(e);
    assertTrue(e.isValid());
    byte[] keyBytes = e.keyBytes();
    assertTrue((new String(keyBytes)).equals(randomKey));
    assertTrue(e.logicalPartitionId() == partitionId);
    assertTrue(e.timestampInNanos() == timeStamp);
    assertTrue(Utils.byteBufferToString(e.value()).equals(randomValue));
    // We are trying to assert that the bytes after the event are not valid.
    // It could be that the first byte is not a valid version number, or it could be
    // that the event CRC is not valid.
    // In the extreme case, we could turn up with a valid event as well, in which case
    // this check will fail (but it would likely pass on a rerun since we're accessing
    // random memory).
    boolean caughtException = false;
    boolean validEvent = true;
    try
    {
      DbusEventInternalReadable f = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer,
                                                                                      serializationBuffer.position());
      validEvent = f.isValid();
    }
    catch (UnsupportedDbusEventVersionRuntimeException ex)
    {
      caughtException = true;
    }
    assertTrue(caughtException || !validEvent);
  }

  @Test
  public void testWriteToJSON() throws IOException, KeyTypeNotImplementedException
  {
    String randomValue = RngUtils.randomString(20);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), serializationBuffer, eventInfo);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    //System.out.println(e);
    WritableByteChannel writeChannel = null;
    File directory = new File(".");
    File writeFile = File.createTempFile("test", ".dbus", directory);
    writeChannel = Utils.openChannel(writeFile, true);
    e.writeTo(writeChannel, Encoding.JSON);
    writeChannel.close();
    writeFile.delete();
  }

  @Test
  public void testHeaderCrcLongKeyWithDirectBuffer() throws KeyTypeNotImplementedException
  {
    String randomValue = RngUtils.randomString(20);
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(5000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), directBuffer, eventInfo);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(directBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());
  }

  @Test
  public void testHeaderCrcStringKeyWithDirectBuffer() throws KeyTypeNotImplementedException
  {
    String randomValue = RngUtils.randomString(20);
    String randomKey = RngUtils.randomString(10);
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(5000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(randomKey.getBytes()), directBuffer, eventInfo);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(directBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());
  }

  //@Test
  public void testKey()
  {
    fail("Not yet implemented");
  }

  //@Test
  public void testPartitionId()
  {
    fail("Not yet implemented");
  }

  //@Test
  public void testScn()
  {
    fail("Not yet implemented");
  }

  @Test
  public void testTimestamp() throws KeyTypeNotImplementedException
  {
    String randomValue = RngUtils.randomString(20);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo =  new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                 schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), serializationBuffer, eventInfo);
    DbusEventInternalReadable e = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0);
    assertEquals("Timestamp matches", timeStamp,e.timestampInNanos());
  }

  @Test
  public void testSchemaVersion() throws KeyTypeNotImplementedException
  {
    final short SCHEMA_VERSION = 152;
    String randomValue = RngUtils.randomString(20);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    // simulate having a schemaid and overwriting the first 2 bytes with schemaVersion
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.put(schemaId);
    buffer.flip();
    // now use first two bytes for schema version
    buffer.putShort(SCHEMA_VERSION);
    buffer.flip();
    byte [] schemaVersion = buffer.array();
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaVersion, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), serializationBuffer, eventInfo);
    DbusEventInternalReadable e = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0);
    assertEquals("SchemaVersion matches", SCHEMA_VERSION,e.schemaVersion());
    assertTrue("SchemaId doesn't match", !Arrays.equals(schemaId, e.schemaId()));
    assertTrue("SchemaId (14 bytes) matches",
               Arrays.equals(Arrays.copyOfRange(schemaId, 2, schemaId.length),
                             Arrays.copyOfRange(e.schemaId(), 2, schemaId.length)));
  }

  @Test
  public void testValueCrc()
  throws Exception
  {
    String randomValue = RngUtils.randomString(20);
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(5000).order(_eventV1Factory.getByteOrder());
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp, srcId,
                                                schemaId, randomValue.getBytes(), false, false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(key), directBuffer, eventInfo);
    DbusEventInternalReadable e = _eventV1Factory.createReadOnlyDbusEventFromBuffer(directBuffer, 0);

    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(e);  // for testing only!
    writableEvent.setSequence(200L);

    // Test case 1: Set the value CRC correctly and ensure it passes
    long calcValueCRC = 0;
    try
    {
      // Set the value CRC, then reset the header CRC since it covers the value CRC:
      calcValueCRC = e.getCalculatedValueCrc();
      writableEvent.setValueCrc(calcValueCRC);
      writableEvent.applyCrc();
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());
    long actualValueCRC = e.bodyCrc();
    assertEquals("value CRCs should match", calcValueCRC, actualValueCRC);

    // Test case 2: Set the value CRC incorrectly
    try
    {
      writableEvent.setValueCrc(calcValueCRC + 1);
      writableEvent.applyCrc();
    }
    catch (Exception e1)
    {
      fail("Not supposed to throw exception");
    }
    actualValueCRC = e.bodyCrc();
    assertEquals("value CRCs should match", calcValueCRC+1, actualValueCRC);

    assertTrue(!e.isValid());
  }

  //@Test
  public void testValue()
  {
    fail("Not yet implemented");
  }

  @Test
  public void testAppendToEventBuffer_one() throws Exception
  {
    String valueStr = "testvalue";
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());

    DbusEventInfo eventInfo = new DbusEventInfo(null, 2L, (short)0, (short)3, 4L,
                                                (short)5, schemaId, valueStr.getBytes(), false,
                                                true);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    DbusEventFactory.serializeEvent(new DbusEventKey(1L), serializationBuffer, eventInfo);
    DbusEventInternalReadable event1 = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0);
    assertTrue("event crc correct", event1.isValid());
    //test JSON_PLAIN_VALUE
    ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
    WritableByteChannel jsonOutChannel = Channels.newChannel(jsonOut);
    event1.writeTo(jsonOutChannel, Encoding.JSON_PLAIN_VALUE);

    byte[] jsonBytes = jsonOut.toByteArray();
    String jsonString = new String(jsonBytes);

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> jsonMap = mapper.readValue(jsonString,
                          new TypeReference<Map<String, Object>>(){});
    assertEquals("key correct", 1L, ((Number)jsonMap.get("key")).longValue());
    assertEquals("sequence correct", 2L, ((Number)jsonMap.get("sequence")).longValue());
    assertEquals("partitionId correct", 3, ((Number)jsonMap.get("logicalPartitionId")).shortValue());
    assertEquals("timestamp correct", 4L, ((Number)jsonMap.get("timestampInNanos")).longValue());
    assertEquals("srcId correct", 5, ((Number)jsonMap.get("srcId")).longValue());
    assertEquals("schemaId correct", Base64.encodeBytes(schemaId), jsonMap.get("schemaId"));
    assertEquals("valueEnc correct", Encoding.JSON_PLAIN_VALUE.toString(),
                 jsonMap.get("valueEnc"));
    assertEquals("value correct", valueStr, jsonMap.get("value"));

    DbusEventBuffer eventBuffer1 =
        new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 10000, 1000,
                                      AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    eventBuffer1.startEvents();
    assertEquals("json deserialization", 1, DbusEventSerializable.appendToEventBuffer(jsonString,
                                                                                      eventBuffer1,
                                                                                      null,
                                                                                      false));
    eventBuffer1.endEvents(2);

    DbusEventIterator it1 = eventBuffer1.acquireIterator("it1");
    assertTrue("buffer has event", it1.hasNext());

    DbusEvent testEvent = it1.next();
    assertEquals("key correct", 1L, testEvent.key());
    assertEquals("sequence correct", 2L, testEvent.sequence());
    assertEquals("partitionId correct", 3, testEvent.logicalPartitionId());
    assertEquals("timestamp correct", 4L, testEvent.timestampInNanos());
    assertEquals("srcId correct", 5, testEvent.srcId());
    assertEquals("schemaId correct", new String(schemaId), new String(testEvent.schemaId()));
    assertEquals("value correct", valueStr, Utils.byteBufferToString(testEvent.value()));
    assertEquals("Get DbusEventKey", 1L, ((DbusEventInternalReadable)testEvent).getDbusEventKey().getLongKey().longValue());

    //test JSON
    jsonOut = new ByteArrayOutputStream();
    jsonOutChannel = Channels.newChannel(jsonOut);
    event1.writeTo(jsonOutChannel, Encoding.JSON);

    jsonBytes = jsonOut.toByteArray();
    jsonString = new String(jsonBytes);

    jsonMap = mapper.readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    assertEquals("key correct", 1L, ((Number)jsonMap.get("key")).longValue());
    assertEquals("sequence correct", 2L, ((Number)jsonMap.get("sequence")).longValue());
    assertEquals("logicalPartitionId correct", 3, ((Number)jsonMap.get("logicalPartitionId")).shortValue());
    assertEquals("timestampInNanos correct", 4L, ((Number)jsonMap.get("timestampInNanos")).longValue());
    assertEquals("srcId correct", 5, ((Number)jsonMap.get("srcId")).longValue());
    assertEquals("schemaId correct", Base64.encodeBytes(schemaId), jsonMap.get("schemaId"));
    assertEquals("valueEnc correct", Encoding.JSON.toString(), jsonMap.get("valueEnc"));
    assertEquals("value correct", Base64.encodeBytes(valueStr.getBytes()), jsonMap.get("value"));

    assertTrue("buffer has event", it1.hasNext());
    testEvent = it1.next();
    assertTrue("end of window", testEvent.isEndOfPeriodMarker());

    DbusEventBuffer eventBuffer2 =
        new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 10000, 1000,
                                      AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    eventBuffer2.startEvents();
    assertTrue("json deserialization", (DbusEventSerializable.appendToEventBuffer(jsonString, eventBuffer2, null, false) > 0));
    eventBuffer2.endEvents(2);

    DbusEventIterator it2 = eventBuffer2.acquireIterator("it2");
    assertTrue("buffer has event", it2.hasNext());

    testEvent = it2.next();
    assertEquals("key correct", 1L, testEvent.key());
    assertEquals("partitionId correct", 3, testEvent.logicalPartitionId());
    assertEquals("timestamp correct", 4L, testEvent.timestampInNanos());
    assertEquals("srcId correct", 5, testEvent.srcId());
    assertEquals("schemaId correct", new String(schemaId), new String(testEvent.schemaId()));
    assertEquals("value correct", valueStr, Utils.byteBufferToString(testEvent.value()));
    assertEquals("Get DbusEventKey", 1L, ((DbusEventInternalReadable)testEvent).getDbusEventKey().getLongKey().longValue());

    assertTrue("buffer has event", it2.hasNext());
    testEvent = it2.next();
    assertTrue("end of window", testEvent.isEndOfPeriodMarker());
  }

  @Test
  public void testAppendToEventBuffer_many() throws Exception
  {
    ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    WritableByteChannel jsonOutChannel = Channels.newChannel(jsonOut);
    for (int i = 0; i < 10; ++i)
    {
      int savePosition = serializationBuffer.position();
      String valueStr = "eventValue" + i;
      DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, (short)(i + 3), 4L + i, (short)(5 + i),
                                                  schemaId, valueStr.getBytes(), false, false);
      eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
      DbusEventFactory.serializeEvent(new DbusEventKey(1L + i), serializationBuffer, eventInfo);
      DbusEventInternalReadable event = _eventV1Factory.createReadOnlyDbusEventFromBuffer(serializationBuffer,
                                                                                          savePosition);
      event.writeTo(jsonOutChannel, Encoding.JSON);
    }

    byte[] jsonBytes = jsonOut.toByteArray();
    String jsonString = new String(jsonBytes);
    BufferedReader jsonIn = new BufferedReader(new StringReader(jsonString));
    DbusEventBuffer eventBuffer1 =
        new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 10000, 1000,
                                      AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    eventBuffer1.startEvents();
    assertEquals("jsonDeserialization", 10, DbusEventSerializable.appendToEventBuffer(jsonIn, eventBuffer1, null, false));
    eventBuffer1.endEvents(2);

    DbusEventIterator eventIter = eventBuffer1.acquireIterator("it1");
    for (int i = 0; i < 10; ++i)
    {
      String valueStr = "eventValue" + i;
      assertTrue("buffer has event " + i, eventIter.hasNext());
      DbusEvent testEvent = eventIter.next();

      assertEquals("key correct for event " + i, 1L + i, testEvent.key());
      assertEquals("partitionId correct for event " + i, 3 + i, testEvent.logicalPartitionId());
      assertEquals("timestamp correct for event " + i, 4L + i, testEvent.timestampInNanos());
      assertEquals("srcId correct for event " + i, 5 + i, testEvent.srcId());
      assertEquals("schemaId correct for event " + i, new String(schemaId), new String(testEvent.schemaId()));
      assertEquals("value correct for event " + i, valueStr, Utils.byteBufferToString(testEvent.value()));
    }

    assertTrue("buffer has event", eventIter.hasNext());
    DbusEvent testEvent = eventIter.next();
    assertTrue("end of window", testEvent.isEndOfPeriodMarker());
  }

  /* This test exercises various code paths involving string keys, most of
   * which are not terribly exciting, but the main goal is improved code
   * coverage.
   */
  @Test
  public void testStringKey() throws Exception
  {
    String stringKey = "TenCharKey";
    String randomValue = RngUtils.randomString(20);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(_eventV1Factory.getByteOrder());
    int numbytes = 0;
    try
    {
      DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 0L, (short)0, partitionId, timeStamp,
                                                  srcId, schemaId, randomValue.getBytes(), true /* enableTracing */,
                                                  false /*autoCommit */);
      eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
      numbytes = DbusEventFactory.serializeEvent(new DbusEventKey(stringKey.getBytes()), serializationBuffer, eventInfo);
    }
    catch (Exception e1)
    {
      fail("Unexpected exception serializing UPSERT event with string key: " + e1.getMessage());
    }
    assertTrue("number of bytes serialized should be positive", numbytes > 0);
    DbusEventInternalReadable e = null;
    try
    {
      e = DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(serializationBuffer, 0, 200L,
                                                                       DbusEventFactory.DBUS_EVENT_V2);
    }
    catch (Exception e1)
    {
      fail("Unexpected exception creating event.");
    }
    assertTrue("event should be valid", e.isValid());
    assertTrue("key type should be string", !e.isKeyNumber());
    assertTrue("key length should match (or exceed?) string length", e.keyBytesLength() >= stringKey.length());
    // this is the sole V1-specific part of this test (not that V2 exists yet...):
    assertEquals("header length should match size of header with string key", 52, ((DbusEventV1)e).headerLength());
    // TODO:  repeat this but with header-scan error and with bodyCrc failure:
    assertEquals("event should scan OK", DbusEventInternalReadable.EventScanStatus.OK, e.scanEvent(false));
    String stringyEvent = e.toString();
    // TODO:  add some sort of substring test for something in stringyEvent
    //        (maybe change key to non-random thing, e.g., "TenCharKey")
    assertEquals(stringKey.getBytes(), e.keyBytes());
    assertEquals("Get DbusEventKey",stringKey.getBytes(), ((DbusEventInternalReadable)e).getDbusEventKey().getStringKeyInBytes());

    // test both string-key and trace-enabled portions of writeTo():
    try
    {
      WritableByteChannel writeChannel = null;
      File directory = new File(".");
      File writeFile = File.createTempFile("test", ".dbus", directory);
      writeChannel = Utils.openChannel(writeFile, true);
      e.writeTo(writeChannel, Encoding.JSON);
      writeChannel.close();
      writeFile.delete();
    }
    catch (Exception e1)
    {
      fail("Unexpected exception writing string-key event to channel: " + e1.getMessage());
    }

    // test DELETE opcode and autocommit == true:
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.DELETE,
                                                0L,
                                                (short)0,
                                                partitionId,
                                                timeStamp,
                                                srcId,
                                                schemaId,
                                                randomValue.getBytes(),
                                                false /* enableTracing */,
                                                true /* autocommit */);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    try
    {
      numbytes = DbusEventFactory.serializeEvent(new DbusEventKey(stringKey.getBytes()),
                                                 serializationBuffer,
                                                 eventInfo);
    }
    catch (Exception e1)
    {
      fail("Unexpected exception serializing DELETE event: " + e1.getMessage());
    }

    // test 3-arg serializeFullEvent(); returns zero rather than throwing exception:
    numbytes = DbusEventFactory.serializeEvent(new DbusEventKey(stringKey.getBytes()),
                                                   serializationBuffer,
                                                   eventInfo);
    assertTrue("numbytes for serialized event should be non-zero", numbytes > 0);

    // test null opcode:
    eventInfo = new DbusEventInfo(null,
                                  0L,
                                  (short)0,
                                  partitionId,
                                  timeStamp,
                                  srcId,
                                  schemaId,
                                  randomValue.getBytes(),
                                  false /* enableTracing */,
                                  false /* autocommit */);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    assertEquals("null opcode should still be null", null, eventInfo.getOpCode());
    try
    {
      numbytes = DbusEventV1.serializeEvent(new DbusEventKey(stringKey.getBytes()),
                                            serializationBuffer,
                                            eventInfo);
    }
    catch (Exception e1)
    {
      fail("Unexpected exception serializing event with null opcode: " + e1.getMessage());
    }
    assertEquals("Event with null opcode should default to UPSERT", DbusOpcode.UPSERT, e.getOpcode());
  }

  /* This just tests an exception-printing helper method and is even less exciting. */
  @Test
  public void testGetStringFromBuffer() throws Exception
  {
    final String verificationStr1 = "{\"message\":null,\"error\":\"java.lang.NullPointerException\"}";
    final String verificationStr2 = "{\"message\":\"Error in _bufferOffset\",\"error\":\"java.lang.RuntimeException\"}\n";

    // {"message":null,"error":"java.lang.NullPointerException"} plus 16 junk bytes at start (-1) and 7 at end (-128)
    final byte[] jsonEncodedException1 = {
        -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1,   -1, // 16 junk bytes
      0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x3a, 0x6e, 0x75, 0x6c, 0x6c, 0x2c,
      0x22, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x3a, 0x22, 0x6a, 0x61, 0x76, 0x61, 0x2e, 0x6c, 0x61,
      0x6e, 0x67, 0x2e, 0x4e, 0x75, 0x6c, 0x6c, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x45, 0x78,
      0x63, 0x65, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x7d, -128, -128, -128, -128, -128, -128, -128  // 7 junk bytes
    };
    // {"message":"Error in _bufferOffset","error":"java.lang.RuntimeException"} (73 + newline + 16 (-2) + 6 (-127))
    final byte[] jsonEncodedException2 = {
        -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2,   -2, // 16 junk bytes
      0x7b, 0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x3a, 0x22, 0x45, 0x72, 0x72, 0x6f,
      0x72, 0x20, 0x69, 0x6e, 0x20, 0x5f, 0x62, 0x75, 0x66, 0x66, 0x65, 0x72, 0x4f, 0x66, 0x66, 0x73,
      0x65, 0x74, 0x22, 0x2c, 0x22, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x3a, 0x22, 0x6a, 0x61, 0x76,
      0x61, 0x2e, 0x6c, 0x61, 0x6e, 0x67, 0x2e, 0x52, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x45, 0x78,
      0x63, 0x65, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x7d, 0x0a, -127, -127, -127, -127, -127, -127  // 6 junk bytes
    };

    ByteBuffer bb1 = ByteBuffer.wrap(jsonEncodedException1, 16, 57);
    assertEquals("failed to find \"version 123\" at expected ByteBuffer offset (wrap())", 123 /* 0x7b */, bb1.get(16));

    ByteBuffer bb2 = ByteBuffer.allocate(128);
    bb2.position(3);  // anywhere but offset zero
    bb2.put(jsonEncodedException2, 16, 74);
    ByteBuffer bb2ro = bb2.asReadOnlyBuffer();
    assertEquals("failed to find \"version 123\" at expected ByteBuffer offset (put())", 123 /* 0x7b */, bb2ro.get(3));

    // sanity checks to make sure we test both code paths
    assertTrue("wrapped byte-array ByteBuffer should have a backing array", bb1.hasArray());
    assertFalse("read-only ByteBuffer shouldn't have a backing array", bb2ro.hasArray());

    String jsonEncodedExceptionString1 = DbusEventFactory.getStringFromBuffer(bb1, 16);
    String jsonEncodedExceptionString2 = DbusEventFactory.getStringFromBuffer(bb2ro, 3);

    assertEquals("JSON string from wrapped byte-array ByteBuffer doesn't match expected",
                 verificationStr1, jsonEncodedExceptionString1);
    assertEquals("JSON string from bulk-put, read-only ByteBuffer doesn't match expected",
                 verificationStr2, jsonEncodedExceptionString2);
  }
}
