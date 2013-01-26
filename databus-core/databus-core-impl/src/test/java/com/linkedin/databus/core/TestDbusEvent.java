package com.linkedin.databus.core;


import com.linkedin.databus.core.DbusEvent.EventScanStatus;
import com.linkedin.databus.core.DbusEvent.HeaderScanStatus;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.util.Base64;
import com.linkedin.databus.core.util.DbusEventCorrupter;
import com.linkedin.databus.core.util.DbusEventCorrupter.EventCorruptionType;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.Utils;
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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;


public class TestDbusEvent {

	private static final long key = 12345L;
	private static final long timeStamp = 3456L;
	private static final short partitionId = 30;
	private static final short srcId = 15;
	private static final byte[] schemaId = "abcdefghijklmnop".getBytes();

  private static final String DATA_ROOT_DIR_PROP_NAME = "test.datadir";
  private static final String DATA_DIR_NAME = "./test_data";
  private static final String OLD_JAVA_EVENT_FILE = "DbusEventUpsertByOldJava.evt";
  public static final Logger LOG = Logger.getLogger(TestDbusEvent.class.getName());

  @Test
  public void testOldEvent() throws Exception
  {
    String datadirName = System.getProperty(DATA_ROOT_DIR_PROP_NAME, DATA_DIR_NAME);

    // Test an event that has been written by the previous Java implementation.
    FileInputStream fis = null;
    File oldJavaEventFile = new File(datadirName, OLD_JAVA_EVENT_FILE);
    try {
      fis = new FileInputStream(new File(datadirName, OLD_JAVA_EVENT_FILE));
    } catch (Exception e)
    {
      fail("Exception opening file input stream:" + oldJavaEventFile.getAbsolutePath());
    }
    // We know that this event is encoded in Big-endian format.
    ByteBuffer buf = ByteBuffer.allocate(1000).order(ByteOrder.BIG_ENDIAN);
    fis.getChannel().read(buf);
    buf.flip();
    DbusEventInternalWritable e = new DbusEventV1();
    e.reset(buf, 0);
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
  public void testLength() {
    fail("Not yet implemented");
  }

	@Test
	public void testIsValid() {
		String randomValue = RngUtils.randomString(100);
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);

        try
        {
          DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, serializationBuffer);
        }
        catch (KeyTypeNotImplementedException e2)
        {
          // TODO Auto-generated catch block
          e2.printStackTrace();
          assertTrue(false);
        }
        DbusEventInternalWritable e = new DbusEventV1();
        e.reset(serializationBuffer, 0);
        e.setSequence(200L);
          try {
            e.applyCrc();
        }
        catch (Exception e1) {
            fail("Not supposed to throw exception");
        }
        assertTrue(e.isValid());

        //start modifying events and check if isValid is still true;
        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.LENGTH,e);
        assertTrue(e.scanHeader()==HeaderScanStatus.ERR);
        assertFalse(e.isValid());
        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.LENGTH,e);
        assertTrue(e.isValid());

        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.HEADERCRC,e);
        assertTrue(e.scanHeader()==HeaderScanStatus.ERR);
        assertFalse(e.isValid());
        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.HEADERCRC,e);
        assertTrue(e.isValid());

        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOADCRC,e);
        //header crc uses payload crc as well!
        assertTrue(e.scanHeader()==HeaderScanStatus.ERR);
        assertFalse(e.isValid());
        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOADCRC,e);
        assertTrue(e.isValid());

        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOAD,e);
        assertTrue(e.scanHeader()==HeaderScanStatus.OK);
        assertTrue(e.scanEvent()==EventScanStatus.ERR);
        assertFalse(e.isValid());
        DbusEventCorrupter.toggleEventCorruption(EventCorruptionType.PAYLOAD,e);
        assertTrue(e.isValid());

        //now check partial events;
        int origLimit = serializationBuffer.limit();
        serializationBuffer.limit(30);
        assertTrue(e.scanHeader()==HeaderScanStatus.PARTIAL);
        assertTrue(e.isPartial());
        assertFalse(e.isValid());
        serializationBuffer.limit(origLimit);
        assertTrue(e.isValid());

        //another partial event - this time the value;
        serializationBuffer.limit(90);
        assertTrue(e.scanHeader()==HeaderScanStatus.OK);
        assertTrue(e.scanEvent()==EventScanStatus.PARTIAL);
        assertTrue(e.isPartial());
        assertFalse(e.isValid());
        serializationBuffer.limit(origLimit);
        assertTrue(e.isValid());

        int headerSize = 56;
        int preambleSize = 5;
        serializationBuffer.limit(headerSize);
        assertTrue(e.scanHeader()==HeaderScanStatus.PARTIAL);
        assertTrue(e.isPartial());
        //magic byte + header crc;

        serializationBuffer.limit(headerSize+preambleSize);
        assertTrue(e.scanHeader()==HeaderScanStatus.OK);
        assertTrue(e.isPartial());
        serializationBuffer.limit(origLimit);
        assertTrue(e.isValid());

        //edge case
        serializationBuffer.limit(headerSize+preambleSize-1);
        assertTrue(e.scanHeader()==HeaderScanStatus.PARTIAL);
        assertTrue(e.isPartial());
        serializationBuffer.limit(origLimit);
        assertTrue(e.isValid());

	}

	DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
	                                         int maxReadBufferSize, AllocationPolicy allocationPolicy, QueuePolicy policy) throws InvalidConfigException
	{
	    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
	    config.setMaxSize(maxEventBufferSize);
	    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
	    config.setScnIndexSize(maxIndexSize);
	    config.setReadBufferSize(maxReadBufferSize);
	    config.setAllocationPolicy(allocationPolicy.name());
	    config.setQueuePolicy(policy.toString());
	    return config.build();
	}

	@Test
	public void testSerialize() throws KeyTypeNotImplementedException {
		String randomValue = RngUtils.randomString(20);
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);

		DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, serializationBuffer);
		DbusEventInternalWritable e = new DbusEventV1();
		e.reset(serializationBuffer, 0);
		e.setSequence(200L);
	      try {
			e.applyCrc();
		}
		catch (Exception e1) {
			fail("Not supposed to throw exception");
		}
		assertTrue(e.isValid());
		assertEquals(key,e.key());
		assertTrue(e.logicalPartitionId() == partitionId);
		assertTrue(e.srcId() == srcId);
		assertFalse("Trace disabled", e.isTraceEnabled());
		assertTrue(e.timestampInNanos() == timeStamp);
		assertTrue(Utils.byteBufferToString(e.value()).equals(randomValue));
		DbusEventInternalWritable f = new DbusEventV1();
		f.reset(serializationBuffer, serializationBuffer.position());
		assertFalse(f.isValid());
	}

  @Test
  public void testEspressoSerialize()
  throws Exception
  {
    // A sample espresso event key whose length changes when converted into String and then getBytes().
    final byte[] keyBytes = {-127, 0, 0, 0};
    String value = "Some data";
    DbusEventKey bkey = new DbusEventKey(keyBytes);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
    DbusEventV1.serializeEvent(bkey, (short)0, partitionId, timeStamp, srcId, schemaId, value.getBytes(), false, serializationBuffer);

    DbusEventInternalWritable e = new DbusEventV1();
    e.reset(serializationBuffer, 0);
    e.setSequence(200L);
    try {
      e.applyCrc();
    }
    catch (Exception e1) {
      fail("Not supposed to throw exception");
    }
    assertTrue(e.isValid());
    assertEquals(keyBytes,e.keyBytes());
    assertTrue(e.logicalPartitionId() == partitionId);
    assertTrue(e.srcId() == srcId);
    assertFalse("Trace disabled", e.isTraceEnabled());
    assertTrue(e.timestampInNanos() == timeStamp);
    assertTrue(Utils.byteBufferToString(e.value()).equals(value));
    DbusEventInternalWritable f = new DbusEventV1();
    f.reset(serializationBuffer, serializationBuffer.position());
    assertFalse(f.isValid());
  }

	@Test
	public void testSerializeWithTraceFlag() throws KeyTypeNotImplementedException {
	    String randomValue = RngUtils.randomString(20);
	    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
	    DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), true, serializationBuffer);
	    DbusEventInternalWritable e = new DbusEventV1();
	    e.reset(serializationBuffer, 0);
	    e.setSequence(200L);
	    try {
	        e.applyCrc();
	    }
	    catch (Exception e1) {
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
	    DbusEventInternalWritable f = new DbusEventV1();
	    f.reset(serializationBuffer, serializationBuffer.position());
	    assertFalse(f.isValid());
	}

	@Test
	public void testSerializeStringKey() throws KeyTypeNotImplementedException {
	    int length = RngUtils.randomPositiveInt() % 30;
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
	    String randomKey = RngUtils.randomString(length);
	    String randomValue = RngUtils.randomString(20);
	    DbusEventV1.serializeEvent(new DbusEventKey(randomKey), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, serializationBuffer);
	     
	    validateEvent(randomKey, randomValue, serializationBuffer);
	     
	    // now test the empty key (0 len)
	    String emptyKey = "";
	    String emptyVal = "";
	    serializationBuffer.clear();
	     
	    DbusEventV1.serializeEvent(new DbusEventKey(emptyKey), (short)0, partitionId, timeStamp, srcId, schemaId, emptyVal.getBytes(), false, serializationBuffer);
	    validateEvent(emptyKey, emptyVal, serializationBuffer);
	}

	private void validateEvent( String randomKey, String randomValue, ByteBuffer serializationBuffer) {
        DbusEventInternalWritable e = new DbusEventV1();
        e.reset(serializationBuffer, 0);
        e.setSequence(200L);
        try {
            e.applyCrc();
        }
        catch (Exception e1) {
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
	    DbusEventInternalWritable f = new DbusEventV1();
	    f.reset(serializationBuffer, serializationBuffer.position());
	    assertFalse(f.isValid());
	}

	@Test
	public void testWriteToJSON() throws IOException, KeyTypeNotImplementedException {
	    String randomValue = RngUtils.randomString(20);
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
	    DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, serializationBuffer);
	    DbusEventInternalWritable e = new DbusEventV1();
	    e.reset(serializationBuffer, 0);
	    e.setSequence(200L);
		try {
			e.applyCrc();
		}
		catch (Exception e1) {
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
	public void testHeaderCrcLongKeyWithDirectBuffer() throws KeyTypeNotImplementedException {
	    String randomValue = RngUtils.randomString(20);   
	    ByteBuffer directBuffer = ByteBuffer.allocateDirect(5000).order(DbusEventV1.byteOrder);
        DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, directBuffer);
        DbusEventInternalWritable e = new DbusEventV1();
        e.reset(directBuffer, 0);
        e.setSequence(200L);
        try {
            e.applyCrc();
        }
        catch (Exception e1) {
            fail("Not supposed to throw exception");
        }
        assertTrue(e.isValid());
	}

	@Test
	public void testHeaderCrcStringKeyWithDirectBuffer() throws KeyTypeNotImplementedException {
	    String randomValue = RngUtils.randomString(20);
	    String randomKey = RngUtils.randomString(10);
	    ByteBuffer directBuffer = ByteBuffer.allocateDirect(5000).order(DbusEventV1.byteOrder);
	    DbusEventV1.serializeEvent(new DbusEventKey(randomKey), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, directBuffer);
	    DbusEventInternalWritable e = new DbusEventV1();
	    e.reset(directBuffer, 0);
	    e.setSequence(200L);
	    try {
	        e.applyCrc();
	    }
	    catch (Exception e1) {
	        fail("Not supposed to throw exception");
	    }
	    assertTrue(e.isValid());
	}


	//@Test
	public void testKey() {
		fail("Not yet implemented");
	}

	//@Test
	public void testPartitionId() {
		fail("Not yet implemented");
	}

	//@Test
	public void testScn() {
		fail("Not yet implemented");
	}

	@Test
	public void testTimestamp() throws KeyTypeNotImplementedException {
	    String randomValue = RngUtils.randomString(20);
  	    ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
	    DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaId, randomValue.getBytes(), false, serializationBuffer);
	    DbusEventInternalWritable e = new DbusEventV1();
	    e.reset(serializationBuffer, 0);
	    assertEquals("Timestamp matches", timeStamp,e.timestampInNanos());
	}

	@Test
	public void testSchemaVersion() throws KeyTypeNotImplementedException {
	    final short SCHEMA_VERSION = 152;
	    String randomValue = RngUtils.randomString(20);
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
        // simulate having a schemaid and overwriting the first 2 bytes with schemaVersion
	    ByteBuffer buffer = ByteBuffer.allocate(16);
	    buffer.put(schemaId);
	    buffer.flip();
	    // now use first two bytes for schema version
	    buffer.putShort(SCHEMA_VERSION);
	    buffer.flip();
	    byte [] schemaVersion = buffer.array();
	    DbusEventV1.serializeEvent(new DbusEventKey(key), (short)0, partitionId, timeStamp, srcId, schemaVersion, randomValue.getBytes(), false, serializationBuffer);
	    DbusEventInternalWritable e = new DbusEventV1();
	    e.reset(serializationBuffer, 0);
	    assertEquals("SchemaVersion matches", SCHEMA_VERSION,e.schemaVersion());
	    assertTrue("SchemaId doesn't match",  ! Arrays.equals(schemaId, e.schemaId()));
	    assertTrue("SchemaId (14 bytes) matches", Arrays.equals(
	               Arrays.copyOfRange(schemaId, 2, schemaId.length),
	               Arrays.copyOfRange(e.schemaId(), 2, schemaId.length)));
	}
	
	//@Test
	public void testValueCrc() {
		fail("Not yet implemented");
	}

	//@Test
	public void testValue() {
		fail("Not yet implemented");
	}

	@Test
	public void testAppendToEventBuffer_one() throws Exception
	{
   	    String valueStr = "testvalue";
		ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
	  
	    DbusEventInfo eventInfo = new DbusEventInfo(null, 2L, (short)0, (short)3, 4L,
	                                              (short)5, schemaId, valueStr.getBytes(), false, 
	                                              true);
        DbusEventV1.serializeFullEvent(new DbusEventKey(1L), serializationBuffer, eventInfo);
        DbusEventInternalWritable event1 = new DbusEventV1(serializationBuffer, 0);
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

        DbusEventBuffer eventBuffer1 = new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 10000, 1000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
        eventBuffer1.startEvents();
        assertEquals("json deserialization", 1, DbusEventV1.appendToEventBuffer(jsonString, eventBuffer1, null, false));
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

        DbusEventBuffer eventBuffer2 = new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 10000, 1000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
        eventBuffer2.startEvents();
        assertTrue("json deserialization", (DbusEventV1.appendToEventBuffer(jsonString, eventBuffer2, null, false) > 0));
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

        assertTrue("buffer has event", it2.hasNext());
        testEvent = it2.next();
        assertTrue("end of window", testEvent.isEndOfPeriodMarker());
    } 

    @Test
    public void testAppendToEventBuffer_many() throws Exception
    {
        ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
        ByteBuffer serializationBuffer = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
        WritableByteChannel jsonOutChannel = Channels.newChannel(jsonOut);
        for (int i = 0; i < 10; ++i)
        {
            int savePosition = serializationBuffer.position();
            String valueStr = "eventValue" + i;
            DbusEventV1.serializeEvent(new DbusEventKey(1L + i), (short)0, (short)(i + 3), 4L + i, (short)(5 + i),
                                 schemaId, valueStr.getBytes(), false, serializationBuffer);
            DbusEventInternalWritable event = new DbusEventV1(serializationBuffer, savePosition);
            event.writeTo(jsonOutChannel, Encoding.JSON);
        }

        byte[] jsonBytes = jsonOut.toByteArray();
        String jsonString = new String(jsonBytes);
        BufferedReader jsonIn = new BufferedReader(new StringReader(jsonString));
        DbusEventBuffer eventBuffer1 = new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 10000, 1000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));

        eventBuffer1.startEvents();
        assertEquals("jsonDeserialization", 10, DbusEventV1.appendToEventBuffer(jsonIn, eventBuffer1, null, false));
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
}
