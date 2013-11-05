package com.linkedin.databus.client.consumer;

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


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.Assert;

import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventPart;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.test.TestUtil;

public class TestInternalMetadata
{
  public static final Logger LOG = Logger.getLogger(TestInternalMetadata.class.getName());

  // what /register response would include:
  private static final String CORRECT_METADATA_SCHEMA = "{\"type\":\"record\",\"name\":\"metadata\",\"namespace\":\"com.linkedin.events.espresso\",\"version\":1,\"fields\":[{\"name\":\"etag\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":[\"null\",\"int\"]},{\"name\":\"expires\",\"type\":[\"null\",\"long\"]}]}";
  private static final String INCORRECT_METADATA_SCHEMA = "{\"type\":\"record\",\"name\":\"metadata\",\"namespace\":\"com.linkedin.events.espresso\",\"version\":1,\"fields\":[{\"name\":\"etag\",\"type\":\"long\"},{\"name\":\"flags\",\"type\":[\"null\",\"string\"]},{\"name\":\"expires\",\"type\":[\"null\",\"int\"]}]}";

  // what onDataEvent() would include implicitly (DbusEvent bits):
  private static final long SCN = 5984881823629L;
  private static final long LONG_KEY = 456L;
  private static final long TIMESTAMP_IN_NANOS = 1370483609111000000L;
  private static final short PARTITION_ID = 23;
  private static final short SOURCE_ID = 345;
  private static final short METADATA_SCHEMA_VERSION = 1;  // matches "version" value within CORRECT_METADATA_SCHEMA
  private static final byte[] METADATA_SCHEMA_CHECKSUM = new byte[] {45,46,47,48};  // incorrect (but not checked)
  private static final short PAYLOAD_SCHEMA_VERSION = 9;
  private static final byte[] PAYLOAD_SCHEMA_CHECKSUM = new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
  private static final byte[] PAYLOAD = "Some payload".getBytes(Charset.forName("UTF-8"));

  @BeforeClass
  public void setUpClass()
  {
    Date now = new Date();
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    TestUtil.setupLogging(true, "TestInternalMetadata-" + dateFormatter.format(now) + ".log", Level.ERROR);
  }

  // construction of METADATA_BYTES:
  //   cat metadata-schema.avsc | tr '\012' ' ' | sed -e 's/  *//g' -e 's/"/\\"/g' -e 's/$/\n/'
  //   contents of metadata-schema.avsc = println(CORRECT_METADATA_SCHEMA) = subset of wiki:  Espresso+Metadata+Schema
  //   java -jar /path/to/avro-tools-1.7.3.jar jsontofrag "`cat metadata-schema.avsc`" metadata-record.json | hexdump -C
  //   contents of metadata-record.json = {"etag":"dunno what an etag is","flags":null,"expires":{"long":1366150681}}
  private static final byte[] METADATA_BYTES = new byte[]
  {
    0x2a, 0x64, 0x75, 0x6e, 0x6e, 0x6f, 0x20, 0x77, 0x68, 0x61, 0x74, 0x20, 0x61, 0x6e, 0x20, 0x65,
    0x74, 0x61, 0x67, 0x20, 0x69, 0x73, 0x00, 0x02, (byte)0xb2, (byte)0xb8, (byte)0xee, (byte)0x96, 0x0a 
  };

  private static final int maxEventLen = 1000;

  private DbusEventPart createMetadataPart()
  {
    return new DbusEventPart(DbusEvent.SchemaDigestType.CRC32,
                             METADATA_SCHEMA_CHECKSUM,
                             METADATA_SCHEMA_VERSION,
                             ByteBuffer.wrap(METADATA_BYTES));
  }

  private DbusEvent createEvent(DbusEventPart metadataPart)
  throws Exception
  {
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
                                                SCN,
                                                PARTITION_ID,
                                                PARTITION_ID,
                                                TIMESTAMP_IN_NANOS,
                                                SOURCE_ID,
                                                PAYLOAD_SCHEMA_CHECKSUM,
                                                PAYLOAD,
                                                false,  // enable tracing
                                                true,  // auto-commit
                                                DbusEventFactory.DBUS_EVENT_V2,
                                                PAYLOAD_SCHEMA_VERSION,
                                                metadataPart);
    DbusEventFactory eventFactory = new DbusEventV2Factory();
    DbusEventKey eventKey = new DbusEventKey(LONG_KEY);
    ByteBuffer serialBuf = ByteBuffer.allocate(maxEventLen).order(eventFactory.getByteOrder());
    DbusEventFactory.serializeEvent(eventKey, serialBuf, eventInfo);

    return eventFactory.createReadOnlyDbusEventFromBuffer(serialBuf, 0);
  }

  private DbusEventAvroDecoder createDecoder(VersionedSchemaSet metadataSchemaSet)
  {
    VersionedSchemaSet sourceSchemaSet = new VersionedSchemaSet();  // empty is OK for our purposes
    return new DbusEventAvroDecoder(sourceSchemaSet, metadataSchemaSet);
  }

  /**
   * Verifies that getMetadata() returns the expected GenericRecord for the event's
   * metadata and that it has the expected fields and values in it.
   */
  @Test
  public void testGetMetadata_HappyPath()
  throws Exception
  {
    LOG.info("starting testGetMetadata_HappyPath()");

    // build the event's metadata and then the event
    DbusEventPart metadataPart = createMetadataPart();
    DbusEvent event = createEvent(metadataPart);

    // create a metadata schema set that correctly corresponds to the metadata
    VersionedSchemaSet metadataSchemaSet = new VersionedSchemaSet();
    metadataSchemaSet.add(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE,
                          metadataPart.getSchemaVersion(),              // METADATA_SCHEMA_VERSION
                          new SchemaId(metadataPart.getSchemaDigest()), // METADATA_SCHEMA_CHECKSUM
                          CORRECT_METADATA_SCHEMA,
                          true);  // preserve original string

    // now create the decoder and use it to extract and decode the event's metadata
    DbusEventAvroDecoder eventDecoder = createDecoder(metadataSchemaSet);
    try
    {
      GenericRecord reuse = null;
      GenericRecord decodedMetadata = eventDecoder.getMetadata(event, reuse);
      Assert.assertNotNull(decodedMetadata, "getMetadata() returned null GenericRecord;");

      Utf8 etag = (Utf8)decodedMetadata.get("etag");
      Assert.assertEquals(etag.toString(), "dunno what an etag is");

      Integer flags = (Integer)decodedMetadata.get("flags");
      Assert.assertEquals(flags, null, "expected flags to be null");

      Long expires = (Long)decodedMetadata.get("expires");
      Assert.assertNotNull(expires, "expected expires to have a value;");
      Assert.assertEquals(expires.longValue(), 1366150681);

      Utf8 nonexistentField = (Utf8)decodedMetadata.get("nonexistentField");
      Assert.assertNull(nonexistentField, "unexpected value for 'nonexistentField';");
    }
    catch (Exception ex)
    {
      Assert.fail("unexpected error decoding metadata: " + ex);
    }

    LOG.info("leaving testGetMetadata_HappyPath()");
  }

  /**
   * Verifies that getMetadata() throws an exception if the metadata schema specified
   * in the event header is unavailable.
   */
  @Test
  public void testGetMetadata_UnhappyPath_MissingSchema()
  throws Exception
  {
    LOG.info("starting testGetMetadata_UnhappyPath_MissingSchema()");

    // build the event's metadata and then the event
    DbusEventPart metadataPart = createMetadataPart();
    DbusEvent event = createEvent(metadataPart);

    // create an empty metadata schema set
    VersionedSchemaSet metadataSchemaSet = new VersionedSchemaSet();

    // now create the decoder and attempt to use it to extract and decode the event's metadata
    DbusEventAvroDecoder eventDecoder = createDecoder(metadataSchemaSet);
    try
    {
      GenericRecord reuse = null;
      GenericRecord decodedMetadata = eventDecoder.getMetadata(event, reuse);
      Assert.fail("getMetadata() should have thrown exception");
    }
    catch (Exception ex)
    {
      // expected case:  event had metadata, but schema to decode it was missing
    }

    LOG.info("leaving testGetMetadata_UnhappyPath_MissingSchema()");
  }

  /**
   * Verifies that getMetadata() returns null if there's a mismatch between the event's metadata
   * and the metadata schema whose signature/checksum is specified in the event header.
   */
  @Test
  public void testGetMetadata_UnhappyPath_BadSchema()
  throws Exception
  {
    LOG.info("starting testGetMetadata_UnhappyPath_BadSchema()");

    // build the event's metadata and then the event
    DbusEventPart metadataPart = createMetadataPart();
    DbusEvent event = createEvent(metadataPart);

    // create a metadata schema set with a schema that claims to match the event's
    // metadata but doesn't actually
    VersionedSchemaSet metadataSchemaSet = new VersionedSchemaSet();
    metadataSchemaSet.add(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE,
                          metadataPart.getSchemaVersion(),              // METADATA_SCHEMA_VERSION
                          new SchemaId(metadataPart.getSchemaDigest()), // METADATA_SCHEMA_CHECKSUM
                          INCORRECT_METADATA_SCHEMA,
                          true);  // preserve original string

    // now create the decoder and attempt to use it to extract and decode the event's metadata
    DbusEventAvroDecoder eventDecoder = createDecoder(metadataSchemaSet);
    try
    {
      GenericRecord reuse = null;
      GenericRecord decodedMetadata = eventDecoder.getMetadata(event, reuse);
      Assert.assertNull(decodedMetadata, "getMetadata() should have returned null;");
    }
    catch (Exception ex)
    {
      Assert.fail("getMetadata() should not have thrown exception: " + ex);
    }

    LOG.info("leaving testGetMetadata_UnhappyPath_BadSchema()");
  }

  /**
   * Verifies that getMetadata() returns null if event has no metadata.
   */
  @Test
  public void testGetMetadata_UnhappyPath_EventHasNoMetadata()
  throws Exception
  {
    LOG.info("starting testGetMetadata_UnhappyPath_EventHasNoMetadata()");

    // build the event without any metadata
    DbusEvent event = createEvent(null);

    // create a metadata schema set, just because we like to
    VersionedSchemaSet metadataSchemaSet = new VersionedSchemaSet();
    metadataSchemaSet.add(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE,
                          METADATA_SCHEMA_VERSION,
                          new SchemaId(METADATA_SCHEMA_CHECKSUM),
                          CORRECT_METADATA_SCHEMA,
                          true);  // preserve original string

    // now create the decoder and attempt to use it to extract and decode the event's metadata
    DbusEventAvroDecoder eventDecoder = createDecoder(metadataSchemaSet);
    try
    {
      GenericRecord reuse = null;
      GenericRecord decodedMetadata = eventDecoder.getMetadata(event, reuse);
      Assert.assertNull(decodedMetadata, "getMetadata() should have returned null;");
    }
    catch (Exception ex)
    {
      Assert.fail("getMetadata() should not have thrown exception: " + ex);
    }

    LOG.info("leaving testGetMetadata_UnhappyPath_EventHasNoMetadata()");
  }

}
