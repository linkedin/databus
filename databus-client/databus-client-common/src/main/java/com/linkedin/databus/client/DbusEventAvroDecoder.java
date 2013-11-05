package com.linkedin.databus.client;
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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventPart;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;

public class DbusEventAvroDecoder implements DbusEventDecoder
{
  public static final String MODULE = DbusEventAvroDecoder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String SRC_ID_FIELD_NAME = "srcId";
  public static final String VALUE_FIELD_NAME = "value";
  public static final String OPCODE_FIELD_NAME = "opCode";

  //BinaryDecoder is threadunsafe. So use threadlocal to wrap it
  private static final ThreadLocal<BinaryDecoder> binDecoder = new ThreadLocal<BinaryDecoder>();

  private final VersionedSchemaSet _schemaSet;
  private final VersionedSchemaSet _metadataSchemaSet;

  public DbusEventAvroDecoder(VersionedSchemaSet schemaSet)
  {
    this(schemaSet,null);
  }

  public DbusEventAvroDecoder(VersionedSchemaSet schemaSet, VersionedSchemaSet metadataSchemaSet)
  {
    super();
    _schemaSet = schemaSet;
    _metadataSchemaSet = metadataSchemaSet;
  }

  @Override
  public GenericRecord getGenericRecord(DbusEvent e, GenericRecord reuse)
  {
    byte[] md5 = new byte[16];
    e.schemaId(md5);
    SchemaId schemaId = new SchemaId(md5);
    VersionedSchema writerSchema = _schemaSet.getById(schemaId);

    if (null == writerSchema)
    {
      LOG.error("Unable to find schema for id " + schemaId + "; event = " + e);
      return null;
    }

    ByteBuffer valueBuffer = e.value();

    byte[] valueBytes = null;
    if (valueBuffer.hasArray())
    {
      valueBytes = valueBuffer.array();
    }
    else
    {
      valueBytes = new byte[valueBuffer.remaining()];
      valueBuffer.get(valueBytes);
    }

    return getGenericRecord(valueBytes, writerSchema.getSchema(), reuse);
  }

  /**
   * Creates a generic Record from the DbusEvent.
   *
   * @param e   DbusEvent to be converted to generic record
   * @return the GenericRecord for the DbusEvent's payload
   */
  public GenericRecord getGenericRecord(DbusEvent e)
  {
    return getGenericRecord(e, null);
  }

  /**
   * Creates a generic record from a byte array.
   *
   * @param valueBytes  byte[] to be converted to generic record
   * @param schema      schema of the input record
   * @return GenericRecord for the given byte array + schema combo
   *
   * TODO:  Add a   getGenericRecord(InputStream data, Schema schema, GenericRecord reuse)
   *        variant; it can use DecoderFactory.createBinaryDecoder(InputStream, BinaryDecorder)
   *        and will allow us to use something like org.apache.avro.ipc.ByteBufferInputStream
   *        to avoid the data copy to a temp array.  (https://rb.corp.linkedin.com/r/172879/)
   */
  public GenericRecord getGenericRecord(byte[] valueBytes, Schema schema, GenericRecord reuse)
  {
    GenericRecord result = null;
    try
    {
      binDecoder.set(DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, binDecoder.get()));
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
      result = reader.read(reuse, binDecoder.get());
      return result;
    }
    catch (Exception ex)  // IOException, ArrayIndexOutOfBoundsException, ...
    {
      LOG.error("getGenericRecord Avro error: " + ex.getMessage(), ex);
    }
    return result;
  }

  /**
   * Deserializes the metadata (if any) of a Databus event to an Avro GenericRecord.  This method
   * is for INTERNAL USE ONLY (by Espresso and Databus).  It is NOT a stable API and may change
   * without warning!
   *
   * @param e       the Databus event whose metadata is to be decoded
   * @param reuse   an existing {@link org.apache.avro.generic.GenericRecord} object where the
   *                deserialized values will be written to. The object can be <b>null</b>, in
   *                which case a new object will be allocated.
   * @return {@link org.apache.avro.generic.GenericRecord} object with the deserialized data, or
   *         null if no metadata exists.  Returned in <b>reuse</b> if provided, else in a newly
   *         allocated object.
   * @throws DatabusRuntimeException if event contains metadata but schema to decode it is missing
   */
  public GenericRecord getMetadata(DbusEvent e, GenericRecord reuse)
  {
    DbusEventPart metadataPart = e.getPayloadMetadataPart();
    ByteBuffer dataBuffer = null;
    if (null == metadataPart || null == (dataBuffer = metadataPart.getData()) || dataBuffer.remaining() <= 0)
    {
      LOG.debug("No metadata for event " + e);
      return null;
    }

    VersionedSchema schema = getMetadataSchema(metadataPart);
    if (null == schema)
    {
      throw new DatabusRuntimeException("No schema available to decode metadata for event " + e);
    }

    byte[] dataBytes = null;
    if (dataBuffer.hasArray())
    {
      dataBytes = dataBuffer.array();
    }
    else
    {
      dataBytes = new byte[dataBuffer.remaining()];
      try
      {
        dataBuffer.get(dataBytes);
      }
      catch (BufferUnderflowException ex)
      {
        LOG.error("metadata buffer error (remaining = " + dataBuffer.remaining() + ") for event " + e, ex);
        return null;
      }
    }

    return getGenericRecord(dataBytes, schema.getSchema(), reuse);
  }

  @Override
  public <T extends SpecificRecord> T getTypedValue(DbusEvent e, T reuse, Class<T> targetClass)
  {
    if (null == reuse)
    {
      try
      {
        reuse = targetClass.newInstance();
      }
      catch (InstantiationException e1)
      {
        LOG.error("getTypedValue class instantiation error (" + e1.getMessage() + ") for event " + e, e1);
        return null;
      }
      catch (IllegalAccessException e1)
      {
        LOG.error("getTypedValue access error (" + e1.getMessage() + ") for event " + e, e1);
        return null;
      }
    }
    byte[] md5 = new byte[16];
    e.schemaId(md5);
    SchemaId schemaId = new SchemaId(md5);
    VersionedSchema writerSchema = _schemaSet.getById(schemaId);

    if (null == writerSchema)
    {
      LOG.error("Unable to find schema for id " + schemaId + "; event = " + e);
      return null;
    }

    ByteBuffer valueBuffer = e.value();
    byte[] valueBytes = new byte[valueBuffer.remaining()];
    valueBuffer.get(valueBytes);
    try
    {
      //JsonDecoder jsonDec = new JsonDecoder(sourceSchema.getSchema(),new ByteArrayInputStream(valueBytes));
      binDecoder.set(DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, binDecoder.get()));
      SpecificDatumReader<SpecificRecord> reader =
          new SpecificDatumReader<SpecificRecord>(writerSchema.getSchema(), reuse.getSchema());
      return targetClass.cast(reader.read(reuse, binDecoder.get()));
    }
    catch (IOException e1)
    {
      LOG.error("getTypedValue IO error (" + e1.getMessage() + ") for event " + e, e1);
    }
    return reuse;
  }

  public void dumpEventValueInJSON(DbusEvent e, OutputStream out)
  {
    byte[] md5 = new byte[16];
    e.schemaId(md5);
    SchemaId schemaId = new SchemaId(md5);
    VersionedSchema sourceSchema = _schemaSet.getById(schemaId);
    ByteBuffer valueBuffer = e.value();
    byte[] valueBytes = new byte[valueBuffer.remaining()];
    valueBuffer.get(valueBytes);

    try {
      Schema schema = sourceSchema.getSchema();
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      binDecoder.set(DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, binDecoder.get()));

      Object datum = reader.read(null, binDecoder.get());
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      JsonGenerator g = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
      // write the src ID
      g.writeStartObject();
      g.writeFieldName(SRC_ID_FIELD_NAME);
      g.writeNumber(e.getSourceId());
      g.writeFieldName(OPCODE_FIELD_NAME);
      g.writeString(e.getOpcode().toString());

      g.writeFieldName(VALUE_FIELD_NAME);
      writer.write(datum, new JsonEncoder(schema, g));
      g.writeEndObject();
      g.writeEndObject();
      try {
        g.writeEndObject();
      }
      catch (JsonGenerationException e_json) {
        // ignore the error as some how avro JsonEncoder may some times missing two }
      }
      g.flush();
    } catch (IOException e1) {
      LOG.error("event value serialization error; event = " + e, e1);
    }
  }

  @Override
  public void dumpEventValueInJSON(DbusEvent e, WritableByteChannel writeChannel)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      dumpEventValueInJSON(e, baos);
      baos.write("\n".getBytes("UTF-8"));

      ByteBuffer writeBuffer = ByteBuffer.wrap(baos.toByteArray());
      writeChannel.write(writeBuffer);
    } catch (IOException e1) {
      LOG.error("event value serialization error; event = " + e, e1);
    }
  }

  @Override
  /**
   * @param e DatabusEvent
   * @return Avro Schema, sourceName, version tuple describing the payload data appearing in 'e'.
   */
  public VersionedSchema getPayloadSchema(DbusEvent e)
  {
    byte[] md5 = new byte[16];
    e.schemaId(md5);
    SchemaId schemaId = new SchemaId(md5);
    VersionedSchema writerSchema = _schemaSet.getById(schemaId);
    return writerSchema;
  }

  protected VersionedSchemaSet getSchemaSet()
  {
    return _schemaSet;
  }

  /**
   * Returns the single version of the metadata schema specified in the given event's header.
   * For INTERNAL USE ONLY (by Espresso and Databus).  This is not a stable API and may change
   * without warning!
   *
   * @param e DbusEvent
   * @return {AvroSchema, "metadata-source", version} tuple for given event 'e' with
   *         metadata-schema-id; null if event contains no metadata
   * @throws DatabusRuntimeException if event contains metadata but schema to decode it is missing
   */
  public VersionedSchema getMetadataSchema(DbusEvent e)
  {
    DbusEventPart metadataPart = e.getPayloadMetadataPart();
    if (null == metadataPart)
    {
      LOG.debug("No metadata for event " + e);
      return null;
    }

    VersionedSchema schema = getMetadataSchema(metadataPart);
    if (null == schema)
    {
      throw new DatabusRuntimeException("No schema available to decode metadata for event " + e);
    }

    return schema;
  }

  /**
   * Returns the single version of the metadata schema specified in the metadata portion of an
   * event's header.  For INTERNAL USE ONLY (by Espresso and Databus).  This is not a stable
   * API and may change without warning!
   *
   * @param metadataPart  metadata portion of a DbusEvent
   * @return {AvroSchema, "metadata-source", version} tuple for metadataPart, or null if
   *         is not available
   */
  public VersionedSchema getMetadataSchema(DbusEventPart metadataPart)
  {
    if (null == _metadataSchemaSet)
    {
      return null;
    }
    SchemaId id = new SchemaId(metadataPart.getSchemaDigest());
    return _metadataSchemaSet.getById(id);
  }

  /**
   * Returns the specified version of the metadata schema.  For INTERNAL USE ONLY (by Espresso
   * and Databus).  This is not a stable API and may change without warning!
   *
   * @param version  version number of the desired metadata schema
   * @return {AvroSchema, "metadata-source", version} tuple for given metadata schema version;
   #         null if none exists
   */
  public VersionedSchema getMetadataSchema(short version)
  {
    if (_metadataSchemaSet != null)
    {
      return _metadataSchemaSet.getSchemaByNameVersion(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE, version);
    }
    return null;
  }

  /**
   * Returns the latest version of the metadata schema.  For INTERNAL USE ONLY (by Espresso
   * and Databus).  This is not a stable API and may change without warning!
   *
   * @return {AvroSchema, "metadata-source", version} tuple of highest-numbered version of
   #         metadata schema; null if none exists
   */
  VersionedSchema getLatestMetadataSchema()
  {
    if (_metadataSchemaSet != null)
    {
      return _metadataSchemaSet.getLatestVersionByName(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE);
    }
    return null;
  }

  public void dumpMetadata(DbusEvent e, FileChannel writeChannel)
  {
    GenericRecord genericRecord = this.getMetadata(e, null);
    if( genericRecord == null ) //no metadata
      return;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try
    {
      String metadataInfo = genericRecord.toString() + "\n";
      baos.write(metadataInfo.getBytes("UTF-8"));
      ByteBuffer writeBuffer = ByteBuffer.wrap(baos.toByteArray());
      writeChannel.write(writeBuffer);
    }
    catch (UnsupportedEncodingException e1)
    {
      LOG.error("event metadata serialization error; event = " + e + "; metadata = " + genericRecord, e1);
    }
    catch (IOException e1)
    {
      LOG.error("event metadata serialization error; event = " + e + "; metadata = " + genericRecord, e1);
    }
  }

}
