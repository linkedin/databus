package com.linkedin.databus.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;

public class DbusEventAvroDecoder implements DbusEventDecoder
{
  public static final String MODULE = DbusEventAvroDecoder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String SRC_ID_FIELD_NAME = "srcId";
  public static final String VALUE_FIELD_NAME = "value";
  //BinaryDecoder is threadunsafe. So use threadlocal to wrap it
  private static final ThreadLocal<BinaryDecoder> binDecoder = new ThreadLocal<BinaryDecoder>();

  private final VersionedSchemaSet _schemaSet;
  
  public DbusEventAvroDecoder(VersionedSchemaSet schemaSet)
  {
    super();
    _schemaSet = schemaSet;
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
      LOG.error("Unable to find schema for id: " + schemaId.toString());
      return null;
    }

    ByteBuffer valueBuffer = e.value();

    byte[] valueBytes = null;
    if ( valueBuffer.hasArray())
    {
      valueBytes = valueBuffer.array();
    } else {
      valueBytes = new byte[valueBuffer.limit()];
      valueBuffer.get(valueBytes);
    }

    return getGenericRecord(valueBytes, writerSchema.getSchema(), reuse);
  }

  /*
   * Creates a generic Record from the DbusEvent
   *
   * @param e DbusEvent to be converted to generic record
   * @return the GenericRecord for the DbusEvent
   */
  public GenericRecord getGenericRecord(DbusEvent e)
  {
    return getGenericRecord(e, null);
  }

  /*
   * Creates a generic Record from byte[]
   *
   * @param valueBytes byte[] to be converted to generic record
   * @param schema   schema of the input record
   * @return the GenericRecord for the DbusEvent
   */
  public GenericRecord getGenericRecord(byte[] valueBytes, Schema schema, GenericRecord reuse)
  {
	GenericRecord result = null;

    try
    {
        binDecoder.set(
          DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, binDecoder.get()));
        GenericDatumReader<GenericRecord> reader = new
                                    GenericDatumReader<GenericRecord>(schema);
        result = reader.read(reuse, binDecoder.get());
        return result;
    } catch (IOException io ) {
      LOG.error("getGenericRecord avro error: " + io.getMessage(), io);
    }
    return result;
  }

  @Override
  public <T extends SpecificRecord> T getTypedValue(DbusEvent e, T reuse,
                                                    Class<T> targetClass)
  {
    if (null == reuse)
    {
      try
      {
        reuse = targetClass.newInstance();
      }
      catch (InstantiationException e1)
      {
        LOG.error("getTypedValue class instantiation error: " + e1.getMessage(), e1);
        return null;
      }
      catch (IllegalAccessException e1)
      {
        LOG.error("getTypedValue access error: " + e1.getMessage(), e1);
        return null;
      }
    }
    byte[] md5 = new byte[16];
    e.schemaId(md5);
    SchemaId schemaId = new SchemaId(md5);
    VersionedSchema writerSchema = _schemaSet.getById(schemaId);

    if (null == writerSchema)
    {
      LOG.error("Unable to find schema for id: " + schemaId.toString());
      return null;
    }

    ByteBuffer valueBuffer = e.value();
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);
    try
    {
      //JsonDecoder jsonDec = new JsonDecoder(sourceSchema.getSchema(),new ByteArrayInputStream(valueBytes));
    	binDecoder.set(
    			DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, binDecoder.get()));
      SpecificDatumReader<SpecificRecord> reader = new SpecificDatumReader<SpecificRecord>(writerSchema.getSchema(), reuse.getSchema());
      return targetClass.cast(reader.read(reuse, binDecoder.get()));
    }
    catch (IOException e1)
    {
      LOG.error("getTypedValue IO error: " + e1.getMessage(), e1);
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
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);

    try {
      Schema schema = sourceSchema.getSchema();
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      binDecoder.set(
              DecoderFactory.defaultFactory().createBinaryDecoder(valueBytes, binDecoder.get()));

      Object datum = reader.read(null, binDecoder.get());
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      JsonGenerator g =
        new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
      // write the src ID
      g.writeStartObject();
      g.writeFieldName(SRC_ID_FIELD_NAME);
      g.writeNumber(e.srcId());
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
      LOG.error("event value serialization error", e1);
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
      LOG.error("event value serialization error", e1);
    }
  }

  @Override
  public VersionedSchema getPayloadSchema(DbusEvent e)
  {
	     byte[] md5 = new byte[16];
	     e.schemaId(md5);
	     SchemaId schemaId = new SchemaId(md5);
	     VersionedSchema writerSchema = _schemaSet.getById(schemaId);
	     return writerSchema;
  }

}
