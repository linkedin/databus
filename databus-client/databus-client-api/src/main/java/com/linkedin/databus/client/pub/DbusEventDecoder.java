package com.linkedin.databus.client.pub;

import java.nio.channels.WritableByteChannel;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 * An interface for the decoders from the payload of a Databus data event to an Avro
 * {@link org.apache.avro.specific.SpecificRecord} or {@link org.apache.avro.generic.GenericRecord}.
 */
public interface DbusEventDecoder
{

  /**
   * Deserializes the payload of a Databus data event to an Avro
   * {@link org.apache.avro.specific.SpecificRecord}. The implementation will do
   * necessary schema conversions if the serialization and deserialization schemas
   * differ.
   *
   * @param <V>     the Avro-generated {@link org.apache.avro.specific.SpecificRecord}
   *                implementation of the data event payload schema.
   * @param e       the Databus event envelope object
   * @param reuse   an existing {@link org.apache.avro.specific.SpecificRecord} object where the
   *                deserialized values will be written to. The object can be <b>null</b> in which
   *                case a new object will be allocated.
   * @param targetClass a reference to the {@link org.apache.avro.specific.SpecificRecord}
   *                implementation class.
   * @return the {@link org.apache.avro.specific.SpecificRecord} implementation object with the
   *         deserialized data; this will be either <b>reuse</b> or the newly allocated object.
   */
  <V extends SpecificRecord> V getTypedValue(DbusEvent e, V reuse, Class<V> targetClass);

  /**
   * Deserializes the payload of a Databus data event to an Avro
   * {@link org.apache.avro.generic.GenericRecord}. The implementation will do
   * necessary schema conversions if the serialization and deserialization schemas
   * differ.
   *
   * @param e       the Databus event envelope object
   * @param reuse   an existing {@link org.apache.avro.specific.SpecificRecord} object where the
   *                deserialized values will be written to. The object can be <b>null</b> in which
   *                case a new object will be allocated.
   * @return the {@link org.apache.avro.generic.GenericRecord} implementation object with the
   *         deserialized data; this will be either <b>reuse</b> or the newly allocated object.
   */
  public GenericRecord getGenericRecord(DbusEvent e, GenericRecord reuse);

  /** Converts the event payload to a JSON format */
  void dumpEventValueInJSON(DbusEvent e, WritableByteChannel writeChannel);
  
  public VersionedSchema getPayloadSchema(DbusEvent e);
}
