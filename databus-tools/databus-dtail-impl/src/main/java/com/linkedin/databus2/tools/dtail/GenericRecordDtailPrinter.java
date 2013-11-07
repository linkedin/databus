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
package com.linkedin.databus2.tools.dtail;

import java.io.OutputStream;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 *
 */
public abstract class GenericRecordDtailPrinter extends DtailPrinter
{
  protected MetadataOutput _metadataOutput;

  public GenericRecordDtailPrinter(DatabusHttpClientImpl client,
                                   StaticConfig conf,
                                   OutputStream out,
                                   MetadataOutput metadataOutput)
  {
    super(client, conf, out);
    _metadataOutput = metadataOutput;
  }

  /**
   * @see com.linkedin.databus2.tools.dtail.DtailPrinter#printEvent(com.linkedin.databus.core.DbusEventInternalReadable, com.linkedin.databus.client.pub.DbusEventDecoder)
   */
  @Override
  public ConsumerCallbackResult printEvent(DbusEventInternalReadable e,
                                           DbusEventDecoder eventDecoder)
  {
    DbusEventAvroDecoder avroDecoder = (DbusEventAvroDecoder)eventDecoder;
    switch (_metadataOutput)
    {
    case NONE:
      GenericRecord payload = eventDecoder.getGenericRecord(e, null);
      return payload != null ? printGenericRecord(payload) : ConsumerCallbackResult.SUCCESS;
    case ONLY:
      GenericRecord metadata = avroDecoder.getMetadata(e, null);
      return null != metadata ? printGenericRecord(metadata) : ConsumerCallbackResult.SUCCESS;
    case INCLUDE:
      GenericRecord payload1 = avroDecoder.getGenericRecord(e, null);
      GenericRecord metadata1 = avroDecoder.getMetadata(e, null);

      Schema pschema = Schema.createUnion(Arrays.asList(avroDecoder.getPayloadSchema(e).getSchema(),
                                                        Schema.create(Type.NULL)));
      Field pfield = new Field("payload", pschema, "payload", null);
      VersionedSchema metaschema = avroDecoder.getMetadataSchema(e);
      Schema mschema = null != metaschema ?
          Schema.createUnion(Arrays.asList(metaschema.getSchema(), Schema.create(Type.NULL))) :
          Schema.createUnion(Arrays.asList(Schema.create(Type.INT), Schema.create(Type.NULL)));
      Field mfield = new Field("metadata", mschema, "metadata", null);
      Schema combined = Schema.createRecord(Arrays.asList(pfield, mfield));
      GenericRecord r = new GenericData.Record(combined);
      r.put(0, payload1);
      r.put(1, metadata1);

      return printGenericRecord(r);
    default:
      LOG.error("unknown metadata output mode: " + _metadataOutput);
      return ConsumerCallbackResult.ERROR_FATAL;
    }
  }

  abstract public ConsumerCallbackResult printGenericRecord(GenericRecord r);
}
