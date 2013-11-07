package com.linkedin.databus2.tools.dtail;
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
import java.io.OutputStream;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonEncoder;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;


/**
 * An event printer using the standard Avro JSON serialization.
 * */
public class AvroJsonDtailPrinter extends GenericRecordDtailPrinter
{
  public static final Logger LOG = Logger.getLogger(AvroJsonDtailPrinter.class);

  private final HashMap<Schema, GenericDatumWriter<GenericRecord>> _jsonWriters;
  private final HashMap<Schema, JsonEncoder> _jsonEncoders;

  public AvroJsonDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out)
  {
    this(client, conf, out, MetadataOutput.NONE);
  }

  public AvroJsonDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out,
                              MetadataOutput metadata)
  {
    super(client, conf, out, metadata);
    _jsonWriters = new HashMap<Schema, GenericDatumWriter<GenericRecord>>(5);
    _jsonEncoders = new HashMap<Schema, JsonEncoder>(5);
  }

  /**
   * @see com.linkedin.databus2.tools.dtail.GenericRecordDtailPrinter#printGenericRecord(org.apache.avro.generic.GenericRecord)
   */
  @Override
  public ConsumerCallbackResult printGenericRecord(GenericRecord r)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    try
    {
      //eventDecoder.dumpEventValueInJSON(e, _out);
      JsonEncoder jsonEnc = _jsonEncoders.get(r.getSchema());
      if (null == jsonEnc)
      {
        jsonEnc = new JsonEncoder(r.getSchema(), _out);
        _jsonEncoders.put(r.getSchema(), jsonEnc);
      }

      GenericDatumWriter<GenericRecord> datumWriter = _jsonWriters.get(r.getSchema());
      if (null == datumWriter)
      {
        datumWriter = new GenericDatumWriter<GenericRecord>(r.getSchema());
        _jsonWriters.put(r.getSchema(), datumWriter);
      }

      datumWriter.write(r, jsonEnc);
      jsonEnc.flush();
      _out.write('\n');
    }
    catch (RuntimeException re)
    {
      LOG.error("event dump error: " + re.getMessage(), re);
      result = ConsumerCallbackResult.ERROR;
    }
    catch (IOException ioe)
    {
      LOG.error("event dump error: " + ioe.getMessage(), ioe);
      result = ConsumerCallbackResult.ERROR;
    }
    return result;
  }

}
