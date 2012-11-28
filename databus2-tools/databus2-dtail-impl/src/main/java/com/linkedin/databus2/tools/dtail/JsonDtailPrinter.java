package com.linkedin.databus2.tools.dtail;

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
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;


public class JsonDtailPrinter extends DtailPrinter
{
  public static final Logger LOG = Logger.getLogger(JsonDtailPrinter.class);

  private final HashMap<Schema, GenericDatumWriter<GenericRecord>> _jsonWriters;
  private final HashMap<Schema, JsonEncoder> _jsonEncoders;

  public JsonDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out)
  {
    super(client, conf, out);
    _jsonWriters = new HashMap<Schema, GenericDatumWriter<GenericRecord>>(5);
    _jsonEncoders = new HashMap<Schema, JsonEncoder>(5);
  }

  @Override
  public ConsumerCallbackResult printEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    try
    {
      //eventDecoder.dumpEventValueInJSON(e, _out);
      GenericRecord r = eventDecoder.getGenericRecord(e, null);

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
