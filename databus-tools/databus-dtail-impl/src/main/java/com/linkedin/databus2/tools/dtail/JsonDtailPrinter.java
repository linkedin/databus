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

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;


/**
 * A printer to JSON which does not use the standard Avro-to-Json serialization because unions look
 * "ugly". It generates JSON that cannot be converted back to the payload schema as the union
 * types have been lost.
 */
public class JsonDtailPrinter extends GenericRecordDtailPrinter
{
  public static final Logger LOG = Logger.getLogger(JsonDtailPrinter.class);

  public JsonDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out)
  {
    super(client, conf, out, MetadataOutput.NONE);
  }

  public JsonDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out,
                          MetadataOutput metadata)
  {
    super(client, conf, out, metadata);
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
      _out.write(r.toString().getBytes());
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
