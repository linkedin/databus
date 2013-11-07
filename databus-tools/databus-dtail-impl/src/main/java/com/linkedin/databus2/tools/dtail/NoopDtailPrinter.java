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

import java.io.OutputStream;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;


public class NoopDtailPrinter extends GenericRecordDtailPrinter
{

  public NoopDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out)
  {
    super(client, conf, out, MetadataOutput.NONE);
  }

  public NoopDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out,
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
    return ConsumerCallbackResult.SUCCESS;
  }

}
