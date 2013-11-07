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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventPart;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 *
 */
public class EventInfoDtailPrinter extends DtailPrinter
{
  private static final SimpleDateFormat EVENT_TS_FORMAT = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");

  static
  {
    EVENT_TS_FORMAT.setCalendar(Calendar.getInstance(TimeZone.getTimeZone("GMT")));
  }

  public EventInfoDtailPrinter(DatabusHttpClientImpl client,
                               StaticConfig conf,
                               OutputStream out)
  {
    super(client, conf, out);
  }

  private static String versionedSchemaId(VersionedSchema v)
  {
    return null == v ? "null" : v.getId().toString();
  }

  /**
   * @see com.linkedin.databus2.tools.dtail.DtailPrinter#printEvent(com.linkedin.databus.core.DbusEventInternalReadable, com.linkedin.databus.client.pub.DbusEventDecoder)
   */
  @Override
  public ConsumerCallbackResult printEvent(DbusEventInternalReadable e,
                                           DbusEventDecoder eventDecoder)
  {
    DbusEventAvroDecoder avroDecoder = (DbusEventAvroDecoder)eventDecoder;
    byte[] payloadSchemaDigest = e.schemaId();
    DbusEventPart metadataPart = e.getPayloadMetadataPart();
    String s = String.format("format=%s opcode=%s partition=%d scn=%d ts=%d (%s.%d) srcid=%d extRepl=%s schema=%s " +
    		                 "payload_schema_digest=%s metadata_schema=%s metadata_schema_digest=%s",
                             e.getClass().getSimpleName(),
                             e.getOpcode(),
                             e.getPartitionId(),
                             e.sequence(),
                             e.timestampInNanos(),
                             EVENT_TS_FORMAT.format(new Date(e.timestampInNanos() / 1000000)),
                             e.timestampInNanos() % 1000000000,
                             e.getSourceId(),
                             e.isExtReplicatedEvent(),
                             versionedSchemaId(eventDecoder.getPayloadSchema(e)),
                             null != payloadSchemaDigest ? Hex.encodeHexString(payloadSchemaDigest) : "null",
                             null != metadataPart ? versionedSchemaId(avroDecoder.getMetadataSchema(e)) : "null",
                             null != metadataPart ? Hex.encodeHexString(metadataPart.getSchemaDigest()) : "null");
    try
    {
      _out.write(s.getBytes("UTF-8"));
      _out.write('\n');
    }
    catch (UnsupportedEncodingException e1)
    {
      return ConsumerCallbackResult.ERROR;
    }
    catch (IOException e1)
    {
      return ConsumerCallbackResult.ERROR;
    }
    return ConsumerCallbackResult.SUCCESS;
  }

}
