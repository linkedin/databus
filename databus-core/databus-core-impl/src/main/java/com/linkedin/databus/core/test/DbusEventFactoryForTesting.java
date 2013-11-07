package com.linkedin.databus.core.test;
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

import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.UnsupportedDbusEventVersionRuntimeException;


public class DbusEventFactoryForTesting
{
  /**
   * Creates a read-only DbusEvent out of an already initialized (serialized) buffer,
   * except with the specified SCN.  The event's header CRC is updated appropriately.
   *
   * @param buf        buffer containing the serialized event
   * @param position   byte-offset of the serialized event in the buffer
   * @param seq        sequence number (SCN) of the new event
   * @param version    desired version of the new event
   * @return  a read-only DbusEvent
   */
  public static DbusEventInternalReadable createReadOnlyDbusEventFromBuffer(ByteBuffer buf,
                                                                            int position,
                                                                            long seq,
                                                                            byte version)
  {
    DbusEventFactory eventFactory;
    if (version == DbusEventFactory.DBUS_EVENT_V1)
    {
      eventFactory = new DbusEventV1Factory();
    }
    else if (version == DbusEventFactory.DBUS_EVENT_V2)
    {
      eventFactory = new DbusEventV2Factory();
    }
    else
    {
      throw new UnsupportedDbusEventVersionRuntimeException(version);
    }
    DbusEventInternalWritable event = eventFactory.createWritableDbusEventFromBuffer(buf, position);
    event.setSequence(seq);
    event.applyCrc();
    return event;
  }
}
