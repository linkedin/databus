package com.linkedin.databus.core.util;
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



import com.linkedin.databus.core.DbusEventInternalWritable;


/**
 *
 * @author snagaraj
 *Methods to inject corrupt bit pattern at various fields of an event
 */
public class DbusEventCorrupter
{
  /**
   * Toggles corrupt state of event ; event state defined by event and sub-field of event
   *
   * @param type :  part of event to inject corrupt
   * @param ev :  DbusEvent that will be modified
   */
  static public void toggleEventCorruption(EventCorruptionType type, DbusEventInternalWritable ev) {
      switch (type) {
          case LENGTH:
              int newSize = ev.size() ^ _corruptionPattern;
              ev.setSize(newSize);
              break;
          case HEADERCRC:
              long headerCrc = ev.headerCrc() ^ _corruptionPattern;
              ev.setHeaderCrc(headerCrc);
              break;
          case PAYLOAD:
              if (ev.payloadLength() > 0) {
                  byte[] payload = new byte[ev.payloadLength()];
                  ev.value().get(payload);
                  payload[0] ^= _corruptionPattern;
                  ev.setValue(payload);
              }
              break;
          case PAYLOADCRC:
              long payloadCrc = ev.valueCrc() ^ _corruptionPattern;
              ev.setValueCrc(payloadCrc);
              break;
      }
  }

  public enum EventCorruptionType {
      LENGTH,
      HEADERCRC,
      PAYLOAD,
      PAYLOADCRC,
      NONE,
  }

  static private final byte _corruptionPattern = 85; //01010101

}
