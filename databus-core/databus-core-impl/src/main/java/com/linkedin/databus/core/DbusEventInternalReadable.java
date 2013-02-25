package com.linkedin.databus.core;
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




import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Read-only interface for Databus events, used by
 * internal databus classes.
 **/
public interface DbusEventInternalReadable extends DbusEvent
{
  public boolean isErrorEvent();
  /** Resets this object to point to a bytebuffer that holds an event */
  public void reset(ByteBuffer buf, int position);

  public void unsetInited();
  public long headerCrc();
  public EventScanStatus scanEvent();
  public EventScanStatus scanEvent(boolean logErrors);
  // TODO What is the difference between payliadLength() and valueLength()?
  public int payloadLength();
  public long valueCrc();
  public DbusEventInternalWritable createCopy();
  public int writeTo(WritableByteChannel writeChannel, Encoding encoding);

  /**
   * @return the versionof the payload schema
   */
  public short schemaVersion();
  public boolean isValid(boolean logErrors);
  public HeaderScanStatus scanHeader();
  public boolean isPartial (boolean logErrors);
  public boolean isPartial();
}
