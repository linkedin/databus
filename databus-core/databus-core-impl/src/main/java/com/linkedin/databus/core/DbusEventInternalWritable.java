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

import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;


/**
 * Read-write interface for Databus events, used by
 * internal databus classes.
 **/
public interface DbusEventInternalWritable extends DbusEventInternalReadable
{
  public void setSequence(long sequence);

  public void applyCrc();
  public int writeTo(WritableByteChannel writeChannel, Encoding encoding);
  public void setSize(int sz);
  public void setHeaderCrc(long crc);
  public void setValue(byte[] bytes);
  public void setValueCrc(long crc);
  /**
   * Creates a copy of the current event.
   *
   * <p> <b>Note: This method should be used with extreme care as the event serialization pointed
   * by the object can be overwritten. It should be used only in buffers with BLOCK_ON_WRITE policy.
   * Further, the object should not be used after {@link DbusEventBuffer.DbusEventIterator#remove()}
   * </b></p>
   * @param  e       an existing object to reuse; if null, a new object will be created
   * @return the event copy
   */
  public DbusEvent clone(DbusEvent e);
  public void setSrcId(short srcId);
  public void setSchemaId(byte[] schemaId);
  // Putting this as writable since exposing the byte  buffer makes it writable.
  // TODO Expose readable bytebuffer and move it to Readable. Also, getting raw bytes without serializing does not make
  // sense, so my be there should be a method hasRawBytes()?
  public ByteBuffer getRawBytes();
}
