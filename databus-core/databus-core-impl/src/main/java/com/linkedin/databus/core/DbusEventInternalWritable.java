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

import java.nio.charset.Charset;


/**
 * Read-write interface for Databus events, used by
 * internal Databus classes.
 **/
public abstract class DbusEventInternalWritable extends DbusEventInternalReadable
{
  /**
   * Denotes the end of the range of srcid values reserved for private use:
   * (Short.MIN_VALUE, PRIVATE_RANGE_MAX_SRCID]
   */
  public static final short EOPMarkerSrcId = -2;
  static final short CHECKPOINT_SRCID = -3;
  // -4 - -50 are reserved for errors
  public static final short PRIVATE_RANGE_MAX_ERROR_SRCID = -4;
  public static final short BOOTSTRAPTOOOLD_ERROR_SRCID = -5;
  public static final short PULLER_RETRIES_EXPIRED = -6;
  public static final short PRIVATE_RANGE_MIN_ERROR_SRCID = -50;
  public static final short PRIVATE_RANGE_MAX_SRCID = -20000;
  public static final short SCN_REGRESS = -51;

  private static final byte[] emptyValue = "".getBytes(Charset.forName("UTF-8"));
  public static final byte[] emptyMd5 = new byte[16];
  public static final byte[] EOPMarkerValue = emptyValue;
  public static final long ZeroLongKey = 0L;
  public static final DbusEventKey ZeroLongKeyObject = new DbusEventKey(0L);
  public static final DbusEventKey EOPMarkerKey = ZeroLongKeyObject;

  // TODO Move these declarations into a TBD DbusEventBase class when DbusEventInternal
  // becomes an interface.

  /**
   * Set the sequence number (SCN) of the event.
   * @param sequence
   */
  public abstract void setSequence(long sequence);

  /**
   * Compute the header CRC and insert it in the appropriate offset in the event.
   */
  public abstract void applyCrc();
  //public abstract int writeTo(WritableByteChannel writeChannel, Encoding encoding);

  // The methods setSize, setHeaderCrc, setValue and setValueCrc are used only
  // by DbusEventCorrupter, and should never be used by others.
  public abstract void setSize(int sz);
  public abstract void setHeaderCrc(long crc);
  public abstract void setValue(byte[] bytes);
  public abstract void setValueCrc(long crc);
  /**
   * Creates a copy of the current event.
   *
   * <p> <b>Note: This method should be used with extreme care as the event serialization pointed
   * by the object can be overwritten. It should be used only in buffers with BLOCK_ON_WRITE policy.
   * Further, the object should not be used after {@link DbusEventBuffer.DbusEventIterator#remove()}
   * </b></p>
   * @param  e   an existing object to reuse; if null, a new object will be created
   * @return the event copy
   */
  public abstract DbusEvent clone(DbusEvent e);
  public abstract void setSrcId(int srcId);

  /**
   * Sets the schema ID (MD5 schema digest) for the payload . It is up to the caller to make sure that there is
   * room in the event to set the schema ID.
   *
   * @Note: This method is only called by EventRewriter in SendEventsRequest.onEvent()
   * @param payloadSchemaId
   */
  public abstract void setSchemaId(byte[] payloadSchemaId);

  // In espresso, we change the schema ID, srcId, and schema version during re-write.
  // In DbusEventV1, re-computing the header CRC is good enough. But in DbusEventV2,
  // we need to re-compute both header and body CRC.
  // For now, we introduce this method.
  // A better way to go is for the event to keep track of what changed, and re-compute
  // appropriate CRCs as needed.
  // TODO Change events to keep track of what changed and re-compute CRCs as needed.
  public abstract void recomputeCrcsAfterEspressoRewrite();

  public static DbusEventInternalWritable convertToDifferentVersion(DbusEventInternalWritable event, byte toVer) {
    byte fromVer = event.getVersion();
    // currently the only conversion we support is from V2 to V1
    if(fromVer == DbusEventFactory.DBUS_EVENT_V2 && toVer == DbusEventFactory.DBUS_EVENT_V1)
      try {
        return ((DbusEventV2)event).convertToV1();
      } catch (KeyTypeNotImplementedException e) {
        throw new DatabusRuntimeException("failed to convert V2 event to V1, because of unsupported key type:" +
              e.getLocalizedMessage());
      }

    // if we do not support the conversion we throw an exception
    throw new DatabusRuntimeException("Unsupported Event Version. cannot convert from " + fromVer + " to "+toVer);
  }
}
