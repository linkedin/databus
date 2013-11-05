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
import java.util.Arrays;


public class DbusEventInfo
{
  private DbusOpcode _opCode;
  private long _sequenceId;
  private short _pPartitionId;
  private short _lPartitionId;
  private long _timeStampInNanos;
  private short _srcId;
  private byte[] _payloadSchemaMd5;
  private ByteBuffer _payloadBuffer;
  private boolean _enableTracing;
  // auto-commit is not a setting within the event. Instead, this boolean is used as an indication to
  // DbusEventVx implementations during serialization that the event payload CRC needs to be computed and inserted at
  // the right place. The header CRC is still done in another call.
  private boolean _autocommit;
  private byte _eventSerializationVersion;
  private short _payloadSchemaVersion;
  /** Flag to determine if the event is replicated into the source DB **/
  private boolean _isReplicated;

  private DbusEventPart _metadata;

  public DbusEventInfo(DbusOpcode opCode,
                       long sequenceId,
                       short pPartitionId,
                       short lPartitionId,
                       long timeStampInNanos,
                       short srcId,
                       byte[] payloadSchemaMd5,
                       byte[] payload,
                       boolean enableTracing,
                       boolean autocommit,
                       byte eventSerializationVersion,
                       short payloadSchemaVersion,
                       DbusEventPart metadata)
  {
    this(opCode, sequenceId, pPartitionId, lPartitionId, timeStampInNanos, srcId, payloadSchemaMd5, payload, enableTracing, autocommit);
    _eventSerializationVersion = eventSerializationVersion;
    _payloadSchemaVersion = payloadSchemaVersion;
    _metadata = metadata;
  }

  public DbusEventInfo(DbusOpcode opCode,
                       long sequenceId,
                       short pPartitionId,
                       short lPartitionId,
                       long timeStampInNanos,
                       short srcId,
                       byte[] payloadSchemaMd5,
                       byte[] payload,
                       boolean enableTracing,
                       boolean autocommit)
  {
    super();
    _opCode = opCode;
    _sequenceId = sequenceId;
    _pPartitionId = pPartitionId;
    _lPartitionId = lPartitionId;
    _timeStampInNanos = timeStampInNanos;
    _srcId = srcId;
    _payloadSchemaMd5 = payloadSchemaMd5;
    _enableTracing = enableTracing;
    _autocommit = autocommit;
    _eventSerializationVersion = DbusEventFactory.DBUS_EVENT_V1;
    _payloadSchemaVersion = 0;  // An invalid value
    _payloadBuffer = null;
    if(payload != null)
      _payloadBuffer = ByteBuffer.wrap(payload);
    _metadata = null;
  }


  /** if opCode value is null - it means use default */
  public DbusOpcode getOpCode()
  {
    return _opCode;
  }
  public void setOpCode(DbusOpcode opCode)
  {
    _opCode = opCode;
  }
  public long getSequenceId()
  {
    return _sequenceId;
  }
  public void setSequenceId(long sequenceId)
  {
    _sequenceId = sequenceId;
  }
  public short getpPartitionId()
  {
    return _pPartitionId;
  }
  public void setpPartitionId(short pPartitionId)
  {
    _pPartitionId = pPartitionId;
  }
  public short getlPartitionId()
  {
    return _lPartitionId;
  }
  public void setlPartitionId(short lPartitionId)
  {
    _lPartitionId = lPartitionId;
  }
  public long getTimeStampInNanos()
  {
    return _timeStampInNanos;
  }
  public void setTimeStampInNanos(long timeStampInNanos)
  {
    _timeStampInNanos = timeStampInNanos;
  }
  public short getSrcId()
  {
    return _srcId;
  }
  public void setSrcId(short srcId)
  {
    _srcId = srcId;
  }
  public byte[] getSchemaId()
  {
    return _payloadSchemaMd5;
  }
  public void setSchemaId(byte[] schemaId)
  {
    _payloadSchemaMd5 = schemaId;
  }

  /** makes a copy in case of read-only ByteBuffer */
  public byte[] getValueBytes()
  {
    if(_payloadBuffer == null)
      return null;

    if(_payloadBuffer.isReadOnly() || ! _payloadBuffer.hasArray()) { //allocate new array
      byte[] val = new byte[_payloadBuffer.remaining()];
      _payloadBuffer.get(val);
      return val;
    }
    // else
    return _payloadBuffer.array();

  }
  public int getValueLength() {
    if(_payloadBuffer == null)
      return 0;

    return _payloadBuffer.remaining();
  }

  public boolean isEnableTracing()
  {
    return _enableTracing;
  }
  public void setEnableTracing(boolean enableTracing)
  {
    _enableTracing = enableTracing;
  }
  public boolean isAutocommit()
  {
    return _autocommit;
  }
  public void setAutocommit(boolean autocommit)
  {
    _autocommit = autocommit;
  }
  public byte getEventSerializationVersion()
  {
    return _eventSerializationVersion;
  }
  public void setEventSerializationVersion(byte eventSerializationVersion)
  {
    _eventSerializationVersion = eventSerializationVersion;
  }
  public short getPayloadSchemaVersion()
  {
    return _payloadSchemaVersion;
  }
  public void setPayloadSchemaVersion(short payloadSchemaVersion)
  {
    _payloadSchemaVersion = payloadSchemaVersion;
  }
  public void setValueByteBuffer(ByteBuffer bb)
  {
    if(bb != null) {
      _payloadBuffer = bb.asReadOnlyBuffer();
    } else {
      _payloadBuffer = null;
    }
  }
  public ByteBuffer getValueByteBuffer() {
    return _payloadBuffer;
  }

  public boolean isReplicated()
  {
    return _isReplicated;
  }

  public void setReplicated(boolean replicated)
  {
    _isReplicated = replicated;
  }

  public DbusEventPart getMetadata()
  {
    return _metadata;
  }

  @Override
  public String toString()
  {
    return "DbusEventInfo{" +
        "_opCode=" + _opCode +
        ", _sequenceId=" + _sequenceId +
        ", _pPartitionId=" + _pPartitionId +
        ", _lPartitionId=" + _lPartitionId +
        ", _timeStampInNanos=" + _timeStampInNanos +
        ", _srcId=" + _srcId +
        ", _payloadSchemaMd5=" + Arrays.toString(_payloadSchemaMd5) +
        ", _payloadBuffer=" + _payloadBuffer +
        ", _enableTracing=" + _enableTracing +
        ", _autocommit=" + _autocommit +
        ", _eventSerializationVersion=" + _eventSerializationVersion +
        ", _payloadSchemaVersion=" + _payloadSchemaVersion +
        ", _isReplicated=" + _isReplicated +
        ", _metadata=" + _metadata +
        '}';
  }
}
