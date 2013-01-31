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



public class DbusEventInfo
{
  private DbusOpcode _opCode;
  private long _sequenceId;
  private short _pPartitionId;
  private short _lPartitionId;
  private long _timeStampInNanos;
  private short _srcId;
  private byte[] _schemaId;
  private byte[] _value;
  private boolean _enableTracing;
  private boolean _autocommit;



  public DbusEventInfo(DbusOpcode opCode,
                       long sequenceId,
                       short pPartitionId,
                       short lPartitionId,
                       long timeStampInNanos,
                       short srcId,
                       byte[] schemaId,
                       byte[] value,
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
    _schemaId = schemaId;
    _value = value;
    _enableTracing = enableTracing;
    _autocommit = autocommit;
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
    return _schemaId;
  }
  public void setSchemaId(byte[] schemaId)
  {
    _schemaId = schemaId;
  }
  public byte[] getValue()
  {
    return _value;
  }
  public void setValue(byte[] value)
  {
    _value = value;
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

}
