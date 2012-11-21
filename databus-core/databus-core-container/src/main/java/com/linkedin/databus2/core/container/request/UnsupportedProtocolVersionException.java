package com.linkedin.databus2.core.container.request;

import com.linkedin.databus2.core.DatabusException;

public class UnsupportedProtocolVersionException extends DatabusException
{
  private static final long serialVersionUID = 1L;

  private final byte _protocolVerson;

  public UnsupportedProtocolVersionException(byte protocolVersion)
  {
    super("unsupported protocol version: " + Byte.toString(protocolVersion));
    _protocolVerson = protocolVersion;
  }

  public byte getProtocolVerson()
  {
    return _protocolVerson;
  }


}
