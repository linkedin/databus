package com.linkedin.databus.core;

import java.nio.ByteBuffer;

public class BootstrapEvent extends DbusEvent
{ 
  public BootstrapEvent(ByteBuffer buf)
  {
    // TODO the current DbusEvent construction doesn't work because it 
    // clears the buffer. We need a constructor that takes a serialized
    // DbusEvent directly.
    super(buf, buf.position());
  }

  public void setId(short srcId)
  {
    setSrcId(srcId);
  }

}
