package com.linkedin.databus.core;


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
