package com.linkedin.databus2.core.container.request;

import org.jboss.netty.buffer.ChannelBuffer;

/** Interface for parsers of binary commands */
public interface BinaryCommandParser
{
  public enum ParseResult
  {
    DISCARD,
    INCOMPLETE_DATA,
    EXPECT_MORE,
    PASS_THROUGH,
    DONE
  }

  /** Start parsing a new binary command */
  void startNew();

  /** Parse the next batch of bytes */
  ParseResult parseBinary(ChannelBuffer buf) throws Exception;

  /** Return the parsed command if the last {@link #parseBinary(ChannelBuffer)} returned DONE and
   * {@link #getError()} is null. Otherwise, it is undefined. */
  IDatabusRequest getCommand();

  /** Return the parsing error (if any) if the last {@link #parseBinary(ChannelBuffer)} returned
   * DONE. Otherwise, it is undefined. */
  ErrorResponse getError();

}
