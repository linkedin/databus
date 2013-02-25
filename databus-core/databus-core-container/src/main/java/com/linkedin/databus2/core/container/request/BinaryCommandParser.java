package com.linkedin.databus2.core.container.request;
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
