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
package com.linkedin.databus.core.util;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Hex;

/**
 * Miscellaneous string utility functions
 */
public class StringUtils
{

  /**
   * Dumps as a hex string the contents of a buffer around a position
   * @param buf     the ByteBuffer to dump
   * @param bufOfs  starting offset in the buffer
   * @param length  the number of bytes to print
   * @return the hexstring
   */
  public static String hexdumpByteBufferContents(ByteBuffer buf, int bufOfs, int length)
  {
    if (length < 0)
    {
      return "";
    }

    final int endOfs = Math.min(buf.limit(), bufOfs + length + 1);
    final byte[] bytes = new byte[endOfs - bufOfs];

    buf = buf.duplicate();
    buf.position(bufOfs);
    buf.get(bytes);
    return new String(Hex.encodeHex(bytes));
  }

}
