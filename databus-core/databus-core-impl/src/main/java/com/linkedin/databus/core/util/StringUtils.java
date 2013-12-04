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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;

/**
 * Miscellaneous string utility functions
 */
public class StringUtils
{

  private static final Pattern ORA_JDBC_URI_PATTERN =
      Pattern.compile("(jdbc:oracle:thin:)([^/@]*)(/[^@]*)(@.*)");
  private static final Pattern MYSQL_JDBC_PATTERN1 = Pattern.compile("\\((user=|password=)([^)]*)\\)");
  private static final Pattern MYSQL_JDBC_PATTERN2 = Pattern.compile("(user=|password=)([^&]*)");

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


  /**
   * Strip username/password information from the JDBC DB uri to be used for logging
   * @param uri     the JDBC URI to sanitize
   * @return the sanitized DB URI
   */
  public static String sanitizeDbUri(String uri)
  {
    String result = uri;
    Matcher m = ORA_JDBC_URI_PATTERN.matcher(uri);
    if (m.matches())
    {
      result = m.group(1) + "*/*" + m.group(4);
    }
    else if (uri.startsWith("jdbc:mysql:"))
    {
      Matcher m1 = MYSQL_JDBC_PATTERN1.matcher(result);
      Matcher m2 = MYSQL_JDBC_PATTERN2.matcher(result);
      if (m1.find())
      {
        result = m1.replaceAll("($1*)");
      }
      else if (m2.find())
      {
        result = m2.replaceAll("$1*");
      }
    }

    return result;
  }

}
