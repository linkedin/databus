package com.linkedin.databus.core.util;
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


import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper methods for processing configs.
 */
public class ConfigHelper
{
  /** Patern for parsing duration strings */
  public static final Pattern DURATION_PATTERN =
      Pattern.compile("\\s*(\\d+)\\s*(ns|nanos|nanosecond|nanoseconds|" +
      		          "us|micros|microsecond|microseconds|" +
      		          "ms|millis|millisecond|milliseconds|" +
      		          "s|sec|second|seconds|" +
      		          "min|minute|minutes|" +
      		          "h|hr|hour|hours|" +
      		          "d|day|days)?\\s*",
      		          Pattern.CASE_INSENSITIVE);
  /** Pattern for parsing size strings */
  public static final Pattern BYTE_SIZE_PATTERN =
      Pattern.compile("\\s*(\\d+)\\s*(k|K|m|M|g|G|t|T|p|P|e|E)?\\s*");

  /**
   * Parses a duration string of the format: duration_value [duration_unit]. duration_value must be
   * a non-negative integer. Available duration_units are:
   * <ul>
   *   <li>ns|nanos|nanosecond|nanoseconds - nanoseconds
   *   <li>us|micros|microsecond|microseconds - microseconds
   *   <li>ms|millis|millisecond|milliseconds - milliseconds
   *   <li>s|sec|second|seconds - seconds
   *   <li>min|minute|minutes - minutes
   *   <li>h|hr|hour|hours - hours
   *   <li>d|day|days - days
   * </ul>
   * @param  durationStr        the string to be parsed
   * @param  defaultUnit        the unit to use if none is specified
   * @return the duration in defaultUnit units
   * @throws InvalidConfigException if the duration string does not follow the above pattern
   * */
  public static long parseDuration(String durationStr, TimeUnit defaultUnit) throws InvalidConfigException
  {
    if (null == durationStr) return 0;
    TimeUnit unit = defaultUnit;
    Matcher m = DURATION_PATTERN.matcher(durationStr);
    if (!m.matches()) throw new InvalidConfigException("invalid duration string: " + durationStr);
    if (1 < m.groupCount() && null != m.group(2) && 0 < m.group(2).length())
    {
      char unitChar1 = m.group(2).charAt(0);
      switch (unitChar1)
      {
      case 'n':
      case 'N': unit = TimeUnit.NANOSECONDS; break;
      case 'u':
      case 'U': unit = TimeUnit.MICROSECONDS; break;
      case 'm':
      case 'M':
        char unitChar3 = m.group(2).length() >= 3 ? m.group(2).charAt(2) : ' ';
        unit = ('n' == unitChar3 || 'N' == unitChar3) ? TimeUnit.MINUTES : TimeUnit.MILLISECONDS;
        break;
      case 's':
      case 'S': unit = TimeUnit.SECONDS; break;
      case 'h':
      case 'H': unit = TimeUnit.HOURS; break;
      case 'd':
      case 'D': unit = TimeUnit.DAYS; break;
      }
    }

    long value = Long.parseLong(m.group(1));
    return defaultUnit.convert(value, unit);
  }

  /** Parses a byte-size string of the format: size_value[size_unit]. size_value must be non-
   * negative. size_unit can be any of the following:
   * <ul>
   *    <il>k - 10^3 bytes
   *    <il>K - 2^10 bytes
   *    <il>m - 10^6 bytes
   *    <il>M - 2^20 bytes
   *    <il>g - 10^9 bytes
   *    <il>G - 2^30 bytes
   *    <il>t - 10^12 bytes
   *    <il>T - 2^40 bytes
   *    <il>p - 10^15 bytes
   *    <il>P - 2^50 bytes
   *    <il>e - 10^18 bytes
   *    <il>E - 2^60 bytes
   * </ul>*/
  public static long parseByteSize(String sizeString) throws InvalidConfigException
  {
    if (null == sizeString) return 0;
    long unit = 1L;
    Matcher m = BYTE_SIZE_PATTERN.matcher(sizeString);
    if (!m.matches()) throw new InvalidConfigException("invalid byte-size string: " + sizeString);
    if (1 < m.groupCount() && null != m.group(2) && 0 < m.group(2).length())
    {
      char unitChar1 = m.group(2).charAt(0);
      switch (unitChar1)
      {
      case 'k': unit = 1000L; break;
      case 'K': unit = 1024L; break;
      case 'm': unit = 1000L * 1000L; break;
      case 'M': unit = 1024L * 1024L; break;
      case 'g': unit = 1000L * 1000L * 1000L; break;
      case 'G': unit = 1024L * 1024L * 1024L; break;
      case 't': unit = 1000L * 1000L * 1000L * 1000L; break;
      case 'T': unit = 1024L * 1024L * 1024L * 1024L; break;
      case 'p': unit = 1000L * 1000L * 1000L * 1000L * 1000L; break;
      case 'P': unit = 1024L * 1024L * 1024L * 1024L * 1024L; break;
      case 'e': unit = 1000L * 1000L * 1000L * 1000L * 1000L * 1000L; break;
      case 'E': unit = 1024L * 1024L * 1024L * 1024L * 1024L * 1024L; break;
      }
    }

    long value = Long.parseLong(m.group(1));
    return unit * value;

  }
}
