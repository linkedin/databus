package com.linkedin.databus2.producers.gg;
/*
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
 */


import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.ConstantPartitionFunction;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.schemas.utils.SchemaHelper;


public class GGEventGenerationFactory
{
  //2013-03-10:11:45:01.001000000
  private final Logger log = Logger.getLogger(getClass());

  /**
   * Given a logical source config, create a partition function.
   *
   * @param sourceConfig
   * @return the partition function
   * @throws InvalidConfigException
   */
  public static PartitionFunction buildPartitionFunction(LogicalSourceStaticConfig sourceConfig)
      throws InvalidConfigException
  {
    String partitionFunction = sourceConfig.getPartitionFunction();
    if (partitionFunction.startsWith("constant:"))
    {
      try
      {
        String numberPart = partitionFunction.substring("constant:".length()).trim();
        short constantPartitionNumber = Short.valueOf(numberPart);
        return new ConstantPartitionFunction(constantPartitionNumber);
      }
      catch(Exception ex)
      {
        // Could be a NumberFormatException, IndexOutOfBoundsException or other exception when trying
        // to parse the partition number.
        throw new InvalidConfigException("Invalid partition configuration (" + partitionFunction + "). " +
                                         "Could not parse the constant partition number.");
      }
    }
    else
    {
      throw new InvalidConfigException("Invalid partition configuration (" + partitionFunction + ").");
    }
  }

  public static String uriToGGDir(String uri)
      throws DatabusException
  {
    if (uri == null)
    {
      throw new DatabusException("uri passed is null and not valid");
    }

    Pattern pattern = Pattern.compile("gg://(.*):(.*)");
    Matcher matcher = pattern.matcher(uri);
    if (!matcher.matches() || matcher.groupCount() != 2)
    {
      throw new DatabusException("Expected uri format for gg path not found");
    }

    return matcher.group(1);
  }

  public static String uriToXmlPrefix(String uri)
      throws DatabusException
  {
    if (uri == null)
    {
      throw new DatabusException("uri passed is null and not valid");
    }

    Pattern pattern = Pattern.compile("gg://(.*):(.*)");
    Matcher matcher = pattern.matcher(uri);
    if (!matcher.matches() || matcher.groupCount() != 2)
    {
      throw new DatabusException("Expected uri format for gg path not found");
    }

    return matcher.group(2);
  }

  public static Object stringToAvroType(String fieldValue, Schema.Field avroField)
      throws DatabusException
  {
    Schema.Type fieldType = SchemaHelper.getAnyType(avroField);
    String recordFieldName = avroField.name();

    switch (fieldType)
    {
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
      case NULL:
        return convertToSimpleType(fieldValue, avroField);
      case RECORD:
      case ARRAY:
        // TODO Add support for these datatypes (warning: when do so, watch out for fieldType
        // vs. avroField.schema() mismatches for arrays within unions:  see DDSDBUS-3093/3136)
        throw new DatabusException("Handling of Avro '" + fieldType + "' field type not yet implemented!");
      case ENUM:
      case FIXED:
      case MAP:
      case UNION:
      default:
        throw new DatabusException("unknown field type: " + recordFieldName + ":" + fieldType);
    }
  }

  public static Object convertToSimpleType(String fieldValue, Schema.Field avroField)
      throws DatabusException
  {
    String databaseFieldType = SchemaHelper.getMetaField(avroField, "dbFieldType");
    String recordFieldName = avroField.name();

    //return int
    if (databaseFieldType.equalsIgnoreCase("INTEGER"))
    {
      return new Integer(fieldValue);
    } //return long
    else if (databaseFieldType.equalsIgnoreCase("LONG"))
    {
      return new Long(fieldValue);
    }
    else if (databaseFieldType.equalsIgnoreCase("DATE"))
    {
       return ggDateStringToLong(fieldValue);
    }
    else if (databaseFieldType.equalsIgnoreCase("TIMESTAMP"))
    {
      return ggTimeStampStringToMilliSeconds(fieldValue);
    }
    //return float
    else if (databaseFieldType.equalsIgnoreCase("FLOAT"))
    {
      return new Float(fieldValue);
    }
    //return double
    else if (databaseFieldType.equalsIgnoreCase("DOUBLE"))
    {
      return new Double(fieldValue);
    }
    //return string
    else if (databaseFieldType.equalsIgnoreCase("CLOB"))
    {
      return fieldValue;
    }
    else if (databaseFieldType.equalsIgnoreCase("VARCHAR"))
    {
      return fieldValue;
    }
    else if (databaseFieldType.equalsIgnoreCase("VARCHAR2"))
    {
      return fieldValue;
    }
    else if (databaseFieldType.equalsIgnoreCase("NVARCHAR"))
    {
      return fieldValue;
    }
    else if (databaseFieldType.equalsIgnoreCase("NVARCHAR2"))
    {
      return fieldValue;
    }
    else if (databaseFieldType.equalsIgnoreCase("XMLTYPE"))
    {
      return fieldValue;
    }
    else if (databaseFieldType.equalsIgnoreCase("CHAR"))
    {
      return fieldValue;
    }
    //return bytes
    else if (databaseFieldType.equalsIgnoreCase("BLOB") || databaseFieldType.equalsIgnoreCase("RAW"))
    {
      if (fieldValue.length() == 0)
      {
        return fieldValue.getBytes(Charset.defaultCharset());
      }
      if (fieldValue.length() <= 2)
      {
        throw new DatabusException("Unable to decode the string because length is less than 2");
      }
      if (!isStringHex(fieldValue))
      {
        throw new DatabusException("Unable to decode the string because it is not hex-encoded");
      }
      try
      {
        return stringToHex(fieldValue.substring(2, fieldValue.length()-1));
      }
      catch (DecoderException e)
      {
        throw new DatabusException("Unable to decode a " + databaseFieldType + " field: " + recordFieldName);
      }
    }
    //return array
    else if (databaseFieldType.equalsIgnoreCase("ARRAY"))
    {
      throw new DatabusException("ARRAY type still not implemented!");           //TODO add support for array
    }
    //return record
    else if (databaseFieldType.equalsIgnoreCase("TABLE"))
    {
      throw new DatabusException("TABLE type still not implemented!");           //TODO add support for table
    }
    else
    {
      throw new DatabusException("unknown field type: " + recordFieldName + ":" + databaseFieldType);
    }
  }

  public static boolean isStringHex(String fieldValue)
  {
    if (fieldValue == null || fieldValue.length() <= 2)
    {
      return false;
    }
    return fieldValue.substring(0, 2).equals("0x");
  }

  public static byte[] stringToHex(String hexString)
      throws DecoderException
  {
    return Hex.decodeHex(hexString.toCharArray());
  }

  public static long ggTimeStampStringToNanoSeconds(String value)
      throws DatabusException
  {
    return (ggTimeStampStringToMilliSeconds(value) * DbusConstants.NUM_NSECS_IN_MSEC);
  }

  public static long ggTimeStampStringToMilliSeconds(String value)
      throws DatabusException
  {
    Pattern _pattern = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2}):(\\d{2}):(\\d{2}):(\\d{2})\\.(\\d{0,9})");
    Matcher matcher = _pattern.matcher(value);
    if (!matcher.matches() || matcher.groupCount() != 7)
    {
      throw new DatabusException("The timestamp format is not as expected, cannot proceed!");
    }

    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    //Explicitly set ms to zero; without initialization it has random ms values :(
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Integer.valueOf(matcher.group(1)),
                 Integer.valueOf(matcher.group(2)) - 1,
                 Integer.valueOf(matcher.group(3)),
                 Integer.valueOf(matcher.group(4)),
                 Integer.valueOf(matcher.group(5)),
                 Integer.valueOf(matcher.group(6)));

    //Prune to the first 3 digits or less
    String milliSecondsString = matcher.group(7);
    int maxSecondsLength = (milliSecondsString.length() > 3) ? 3 : milliSecondsString.length();
    String prunedMilliSeconds = milliSecondsString.substring(0, maxSecondsLength);

    //Add the ms value to the calendar object
    calendar.add(Calendar.MILLISECOND, Integer.valueOf(prunedMilliSeconds));
    return calendar.getTimeInMillis();
  }

  public static long ggDateStringToLong(String value)
      throws DatabusException
  {
    Pattern _pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}).*");
    Matcher matcher = _pattern.matcher(value);
    if (!matcher.matches() || matcher.groupCount() != 1)
    {
      throw new DatabusException("The date format is not as expected, cannot proceed!");
    }
    String dateFormatString = matcher.group(1);
    long dateLong = Date.valueOf(dateFormatString).getTime();
    return dateLong;
  }

}
