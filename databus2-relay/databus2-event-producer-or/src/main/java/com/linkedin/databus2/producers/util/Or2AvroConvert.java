package com.linkedin.databus2.producers.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.Int24Column;
import com.google.code.or.common.glossary.column.LongColumn;
import com.google.code.or.common.glossary.column.LongLongColumn;
import com.google.code.or.common.glossary.column.NullColumn;
import com.google.code.or.common.glossary.column.ShortColumn;
import com.google.code.or.common.glossary.column.TinyColumn;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

public class Or2AvroConvert
{
  private static final int maxCacheSize = 10000;
  protected static final Map<Field, Or2AvroBasicConvert> convertCacheMap = new HashMap<Field, Or2AvroBasicConvert>(maxCacheSize);

  public static final int TINYINT_MAX_VALUE = 256;
  public static final int SMALLINT_MAX_VALUE = 65536;
  public static final int MEDIUMINT_MAX_VALUE = 16777216;
  public static final long INTEGER_MAX_VALUE = 4294967296L;
  public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");

  public static Object convert(Column s, Field avroField) throws DatabusException
  {
    if (s instanceof NullColumn)
    {
      return null;
    }
    Or2AvroBasicConvert converter = fetchConverter(avroField);
    return converter.convert(s, avroField);
  }

  private static Or2AvroBasicConvert fetchConverter(Field avroField) throws DatabusException
  {
    Or2AvroBasicConvert converter = convertCacheMap.get(avroField);
    if (converter != null)
    {
      return converter;
    }
    synchronized (convertCacheMap)
    {
      converter = convertCacheMap.get(avroField);
      if (converter != null)
      {
        return converter;
      }
      if (convertCacheMap.size() > maxCacheSize)
      {
        convertCacheMap.clear();
      }
      String schemaStr = avroField.schema().toString();
      if (schemaStr.contains("int"))
      {
        converter = IntegerConverter.fetchInstance();
      }
      else if (schemaStr.contains("long"))
      {
        converter = LongConverter.fetchInstance();
      }
      else if (schemaStr.contains("double"))
      {
        converter = DoubleConverter.fetchInstance();
      }
      else if (schemaStr.contains("string"))
      {
        converter = StringConverter.fetchInstance();
      }
      else if (schemaStr.contains("bytes"))
      {
        converter = BytesConverter.fetchInstance();
      }
      else if (schemaStr.contains("float"))
      {
        converter = FloatConverter.fetchInstance();
      }
      if (converter == null)
      {
        throw new DatabusException("schema is " + schemaStr + ", converter not exist!");
      }
      convertCacheMap.put(avroField, converter);
      return converter;
    }
  }

  public static abstract class Or2AvroBasicConvert
  {
    public abstract Object convert(Column s, Field avroField) throws DatabusException;
  }

  public static class IntegerConverter extends Or2AvroBasicConvert
  {
    private static IntegerConverter converter = null;

    public static IntegerConverter fetchInstance()
    {
      if (converter == null)
      {
        converter = new IntegerConverter();
      }
      return converter;
    }

    @Override
    public Object convert(Column s, Field avroField) throws DatabusException
    {
      Object obj = s.getValue();
      Integer res = null;
      if(obj instanceof Number)
      {
        res = ((Number) obj).intValue();
      }
      else
      {
        throw new DatabusException((obj == null ? "" : (obj.getClass() + " | " + obj)) + "can't be converted into Integer");
      }
      if(res.intValue() >= 0)
      {
    	  return res.intValue();
      }
      return res.intValue() + (int) unsignedOffset(s, avroField);
    }
  }

  public static class LongConverter extends Or2AvroBasicConvert
  {
    private static LongConverter converter = null;

    public static LongConverter fetchInstance()
    {
      if (converter == null)
      {
        converter = new LongConverter();
      }
      return converter;
    }

    @Override
    public Object convert(Column s, Field avroField) throws DatabusException
    {
      Object obj = s.getValue();
      Long res = null;
      if (obj instanceof Date)
      {
        return ((Date) obj).getTime();
      }
      else if (obj instanceof Number)
      {
    	  res = ((Number) obj).longValue();
      }
      else
      {
        throw new DatabusException((obj == null ? "" : (obj.getClass() + " | " + obj)) + " can't be converted into Long");
      }
      if (res.longValue() >= 0)
      {
		return res.longValue();
	  }
	  return res.longValue() + unsignedOffset(s, avroField);
    }
  }

  public static class DoubleConverter extends Or2AvroBasicConvert
  {
    private static DoubleConverter converter = null;

    public static DoubleConverter fetchInstance()
    {
      if (converter == null)
      {
        converter = new DoubleConverter();
      }
      return converter;
    }

    @Override
    public Object convert(Column s, Field avroField) throws DatabusException
    {
      Object obj = s.getValue();
      Double res = null;
      if (obj instanceof Number)
      {
        res = ((Number) obj).doubleValue();
      }
      else
      {
        throw new DatabusException((obj == null ? "" : (obj.getClass() + " | " + obj)) + " can't be converted into Double");
      }
      return res;
    }
  }

  public static class StringConverter extends Or2AvroBasicConvert
  {
    private static StringConverter converter = null;

    public static StringConverter fetchInstance()
    {
      if (converter == null)
      {
        converter = new StringConverter();
      }
      return converter;
    }

    @Override
    public Object convert(Column s, Field avroField)
    {
      if (s.getValue() instanceof byte[])
      {
        return new String((byte[]) s.getValue(), Charset.defaultCharset());
      }
      return s.getValue().toString();//s.getValue will never  be null
    }
  }

  public static class BytesConverter extends Or2AvroBasicConvert
  {
    private static BytesConverter converter = null;

    public static BytesConverter fetchInstance()
    {
      if (converter == null)
      {
        converter = new BytesConverter();
      }
      return converter;
    }

    @Override
    public Object convert(Column s, Field avroField) throws DatabusException
    {
      if (!(s.getValue() instanceof byte[]))
      {
		throw new DatabusException(
				avroField.name() + " is assigned to be bytes array, but it can't be converted to byte array | "
						+ avroField.schema().toString());
      }
      byte[] byteArr = (byte[]) s.getValue();
      return ByteBuffer.wrap(byteArr);
    }
  }

  public static class FloatConverter extends Or2AvroBasicConvert
  {
    private static FloatConverter converter = null;

    public static FloatConverter fetchInstance()
    {
      if (converter == null)
      {
        converter = new FloatConverter();
      }
      return converter;
    }

    @Override
    public Object convert(Column s, Field avroField) throws DatabusException
    {
      Object obj = s.getValue();
      Float res = null;
      if (obj instanceof Number)
      {
        res = ((Number) obj).floatValue();
      }
      else
      {
        throw new DatabusException((obj == null ? "" : (obj.getClass() + " | " + obj)) + "can't be converted to Float");
      }
      return res;
    }
  }

  private final static long unsignedOffset(Column s, Field avroField)
  {
    if (s instanceof Int24Column)
    {
      Int24Column lc = (Int24Column) s;
      int i = lc.getValue().intValue();
      if (i < 0 && SchemaHelper.getMetaField(avroField, "dbFieldType").contains("UNSIGNED"))
      {
        return MEDIUMINT_MAX_VALUE;
      }
      return 0L;
    }
    else if (s instanceof LongColumn)
    {
      LongColumn lc = (LongColumn) s;
      Long i = lc.getValue().longValue();
      if (i < 0 && SchemaHelper.getMetaField(avroField, "dbFieldType").contains("UNSIGNED"))
      {
        return INTEGER_MAX_VALUE;
      }
      return 0L;
    }
    else if (s instanceof LongLongColumn)
    {
      LongLongColumn llc = (LongLongColumn) s;
      Long l = llc.getValue();
      if (l < 0 && SchemaHelper.getMetaField(avroField, "dbFieldType").contains("UNSIGNED"))
      {
        return BIGINT_MAX_VALUE.longValue();
      }
      return 0L;
    }
    else if (s instanceof ShortColumn)
    {
      ShortColumn sc = (ShortColumn) s;
      Integer i = sc.getValue();
      if (i < 0 && SchemaHelper.getMetaField(avroField, "dbFieldType").contains("UNSIGNED"))
      {
        return SMALLINT_MAX_VALUE;
      }
      return 0L;
    }
    else if (s instanceof TinyColumn)
    {
      TinyColumn tc = (TinyColumn) s;
      Integer i = tc.getValue();
      if (i < 0 && SchemaHelper.getMetaField(avroField, "dbFieldType").contains("UNSIGNED"))
      {
        return TINYINT_MAX_VALUE;
      }
      return 0L;
    }
    return 0;
  }
}
