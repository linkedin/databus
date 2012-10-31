package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class BytesFieldGenerate extends SchemaFiller{

  public static int defaultMaxBytesLength = 50;

  public static int getMaxBytesLength()
  {
    return defaultMaxBytesLength;
  }

  public static void setMaxBytesLength(int bytesLength)
  {
    BytesFieldGenerate.defaultMaxBytesLength = bytesLength;
  }

  public BytesFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord genericRecord)
  {
    genericRecord.put(field.name(), generateBytes());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateBytes();
  }

  public byte[] generateBytes()
  {
    return randGenerator.getNextBytes(getMaxBytesLength());
  }
}
