package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class StringFieldGenerate extends SchemaFiller {

  public static int minStringLength = 0;
  public static int maxStringLength = 1024;

  public StringFieldGenerate(Field field)
  {
    super(field);
    //TODO read min and max length from config and set and use with generator
  }

  @Override
  public void writeToRecord(GenericRecord genericRecord)
  {
    genericRecord.put(field.name(), generateString());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateString();
  }

  public String generateString()
  {
    return randGenerator.getNextString(minStringLength, maxStringLength);
  }

}
