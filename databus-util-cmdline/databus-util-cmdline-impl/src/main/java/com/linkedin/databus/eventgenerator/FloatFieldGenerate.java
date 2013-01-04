package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class FloatFieldGenerate extends SchemaFiller {

  public FloatFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record)
  {
    record.put(field.name(), generateFloat());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
      return generateFloat();
  }
  
  public Float generateFloat()
  {
    return randGenerator.getNextFloat();
  }
}
