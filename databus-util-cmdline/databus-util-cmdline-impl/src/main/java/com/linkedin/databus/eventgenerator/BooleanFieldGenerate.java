package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class BooleanFieldGenerate extends SchemaFiller{

  public BooleanFieldGenerate(Field field)
  {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException
  {
      record.put(field.name(), generateBoolean());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateBoolean();
  }

  public boolean generateBoolean()
  {
    return randGenerator.getNextBoolean();
  }
}
