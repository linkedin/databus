package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class LongFieldGenerate extends SchemaFiller {

  public LongFieldGenerate(Field field) {
    super(field);
  }
  
  @Override
  public void writeToRecord(GenericRecord record)
  {
    record.put(field.name(), generateLong());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateLong();
  }
  
  public Long generateLong()
  {
    return randGenerator.getNextLong();
  }
}
