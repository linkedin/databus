package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class NullFieldGenerate extends SchemaFiller {

  public NullFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record)
  {
    record.put(field.name(), null);
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return null;
  }

}
