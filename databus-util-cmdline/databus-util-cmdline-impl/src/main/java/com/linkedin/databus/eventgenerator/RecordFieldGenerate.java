package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class RecordFieldGenerate extends SchemaFiller {

  public RecordFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException
  {
   
    record.put(field.name(), generateRecord());
  }
  
  
  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateRecord();
  }
  
  
  public GenericRecord generateRecord() throws UnknownTypeException
  {
    GenericRecord subRecord = new GenericData.Record(field.schema());
    for(Field field : this.field.schema().getFields())
    {
      SchemaFiller fill;
      fill = SchemaFiller.createRandomField(field);
      fill.writeToRecord(subRecord);
    }

    return subRecord;
  }

}