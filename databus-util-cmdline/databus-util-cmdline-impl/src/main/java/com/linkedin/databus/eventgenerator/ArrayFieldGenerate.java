package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class ArrayFieldGenerate extends SchemaFiller {

  
  public static int maxArrayLength = 10;

  public ArrayFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException
  {	
     record.put(field.name(), generateArray());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateArray();
  }
  
  public GenericData.Array<Object> generateArray() throws UnknownTypeException
  {
    Schema innerElementSchema = field.schema().getElementType();
    GenericData.Array<Object> array = new GenericData.Array<Object>(10, field.schema());
    

    for(int i = 0; i < randGenerator.getNextInt()%maxArrayLength; i++)
    {
      /* --> uglier way to fetch the random object
      Field fakeField = new Field(field.name()+"fake", innerElementSchema, null, null);
      List<Schema.Field> tlist = new ArrayList<Schema.Field>();
      tlist.add(fakeField);
      Schema fakeSchema = Schema.createRecord(tlist);
      GenericRecord tempRecord  = new GenericData.Record(fakeSchema);   
      SchemaFiller schemaFill = SchemaFiller.createRandomField(fakeField);
      */
      Field fakeField = new Field(field.name()+"fake", innerElementSchema, null, null);
      SchemaFiller schemaFill = SchemaFiller.createRandomField(fakeField);  //Safe from infinite recursion (array within an array, assuming nullable)
      array.add(schemaFill.generateRandomObject());
    }
    return array;
  }
}
