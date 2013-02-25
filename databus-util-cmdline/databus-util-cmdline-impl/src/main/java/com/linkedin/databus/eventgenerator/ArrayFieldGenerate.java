package com.linkedin.databus.eventgenerator;
/*
 *
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
 *
*/


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
