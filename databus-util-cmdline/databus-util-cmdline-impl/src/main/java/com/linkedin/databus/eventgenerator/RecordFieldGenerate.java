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
