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


import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class UnionFieldGenerate extends SchemaFiller {

  public UnionFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException
  {
    getUnionFieldFiller().writeToRecord(record);
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return getUnionFieldFiller().generateRandomObject();
  }
  
  public SchemaFiller getUnionFieldFiller() throws UnknownTypeException
  {
    List<Schema> schemas = field.schema().getTypes();
    Schema schema = null;
    for (Schema s: schemas)
    {
    	schema = s;  
    	if (schema.getType()!=Schema.Type.NULL) break;
    }
    Field tempField = new Field(field.name(), schema, null, null);
    return SchemaFiller.createRandomField(tempField);
  }

}
