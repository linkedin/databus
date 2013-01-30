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


import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

public class MapFieldGenerate extends SchemaFiller
{

  public static int maxNumberOfMapFields = 10;

  public static int getMaxNumberOfMapFields()
  {
    return maxNumberOfMapFields;
  }

  public static void setMaxNumberOfMapFields(int maxNumberOfMapFields)
  {
    MapFieldGenerate.maxNumberOfMapFields = maxNumberOfMapFields;
  }

  public MapFieldGenerate(Field field)
  {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record) throws UnknownTypeException
  {
    record.put(field.name(), generateMap());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateMap();
  }

  public Map<Utf8,Object> generateMap() throws UnknownTypeException
  {
    Map<Utf8,Object> map = new HashMap<Utf8,Object>(randGenerator.getNextInt()%maxNumberOfMapFields);
    Field fakeField = new Field(field.name(), field.schema().getValueType(), null, null);

    int count = randGenerator.getNextInt()%getMaxNumberOfMapFields();
    for(int i =0 ; i < count ; i++ )
    {
      SchemaFiller filler = SchemaFiller.createRandomField(fakeField); // create a new filler each time to emulate null-able fields
      map.put(new Utf8(field.name()+i), filler.generateRandomObject());
    }
    return map;
  }

}
