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
import org.apache.avro.generic.GenericRecord;

public class IntegerFieldGenerate extends SchemaFiller {


  public static int minIntLength = -10;
  public static int maxIntLength = Integer.MAX_VALUE;

  public static int getMinIntLength()
  {
    return minIntLength;
  }

  public static void setMinIntLength(int minIntLength)
  {
    IntegerFieldGenerate.minIntLength = minIntLength;
  }

  public static int getMaxIntLength()
  {
    return maxIntLength;
  }

  public static void setMaxIntLength(int maxIntLength)
  {
    IntegerFieldGenerate.maxIntLength = maxIntLength;
  }

  public IntegerFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord genericRecord)
  {
    genericRecord.put(this.field.name(), generateInteger());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateInteger();
  }

  public Integer generateInteger()
  {
    return randGenerator.getNextInt(minIntLength, maxIntLength);
  }

}
