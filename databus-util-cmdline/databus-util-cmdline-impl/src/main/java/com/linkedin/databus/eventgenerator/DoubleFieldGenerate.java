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

public class DoubleFieldGenerate extends SchemaFiller {

  private static double minValue = 0;
  private static double maxValue = Double.MAX_VALUE;
  
  public static double getMinValue()
  {
    return minValue;
  }

  public static void setMinValue(double minValue)
  {
    DoubleFieldGenerate.minValue = minValue;
  }

  public static double getMaxValue()
  {
    return maxValue;
  }

  public static void setMaxValue(double maxValue)
  {
    DoubleFieldGenerate.maxValue = maxValue;
  }

  public DoubleFieldGenerate(Field field) {
    super(field);
  }

  @Override
  public void writeToRecord(GenericRecord record)
  {
    record.put(field.name(), generateDouble());
  }

  @Override
  public Object generateRandomObject() throws UnknownTypeException
  {
    return generateDouble();
  }
  
  public Double generateDouble()
  {
    return randGenerator.getNextDouble();
  }
}
