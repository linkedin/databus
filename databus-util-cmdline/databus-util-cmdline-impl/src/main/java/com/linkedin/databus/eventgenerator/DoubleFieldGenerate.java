package com.linkedin.databus.eventgenerator;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class DoubleFieldGenerate extends SchemaFiller {

  public static double minValue = 0;
  public static double maxValue = Double.MAX_VALUE;
  
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
