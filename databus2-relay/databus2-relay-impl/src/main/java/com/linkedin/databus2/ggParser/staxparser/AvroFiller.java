package com.linkedin.databus2.ggParser.staxparser;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class AvroFiller
{
  AvroSchemaHandler _avroHelper;
  GenericRecord _record;

  public AvroFiller(Schema schema)
      throws Exception
  {
    _record = new GenericData.Record(schema);
    _avroHelper = new AvroSchemaHandler(_record.getSchema());
  }


}