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


import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;


public class DataGenerator {

  public final static String MODULE = DataGenerator.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  Schema schema;

  /*
   * Takes a schema file as input
   */
  public DataGenerator(File schemaFile) throws IOException {
    schema = Schema.parse(schemaFile);
  }

  /*
   * Takes a schema string as an input
   */
  public DataGenerator(String schema) {
    this.schema = Schema.parse(schema);
  }


  /*
   * Generate random based on the avro schema
   * The schema must be of a record type to work
   * 
   * @return returns the randomly generated record
   */
  public GenericRecord generateRandomRecord() throws UnknownTypeException {

    if(schema.getType() != Schema.Type.RECORD)
    {
      LOG.error("The schema first level must be record.");
      return null;
    }

    GenericRecord record = new GenericData.Record(schema);
    for(Field field : schema.getFields() )
    {
      SchemaFiller schemaFill = SchemaFiller.createRandomField(field);
      schemaFill.writeToRecord(record);
    }	
    return record;
  }


  static public void prettyPrint(GenericRecord record)
  {
    try {
      LOG.info(new JSONObject(record.toString()).toString(2));
    } catch (JSONException e) {
      LOG.error("Unable to parser json: The Json created by the generator is not valid!");
      e.printStackTrace();
    }
  }

  public static Options loadOptions() {
    Options opt = new Options();
    opt.addOption("s", "schemaLocation", true, "location of the schema");
    opt.addOption("minStringLength", true,	"Minimum length of string to be generated");
    opt.addOption("maxStringLength", true,	"Maximum length of string to be generated");
    opt.addOption("minIntegerRange", true,	"Start range of integer");
    opt.addOption("maxIntegerRange", true,	"End range of integer");
    opt.addOption("minLongRange", true,    "Start range of long");
    opt.addOption("maxLongRange", true,  "End range of long");
    opt.addOption("maxBytesLength", true,  "Maximum length of the bytes to be generated");

    return opt;
  }

  public static void printHelp(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("help", opts);
  }

  public static void main(String[] args) throws IOException, UnknownTypeException {

    // load and verify the options
    CommandLineParser parser = new PosixParser();
    Options opts = loadOptions();

    CommandLine cmd = null;
    try {
      cmd = parser.parse(opts, args);
    } catch (org.apache.commons.cli.ParseException parseEx) {
      LOG.error("Invalid option");
      printHelp(opts);
      return;
    }

    // check for necessary options
    String fileLoc = cmd.getOptionValue("schemaLocation");
    if (fileLoc == null) {
      LOG.error("schemaLocation not specified");
      printHelp(opts);
    }

    //get string length and check if min is greater than 0

    // Generate the record
    File schemaFile = new File(fileLoc);
    DataGenerator dataGenerator = new DataGenerator(schemaFile);
    GenericRecord record = dataGenerator.generateRandomRecord();
    DataGenerator.prettyPrint(record);
    
  }

  // TODO add thread based generator here

}
