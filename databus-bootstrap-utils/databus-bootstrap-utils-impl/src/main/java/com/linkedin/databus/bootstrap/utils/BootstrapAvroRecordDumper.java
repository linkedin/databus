package com.linkedin.databus.bootstrap.utils;
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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;

public class BootstrapAvroRecordDumper 
{
	public static final String MODULE = BootstrapAvroRecordDumper.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static final String OUTPUT_DIR_OPT_LONG_NAME = "output_dir";
	public static final char   OUTPUT_DIR_OPT_CHAR = 'o';

	public static String outputDir = null;
	
	public static void main(String[] args)
			throws Exception
	{
		BootstrapTableReader.init(args);
		BootstrapTableReader reader = new BootstrapTableReader(new DumpEventHandler(outputDir, BootstrapTableReader.getSchema()));
		reader.execute();
	}

	@SuppressWarnings("static-access")
	public static void parseArgs(String[] args) throws IOException
	{
		CommandLineParser cliParser = new GnuParser();


		Option outputDirOption = OptionBuilder.withLongOpt(OUTPUT_DIR_OPT_LONG_NAME)
				.withDescription("Help screen")
				.create(OUTPUT_DIR_OPT_CHAR);

		Options options = new Options();
		options.addOption(outputDirOption);


		CommandLine cmd = null;
		try
		{
			cmd = cliParser.parse(options, args);
		}
		catch (ParseException pe)
		{
			LOG.fatal("Bootstrap Avro Record Dumper: failed to parse command-line options.", pe);
			throw new RuntimeException("Bootstrap Avro Record Dumper: failed to parse command-line options.", pe);
		}
		
		if (cmd.hasOption(OUTPUT_DIR_OPT_CHAR))
		{
			outputDir = cmd.getOptionValue(OUTPUT_DIR_OPT_CHAR);
		}
	}

	public static class DumpEventHandler
	implements BootstrapReaderEventHandler
	{
		private String _query = null;

		private DataFileWriter<GenericRecord> writer = null;
		private File directory = null;
		private File currentFile = null;

		public DumpEventHandler(String dir, Schema schema)
				throws IOException
		{
			DataFileWriter<GenericRecord> writeCreator = new DataFileWriter<GenericRecord>(new GenericDatumWriter(schema));
			directory = new File(dir);

			if (! directory.isDirectory())
				throw new RuntimeException("The path (" + dir + ") either does not exist or is not a directory !!");

			currentFile = new File(directory.getAbsolutePath() + "/part-000.avro");
			writer = writeCreator.create(schema, currentFile);
		}

		@Override
		public void onRecord(DbusEvent event, GenericRecord record) 
		{
			try {
				writer.append(record);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		@Override
		public void onStart(String query) 
		{
			_query = query;
		}

		@Override
		public void onEnd(int count) 
		{
			System.out.println("Read " + count + " records by executing query :( " + _query + ")");
		}

	}
}
