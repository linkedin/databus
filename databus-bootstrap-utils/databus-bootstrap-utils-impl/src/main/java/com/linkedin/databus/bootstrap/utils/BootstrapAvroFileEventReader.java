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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.producers.db.SourceDBEventReader;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RateMonitor;

public class BootstrapAvroFileEventReader 
extends DbusSeederBaseThread
implements SourceDBEventReader
{
	public static final Logger LOG = Logger.getLogger(BootstrapAvroFileEventReader.class.getName());
	private static final long MILLISEC_TO_MIN = (1000 * 60);
	
	private StaticConfig _config;
	private BootstrapEventBuffer _bootstrapEventBuffer;
    private List<OracleTriggerMonitoredSourceInfo> _sources;
	private final Map<String, Long> _lastRows;


	public BootstrapAvroFileEventReader(StaticConfig config, 
				                        List<OracleTriggerMonitoredSourceInfo> sources,
				                         Map<String, Long> lastRows,
                                        BootstrapEventBuffer bootstrapEventBuffer) {
		super("BootstrapAvroFileEventReader");
		_config = config;
		_sources = sources;
		_lastRows = new HashMap<String,Long>(lastRows);
		_bootstrapEventBuffer = bootstrapEventBuffer;
	}

	@Override
	public void run()
	{
	  try
	  {
	    readEventsFromAllSources(0);
	  } catch (Exception ex) {
		 LOG.error("Got Error when executing readEventsFromAllSources !!",ex);
	  }
	  LOG.info(Thread.currentThread().getName() + " done seeding ||");
	}
	
	@Override
	public 	ReadEventCycleSummary readEventsFromAllSources( long sinceSCN) 
			throws DatabusException, EventCreationException,
			UnsupportedKeyException 
	{
	  List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();
	  boolean error = false;

	  long startTS = System.currentTimeMillis();
	  long endScn = -1;
	  long minScn = Long.MAX_VALUE;
	  try
	  {
    	for ( OracleTriggerMonitoredSourceInfo sourceInfo : _sources)
    	{
          endScn = _config.getSeedWindowSCNMap().get(sourceInfo.getEventView());
          minScn = Math.min(endScn,minScn);
    	  LOG.info("Bootstrapping " + sourceInfo.getEventView());
    	  _bootstrapEventBuffer.start(endScn);
    	  
    	  String dir = _config.getAvroSeedInputDirMap().get(sourceInfo.getEventView());
    	  
    	  File d = new File(dir);

    	  EventReaderSummary summary = readEventsFromHadoopFiles(sourceInfo, d, endScn);
    	  // Script assumes seeding is done for one schema at a time
    	  _bootstrapEventBuffer.endEvents(BootstrapEventBuffer.END_OF_SOURCE, endScn, null);
    	  summaries.add(summary);
    	}
      } catch (Exception ex) {
    	error = true;
    	throw new DatabusException(ex);
      } finally {
    	 // Notify writer that I am done
    	if ( error )
    	{
    	  _bootstrapEventBuffer.endEvents(BootstrapEventBuffer.ERROR_CODE, endScn,null);
    	  LOG.error("Seeder stopping unexpectedly !!");
    	} else {
    	  _bootstrapEventBuffer.endEvents(BootstrapEventBuffer.END_OF_FILE, endScn,null);
    	  LOG.info("Completed Seeding !!");
    	}
      }
      LOG.info("Start SCN :" + minScn);
   
      long endTS = System.currentTimeMillis();

      ReadEventCycleSummary cycleSummary = new ReadEventCycleSummary("seeder",
                                                                   summaries, minScn,
                                                                   (endTS - startTS));
	  return cycleSummary;
	}

	private EventReaderSummary readEventsFromHadoopFiles(OracleTriggerMonitoredSourceInfo sourceInfo, File avroSeedDir, Long windowSCN)
	{
	    DataFileReader<GenericRecord> reader = null;
	    
	    File[] files = avroSeedDir.listFiles();

	    List<File> fileList = Arrays.asList(files);
	    
	    Collections.sort(fileList);
	    
    	long numRead = 0;
    	long prevNumRead = 0;

    	long numBytes = 0;
    	long timestamp = System.currentTimeMillis();
    	long timeStart = timestamp;
    	long lastTime = timestamp;
    	long commitInterval = _config.getCommitInterval();
    	long totLatency = 0;
    	GenericRecord record = null;
    	RateMonitor seedingRate = new RateMonitor("Seeding Rate");
    	seedingRate.start();		
    	seedingRate.suspend();
    	
    	long startRowId = _lastRows.get(sourceInfo.getEventView());
    	
    	LOG.info("Last Known Row Id is :" + startRowId);
    	
    	boolean resumeSeedingRate = true;
    	
		for (File avroSeedFile : files)
	    {
	    	if (! avroSeedFile.isFile())
	    		continue;
	    	
	    	LOG.info("Seeding from File : " + avroSeedFile);
	    	
	    	try {
	    		reader = new DataFileReader<GenericRecord>(avroSeedFile, new GenericDatumReader<GenericRecord>());
	    	} catch (IOException e) {
	    		LOG.fatal("Failed to bootstrap from file " + avroSeedFile.getAbsolutePath(), e);
	    		throw new RuntimeException("Failed to bootstrap from file " + avroSeedFile.getAbsolutePath(), e);
	    	}

	    	
	    	try
	    	{	
	    		boolean committed = false;
	    		for (GenericRecord hdfsRecord : reader) 
	    		{
	    			record = hdfsRecord;
	    			committed = false;
	    			numRead++;

		    		if (numRead < startRowId)
		    			continue;
		    		
		    		if (resumeSeedingRate)
		    		{
		    			seedingRate.resume();
		    			resumeSeedingRate = false;
		    		}
		    		
		    		seedingRate.tick();
		    		
	    			//LOG.info("Read record :" + record);	    			
	    			long start = System.nanoTime();
	    			long eventSize = sourceInfo.getFactory().createAndAppendEvent(windowSCN, timestamp, hdfsRecord,
	    									_bootstrapEventBuffer, false, null);	    		
	    			
	    			numBytes+=eventSize;
	    			long latency = System.nanoTime() - start;
	    			totLatency += latency;
	    			if (numRead%commitInterval == 0)
	    			{
	    				_bootstrapEventBuffer.endEvents(numRead,timestamp,null);
	    				_bootstrapEventBuffer.startEvents();
	    				long procTime = totLatency/1000000000;
	    				long currTime = System.currentTimeMillis();
	    				long diff = (currTime - lastTime)/1000;
	    				long timeSinceStart = (currTime - timeStart)/1000;
	    				LOG.info("Processed " + commitInterval + " rows in " + diff
	    						+ " seconds, Avro Processing Time (seconds) so far :" + (procTime)
	    						+ ",Seconds elapsed since start :" + (timeSinceStart)
	    						+ ",Overall Row Rate:" + seedingRate.getRate() +
	    						", NumRows Fetched so far:" + numRead +
	    						". TotalEventSize :" + numBytes);
	    				lastTime = currTime;
	    				seedingRate.resume();
	    				committed = true;
	    			}
	    		}
	    		
	    		if ( ! committed)
	    		{
    				_bootstrapEventBuffer.endEvents(numRead,timestamp,null);
    				_bootstrapEventBuffer.startEvents();
    				long procTime = totLatency/1000000000;
    				long currTime = System.currentTimeMillis();
    				long diff = (currTime - lastTime)/1000;
    				long timeSinceStart = (currTime - timeStart)/1000;
    				LOG.info("Completed Seeding from : " + avroSeedFile + ", Processed " + commitInterval + " rows in " + diff
    						+ " seconds, Avro Processing Time (seconds) so far :" + (procTime)
    						+ ",Seconds elapsed since start :" + (timeSinceStart)
    						+ ",Overall Row Rate:" + seedingRate.getRate() +
    						", NumRows Fetched so far:" + numRead +
    						". TotalEventSize :" + numBytes);
    				lastTime = currTime;
    				seedingRate.resume();
	    		}
	    	} catch (Exception e) {
	    		LOG.fatal("NumRead :" + numRead + ", Got Exception while processing generic record :" + record, e);
	    		throw new RuntimeException(e);
	    	} 
			LOG.info("Processed " + (numRead - prevNumRead) + " rows of Source: " +  sourceInfo.getSourceName() + " from file " + avroSeedFile );
			prevNumRead = numRead;
	    }
		
		long timeEnd = System.currentTimeMillis();
		long elapsedMin = (timeEnd - timeStart)/(MILLISEC_TO_MIN);
		LOG.info("Processed " + numRead + " rows of Source: " +  sourceInfo.getSourceName() + " in " + elapsedMin + " minutes" );
		return new EventReaderSummary(sourceInfo.getSourceId(), sourceInfo.getSourceName(), -1,
										(int)numRead, numBytes, (timeEnd - timeStart),(timeEnd-timeStart)/numRead,0,0,0);
	}
	
	@Override
	public List<OracleTriggerMonitoredSourceInfo> getSources() {
		return _sources;
	}
	
	public Map<String, String> getPKeyNameMap()
	{
		return _config.getPKeyNameMap();
	}
	
	public static class StaticConfig
	{
		private final Map<String, String> avroSeedInputDirMap;
		private final Map<String, Long> seedWindowSCNMap;
		private final Map<String, String> pKeyNameMap;
		private final int commitInterval;
		
		public StaticConfig(Map<String, String> sourceAvroSchemaMap,
				Map<String, Long> seedWindowSCNMap,
				Map<String, String> pKeyNameMap,
				int commitInterval) {
			super();
			this.avroSeedInputDirMap = sourceAvroSchemaMap;
			this.seedWindowSCNMap = seedWindowSCNMap;
			this.pKeyNameMap = pKeyNameMap;
			this.commitInterval = commitInterval;
		}

		public Map<String, String> getAvroSeedInputDirMap() {
			return avroSeedInputDirMap;
		}				

		public Map<String, Long> getSeedWindowSCNMap() {
			return seedWindowSCNMap;
		}

		
		public Map<String, String> getPKeyNameMap() {
			return pKeyNameMap;
		}

		public int getCommitInterval() {
			return commitInterval;
		}				
	}

	public static class Config implements ConfigBuilder<StaticConfig>
	{
		private static final int DEFAULT_COMMIT_INTERVAL = 10000;
		private static final String DEFAULT_AVRO_SEED_INPUT_FILE = "DEFAULT_FILE_NAME";
		private static final Long DEFAULT_WINDOW_SCN = -1L;
		private static final String DEFAULT_PKEY_NAME = "key";

		private HashMap<String, String> avroSeedInputDirMap;
		private int commitInterval;
		private HashMap<String, Long> seedWindowSCNMap;
		private Map<String, String> pKeyNameMap;

		
		public Config()
		{
			avroSeedInputDirMap = new HashMap<String, String>();
			seedWindowSCNMap = new HashMap<String, Long>();
			pKeyNameMap = new HashMap<String, String>();
			commitInterval = DEFAULT_COMMIT_INTERVAL;
		}
		
		public Long getSeedWindowSCN(String sourceName)
		{
			Long scn = seedWindowSCNMap.get(sourceName);
			
			if ( null == scn)
			{
				seedWindowSCNMap.put(sourceName,DEFAULT_WINDOW_SCN);
				return DEFAULT_WINDOW_SCN;
			}
			return scn;
		}
		
		public String getAvroSeedInputDir(String sourceName)
		{
			String file = avroSeedInputDirMap.get(sourceName);

			if ( null == file)
			{
				avroSeedInputDirMap.put(sourceName, DEFAULT_AVRO_SEED_INPUT_FILE);
				return DEFAULT_AVRO_SEED_INPUT_FILE;
			}
			return file;
		}

		public void setSeedWindowSCN(String sourceName, Long scn)
		{
			seedWindowSCNMap.put(sourceName,scn);
		}

		public void setAvroSeedInputDir(String sourceName, String file)
		{
			avroSeedInputDirMap.put(sourceName, file);
		}
		
		public int getCommitInterval() {
			return commitInterval;
		}


		public void setCommitInterval(int commitInterval) {
			this.commitInterval = commitInterval;
		}

		public  String getPKeyName(String srcName)
		{
			String key = pKeyNameMap.get(srcName);
			if ( null == key)
			{
				pKeyNameMap.put(srcName, DEFAULT_PKEY_NAME);
				return DEFAULT_PKEY_NAME;
			}
			return key;
		}
		
	    public void setPKeyName(String srcName, String key)
	    {
	       pKeyNameMap.put(srcName, key);
	    }

		@Override
		public StaticConfig build()
			throws InvalidConfigException
		{
			LOG.info("BootstrapAvroFileEventReader starting with config :" + this.toString());
			
			for( String file : avroSeedInputDirMap.values())
			{
				File f = new File(file);
				
				if (! (f.isDirectory()) || (!f.canRead()))
				{
					LOG.error("File (" + f + ") does not exist or cannot be read !!");
					throw new InvalidConfigException("File (" + f + ") does not exist or cannot be read !!");
				}
			}
						
			return new StaticConfig(avroSeedInputDirMap, seedWindowSCNMap, pKeyNameMap, commitInterval);
		}

		@Override
		public String toString() {
			return "Config [avroSeedInputDirMap=" + avroSeedInputDirMap
					+ ", commitInterval=" + commitInterval
					+ ", seedWindowSCNMap=" + seedWindowSCNMap
					+ ", _pKeyNameMap=" + pKeyNameMap + "]";
		}
		
		
	}
}
