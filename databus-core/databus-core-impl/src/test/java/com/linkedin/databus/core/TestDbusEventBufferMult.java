package com.linkedin.databus.core;
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


import com.linkedin.databus.core.DbusEventBufferMult.PhysicalPartitionKey;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalPartition;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BufferNotFoundException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;
import com.linkedin.databus2.core.filter.ConjunctionDbusFilter;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.LogicalSourceAndPartitionDbusFilter;
import com.linkedin.databus2.core.filter.PhysicalPartitionDbusFilter;
import com.linkedin.databus2.core.filter.SourceDbusFilter;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestDbusEventBufferMult {
  public static final Logger LOG = Logger.getLogger(TestDbusEventBufferMult.class);

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  /** Encapsulate test set so that multiple threads can run in parallel */
  static class TestSetup
  {
    PhysicalSourceStaticConfig [] _physConfigs;
    DbusEventBufferMult _eventBuffer;

    TestSetup() throws JsonParseException, JsonMappingException, IOException, InvalidConfigException
    {
      ObjectMapper mapper = new ObjectMapper();
      InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(_configSource1));

      PhysicalSourceConfig pConfig1 = mapper.readValue(isr, PhysicalSourceConfig.class);

      isr.close();
      isr = new InputStreamReader(IOUtils.toInputStream(_configSource2));
      PhysicalSourceConfig pConfig2 = mapper.readValue(isr, PhysicalSourceConfig.class);

      PhysicalSourceStaticConfig pStatConf1 = pConfig1.build();
      PhysicalSourceStaticConfig pStatConf2 = pConfig2.build();

      _physConfigs =  new PhysicalSourceStaticConfig [] {pStatConf1, pStatConf2};

      DbusEventBuffer.StaticConfig config = new DbusEventBuffer.Config().build();
      _eventBuffer = new DbusEventBufferMult(_physConfigs, config);
      for(DbusEventBuffer b : _eventBuffer.bufIterable()) {
        b.start(1);
      }
    }

  }

  DbusEventBuffer.StaticConfig _config;
  PhysicalSourceStaticConfig [] _pConfigs;
  DbusEventBufferMult _eventBufferMult;
  TestDbusEvent [] _events;
  PhysicalSource _phSource;
  PhysicalPartition _phPartition;

  ObjectMapper _mapper;

  static final String _configSource1 = "{\n" +
  "    \"name\" : \"multBufferTest1\",\n" +
  "    \"id\" : 100,\n" +
  "    \"uri\" : \"uri1\",\n" +
  "        \"slowSourceQueryThreshold\" : 2000,\n" +
  "        \"sources\" :\n" +
  "        [\n" +
  "                {\"id\" : 1, \n" +
  "                 \"name\" : \"srcName1\",\n" +
  "                 \"uri\": \"member2.member_profile\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 0 \n" +
  "                },\n" +
  "                {\"id\" : 2, \n" +
  "                 \"name\" : \"srcName2\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 0 \n" +
  "                },\n" +
  "                {\"id\" : 2, \n" +
  "                 \"name\" : \"srcName2\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 1 \n" +
  "                }\n" +
  "        ]\n" +
  "}";

  static final String _configSource2 = "{\n" +
  "    \"name\" : \"multBufferTest2\",\n" +
  "    \"id\" : 101,\n" +
  "    \"uri\" : \"uri2\",\n" +
  "        \"slowSourceQueryThreshold\" : 2000,\n" +
  "        \"sources\" :\n" +
  "        [\n" +
  "                {\"id\" : 11, \n" +
  "                 \"name\" : \"srcName11\",\n" +
  "                 \"uri\": \"member2.member_profile\", \n" +
  "                 \"partitionFunction\" : \"constant:1\",\n" +
  "                 \"partition\" : 0 \n" +
  "                },\n" +
  "                {\"id\" : 12, \n" +
  "                 \"name\" : \"srcName12\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\",\n" +
  "                 \"partition\" : 0 \n" +
  "                },\n" +
  "                {\"id\" : 12, \n" +
  "                 \"name\" : \"srcName12\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\"\n" +
  "                },\n" +
  "                {\"id\" : 2, \n" +
  "                 \"name\" : \"srcName2\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 2 \n" +
  "                }\n" +
  "        ]\n" +
  "}";

  static final String _configSource3 = "{\n" +
  "    \"name\" : \"multBufferTest3\",\n" +
  "    \"id\" : 102,\n" +
  "    \"uri\" : \"uri3\",\n" +
  "        \"slowSourceQueryThreshold\" : 2000,\n" +
  "        \"sources\" :\n" +
  "        [\n" +
  "                {\"id\" : 21, \n" +
  "                 \"name\" : \"srcName21\",\n" +
  "                 \"uri\": \"member2.member_profile\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 0 \n" +
  "                },\n" +
  "                {\"id\" : 22, \n" +
  "                 \"name\" : \"srcName22\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 0 \n" +
  "                },\n" +
  "                {\"id\" : 23, \n" +
  "                 \"name\" : \"srcName22\",\n" +
  "                 \"uri\" : \"member2.member_account\", \n" +
  "                 \"partitionFunction\" : \"constant:1\", \n" +
  "                 \"partition\" : 0 \n" +
  "                }\n" +
  "        ]\n" +
  "}";

  public PhysicalSourceConfig convertToPhysicalSourceConfig(String str) {
    _mapper = new ObjectMapper();
    InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(str));
    PhysicalSourceConfig pConfig = null;
    try
    {
      pConfig = _mapper.readValue(isr, PhysicalSourceConfig.class);
    }
    catch (JsonParseException e) {
      fail("Failed parsing", e);
    }
    catch (JsonMappingException e){
      fail("Failed parsing", e);
     }
    catch (IOException e){
      fail("Failed parsing", e);
     }

    try
    {
      isr.close();
    }
    catch (IOException e)
    {
      fail("Failed",e);
    }
    return pConfig;
  }

  @BeforeTest
  public void setUpTest () throws IOException, InvalidConfigException {
    LOG.info("Setting up Test");

    PhysicalSourceStaticConfig pStatConf1 = convertToPhysicalSourceConfig(_configSource1).build();
    PhysicalSourceStaticConfig pStatConf2 = convertToPhysicalSourceConfig(_configSource2).build();
    PhysicalSourceStaticConfig pStatConf3 = convertToPhysicalSourceConfig(_configSource3).build();

    _pConfigs =  new PhysicalSourceStaticConfig [] {pStatConf1, pStatConf2, pStatConf3};

    // generate testData
    int scn = 100;
    String srcName = "srcName";
    int srcId = 1;
    PhysicalSource pS = pStatConf1.getPhysicalSource();
    PhysicalPartition pP = pStatConf1.getPhysicalPartition();
    _events = new TestDbusEvent [20];
    LogicalPartition lP = new LogicalPartition((short)0);
    for(int i=0; i<_events.length; i++) {
        _events[i] = new TestDbusEvent(i, scn,
                                       new LogicalSource(srcId, srcName+srcId),
                                       pS, pP, lP);
        switch(i) {
        case 4: srcId =2;
        break;
        case 9: srcId = 11;
        pS = pStatConf2.getPhysicalSource(); pP = pStatConf2.getPhysicalPartition();
        break;
        case 14: srcId = 12; break;
        }
        if((i & 1) == 1) scn++;
    };
  }

  private static class TestDbusEvent {
    public TestDbusEvent(int k, int scn, LogicalSource lS,
                         PhysicalSource pS, PhysicalPartition pP, LogicalPartition lP) {
      _key = k; _scn = scn;
      _lSource = lS; _lPartition=lP;
      _phPartition=pP; _phSource=pS;
    }
    public int _key;
    public int _scn;
    public LogicalSource _lSource;
    public PhysicalPartition _phPartition;
    public PhysicalSource _phSource;
    public LogicalPartition _lPartition;
  }


  // create the bufs - we want a new set for every test
  private void createBufMult() throws InvalidConfigException {
    if(_config == null) {
      try {
        DbusEventBuffer.Config cfgBuilder = new DbusEventBuffer.Config();
        LOG.info("max size = " + cfgBuilder.getMaxSize());
        LOG.info("scnIndex size = " + cfgBuilder.getScnIndexSize());
        cfgBuilder.setMaxSize(10*1024*1024);
        cfgBuilder.setScnIndexSize(2*1024*1024);
        _config = cfgBuilder.build();
        //_config = new DbusEventBuffer.Config().build();
      }
      catch (InvalidConfigException e) {
        fail("invalid configuration" ,e);
      }
    }

    _eventBufferMult = new DbusEventBufferMult(_pConfigs, _config);
    for(DbusEventBuffer b : _eventBufferMult.bufIterable()) {
      b.start(1);
    }
  }

  private void addEvents() {
    byte [] schema = "abcdefghijklmnop".getBytes();
    for(int i=0; i<_events.length; i++) {
      TestDbusEvent e = _events[i];
      DbusEventBufferAppendable buf = _eventBufferMult.getDbusEventBufferAppendable(e._phPartition);
      //System.out.println("i= " + i + "; phS = " + e._phSource + "; phP = " + e._phPartition +
     //                    "; buf=" + buf.hashCode());

      // two events per scn
      if(i%2 == 0)
        buf.startEvents();

      DbusEventKey key = new DbusEventKey(e._key);
      //short lPartitionId = (short) (key.getLongKey() % Short.MAX_VALUE);
      String value = "" + i;
      short srcId = e._lSource.getId().shortValue();
      LogicalPartition lPart = e._lPartition;
      //System.out.print("appending events for i=" + i + "; lSource=" + srcId);
      short pPartitionId = _eventBufferMult.getPhysicalPartition(srcId, lPart).getId().shortValue();
      //System.out.println(";partitionid=" + pPartitionId);

      assertTrue(buf.appendEvent(key, pPartitionId, lPart.getId(), System.currentTimeMillis(), srcId,
                                 schema, value.getBytes(), false, null));

      if(i%2 != 0)
        buf.endEvents(e._scn, null);
    }
  }

  @Test
  public void verifyMultBuffer() throws InvalidConfigException {
    createBufMult();
    assertEquals(_eventBufferMult.bufsNum(), 3);
  }

  @Test
  // reading from the source that belong to the same physical buffer
  public void verifyReadingSingleBuf() throws IOException, ScnNotFoundException, DatabusException, OffsetNotFoundException {
    createBufMult();
    addEvents();
    Set<Integer> srcIds = new HashSet<Integer>(2);
    srcIds.add(1);
    srcIds.add(2);
    // total expected events 15 = 10 original (from first buffer (src 1 and 2) + 5 control events
    batchReading(srcIds, 10);
  }

  @Test
  //reading from the source that belong to two different physical buffers
  public void verifyReadingMultBuf() throws IOException, ScnNotFoundException, InvalidConfigException, DatabusException, OffsetNotFoundException {
    createBufMult();
    addEvents();
    Set<Integer> srcIds = new HashSet<Integer>(2);
    srcIds.add(1);
    srcIds.add(12);

    // total expected events 20 = 20 original events - 10 filtered out (srcs 2 and 11)
    batchReading(srcIds, 10);
  }

  @Test
  public void verifyReadingLogicalPartitionWildcard() throws IOException, ScnNotFoundException, InvalidConfigException, DatabusException, OffsetNotFoundException {
  	 createBufMult();

  	 PhysicalSourceStaticConfig pStatConf1 = convertToPhysicalSourceConfig(_configSource1).build();
     PhysicalSourceStaticConfig pStatConf2 = convertToPhysicalSourceConfig(_configSource2).build();

     PhysicalPartition pP = pStatConf1.getPhysicalPartition();
     DbusEventBufferAppendable buf = _eventBufferMult.getDbusEventBufferAppendable(pP);

     buf.startEvents();
     byte [] schema = "abcdefghijklmnop".getBytes();
     assertTrue(buf.appendEvent(new DbusEventKey(1), (short)100, (short)0,
                                   System.currentTimeMillis() * 1000000, (short)2,
                                   schema, new byte[100], false, null));

     assertTrue(buf.appendEvent(new DbusEventKey(1), (short)100, (short)1,
         System.currentTimeMillis() * 1000000, (short)2,
         schema, new byte[100], false, null));
     buf.endEvents(100, null);

     pP = pStatConf2.getPhysicalPartition();
     buf = _eventBufferMult.getDbusEventBufferAppendable(pP);
     buf.startEvents();

     assertTrue(buf.appendEvent(new DbusEventKey(1), (short)101, (short)2,
         System.currentTimeMillis() * 1000000, (short)2,
         schema, new byte[100], false, null));
     buf.endEvents(200, null);

     Set<Integer> srcIds = new HashSet<Integer>(1);
     srcIds.add(2);

     // total expected events 3 - for pp100:lp=0,pp100:lp=1;pp101:lp=2
     batchReading(srcIds, 3);
  }

  private StatsCollectors<DbusEventsStatisticsCollector> createStats(String[] pNames)
  {
	  DbusEventsStatisticsCollector stats = new AggregatedDbusEventsStatisticsCollector(1, "test", true, true,
              null);
	  StatsCollectors<DbusEventsStatisticsCollector> statsColl = new StatsCollectors<DbusEventsStatisticsCollector>(stats);

	  int count = 1;
	  for (String pname : pNames)
	  {
		  DbusEventsStatisticsCollector s = new DbusEventsStatisticsCollector(1, "test"+count, true, true,
	              null);
		   statsColl.addStatsCollector(pname, s);
		   ++count;
	  }
	  return statsColl;
  }

  @Test
  public void testMultiPPartionStreamStats() throws Exception
  {
    createBufMult();

    PhysicalPartition[] p = {_pConfigs[0].getPhysicalPartition(),
                             _pConfigs[1].getPhysicalPartition(),
                             _pConfigs[2].getPhysicalPartition()
                            };

    //generate a bunch of windows for 3 partitions
    int windowsNum = 10;
    for (int i = 1; i <= windowsNum; ++i)
    {
      DbusEventBufferAppendable buf = _eventBufferMult.getDbusEventBufferAppendable(p[0]);

      buf.startEvents();
      byte [] schema = "abcdefghijklmnop".getBytes();
      assertTrue(buf.appendEvent(new DbusEventKey(1), (short)100, (short)0,
                                    System.currentTimeMillis() * 1000000, (short)2,
                                    schema, new byte[10], false, null));
      buf.endEvents(100 * i, null);

      buf = _eventBufferMult.getDbusEventBufferAppendable(p[1]);
      buf.startEvents();
      assertTrue(buf.appendEvent(new DbusEventKey(1), (short)101, (short)2,
                                 System.currentTimeMillis() * 1000000, (short)2,
                                 schema, new byte[100], false, null));
      assertTrue(buf.appendEvent(new DbusEventKey(2), (short)101, (short)2,
                                 System.currentTimeMillis() * 1000000, (short)2,
                                 schema, new byte[10], false, null));
      buf.endEvents(100 * i + 1, null);

      buf = _eventBufferMult.getDbusEventBufferAppendable(p[2]);
      buf.startEvents();
      assertTrue(buf.appendEvent(new DbusEventKey(1), (short)101, (short)2,
                                 System.currentTimeMillis() * 1000000, (short)2,
                                 schema, new byte[100], false, null));
      assertTrue(buf.appendEvent(new DbusEventKey(2), (short)101, (short)2,
                                 System.currentTimeMillis() * 1000000, (short)2,
                                 schema, new byte[10], false, null));
      assertTrue(buf.appendEvent(new DbusEventKey(3), (short)101, (short)2,
                                 System.currentTimeMillis() * 1000000, (short)2,
                                 schema, new byte[10], false, null));
      buf.endEvents(100 * i + 2, null);
    }
    String[] pnames = new String[p.length];
    int count=0;
    for (PhysicalPartition ip:p)
    {
    	pnames[count++] = ip.toSimpleString();
    }

    StatsCollectors<DbusEventsStatisticsCollector> statsColl = createStats(pnames);

    PhysicalPartitionKey[] pkeys =
      {new PhysicalPartitionKey(p[0]),
       new PhysicalPartitionKey(p[1]),
       new PhysicalPartitionKey(p[2])};

    CheckpointMult cpMult = new CheckpointMult();
    for (int i =0; i < 3; ++i)
    {
      Checkpoint cp = new Checkpoint();
      cp.setFlexible();
      cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
      cpMult.addCheckpoint(p[i], cp);
    }

    DbusEventBufferBatchReadable reader =
        _eventBufferMult.getDbusEventBufferBatchReadable(cpMult, Arrays.asList(pkeys), statsColl);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WritableByteChannel writeChannel = Channels.newChannel(baos);
    reader.streamEvents(false, 1000000, writeChannel, Encoding.BINARY, new AllowAllDbusFilter());
    writeChannel.close();
    baos.close();

    //make sure we got the physical partition names right
    List<String> ppartNames = statsColl.getStatsCollectorKeys();
    assertEquals(ppartNames.size(), 3);

    HashSet<String> expectedPPartNames = new HashSet<String>(Arrays.asList(p[0].toSimpleString(),
                                                                           p[1].toSimpleString(),
                                                                           p[2].toSimpleString()));
    for (String ppartName: ppartNames)
    {
      assertTrue(expectedPPartNames.contains(ppartName));
    }

    //verify event counts per partition
    DbusEventsTotalStats[] ppartStats = {statsColl.getStatsCollector(p[0].toSimpleString()).getTotalStats(),
                                         statsColl.getStatsCollector(p[1].toSimpleString()).getTotalStats(),
                                         statsColl.getStatsCollector(p[2].toSimpleString()).getTotalStats()};
    assertEquals(ppartStats[0].getNumDataEvents(), windowsNum);
    assertEquals(ppartStats[1].getNumDataEvents(), windowsNum * 2);
    assertEquals(ppartStats[2].getNumDataEvents(), windowsNum * 3);
    assertEquals(ppartStats[0].getNumSysEvents(), windowsNum);
    assertEquals(ppartStats[1].getNumSysEvents(), windowsNum);
    assertEquals(ppartStats[2].getNumSysEvents(), windowsNum);

    assertEquals(statsColl.getStatsCollector().getTotalStats().getNumDataEvents(), windowsNum * (1 + 2 + 3));
    assertEquals(statsColl.getStatsCollector().getTotalStats().getNumSysEvents(), windowsNum * 3);

    assertEquals(statsColl.getStatsCollector().getTotalStats().getMaxTimeLag(),
                 Math.max(ppartStats[0].getTimeLag(), Math.max(ppartStats[1].getTimeLag(), ppartStats[2].getTimeLag())));
    assertEquals(statsColl.getStatsCollector().getTotalStats().getMinTimeLag(),
                 Math.min(ppartStats[0].getTimeLag(), Math.min(ppartStats[1].getTimeLag(), ppartStats[2].getTimeLag())));
  }

  private void batchReading(Set<Integer> srcIds, int expectEvents)
  throws IOException, ScnNotFoundException, DatabusException, OffsetNotFoundException {

    // for debug generate this line
    String inputSrcIds = Arrays.toString(srcIds.toArray());

    LOG.info("Reading events from " + inputSrcIds);

    // create checkpoints
    CheckpointMult cpMult = new CheckpointMult();
    for(PhysicalSourceStaticConfig pConf : _pConfigs) {
      PhysicalPartition pPart = pConf.getPhysicalPartition();
      Checkpoint cp;
      if(cpMult.getCheckpoint(pPart) == null) { // needs a new checkpoint
        cp = new Checkpoint();
        cp.setFlexible();
        cpMult.addCheckpoint(pPart, cp);
      }
    }

    DbusEventBufferBatchReadable read =
      _eventBufferMult.getDbusEventBufferBatchReadable(srcIds, cpMult, null);

    int batchFetchSize = 2000;
    int totalRead = 0;
    int numEventsRead = Integer.MAX_VALUE;
    int numNonControlEventsRead = 0;
    int maxIterNum = 100, iterCount = 0;
    while(numEventsRead > 0) {
      if(iterCount++ > maxIterNum) {
        fail("Checkpoint doesn't work - it is a never-ending loop");
      }
      ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
      WritableByteChannel jsonOutChannel = Channels.newChannel(jsonOut);

      numEventsRead = read.streamEvents(false, batchFetchSize, jsonOutChannel,
                                        Encoding.JSON_PLAIN_VALUE,  new SourceDbusFilter(srcIds));

      totalRead += numEventsRead;
      LOG.info("read for " + inputSrcIds + ": " + numEventsRead + " events");
      byte[] jsonBytes = jsonOut.toByteArray();
      if(jsonBytes.length == 0)
        break; // nothing more to read
      String jsonString = new String(jsonBytes);
      String [] jsonStrings = jsonString.split("\n");
      assertEquals(jsonStrings.length, numEventsRead);

      ObjectMapper mapper = new ObjectMapper();

      for(int i=0; i<jsonStrings.length; i++) {
        // verify what was written
        Map<String, Object> jsonMap = mapper.readValue(jsonStrings[i],
                                                       new TypeReference<Map<String, Object>>(){});
        assertEquals(jsonMap.size(), 10);
        if(jsonMap.get("opcode") != null) { // not a control message
          numNonControlEventsRead++;
          Integer srcId = (Integer)jsonMap.get("srcId");
          Integer physicalPartitionId = (Integer)jsonMap.get("physicalPartitionId");
          Integer logicalPartitionId = (Integer)jsonMap.get("logicalPartitionId");
          PhysicalPartition pPartition = _eventBufferMult.getPhysicalPartition(srcId,
                                         new LogicalPartition(logicalPartitionId.shortValue()));
          LOG.info("EVENT: " + jsonMap.toString());
          assertTrue( srcIds.contains(srcId), "src id " + srcId + " doesn't match to " + inputSrcIds);
          assertEquals(physicalPartitionId, pPartition.getId(), "physical partition id didn't match");
        } else {
          LOG.info("Control event: " + jsonMap.toString());
        }
      }
    }
    assertTrue(totalRead>numNonControlEventsRead);
    assertEquals(numNonControlEventsRead, expectEvents);

  }

  @Test
  public void testGetBuffer() throws InvalidConfigException {
    // get buffer by physical partition
    createBufMult();
    addEvents();
    Integer pPartitionId = 100;

    // get buffer by partition id
    PhysicalPartition pPartition = new PhysicalPartition(pPartitionId, "multBufferTest1");
    DbusEventBufferAppendable buf = _eventBufferMult.getDbusEventBufferAppendable(pPartition);
    assertNotNull(buf,"cannot get by pPartition" + pPartition);

    // get buffer by physical partition
    buf = _eventBufferMult.getDbusEventBufferAppendable(pPartition);
    assertNotNull(buf,"cannot get by pPartition" + pPartition);

    // get buffer by logical source id
    int lSrcId = 1;
    buf = _eventBufferMult.getDbusEventBufferAppendable(lSrcId);
    assertNotNull(buf,"cannot get by lSrcId" + lSrcId);


    // logical source 2 , default partition  - SAME BUFFER
    lSrcId = 2;
    LogicalSource lSource = new LogicalSource(lSrcId, "srcName2"); // correct source name, default partition 0
    DbusEventBufferAppendable buf1 = _eventBufferMult.getDbusEventBufferAppendable(lSource);
    assertNotNull(buf1, "cannot get buffer by lSource " + lSource);
    assertTrue(buf1 == buf, "buf and buf1 should be the same buffer");

    // logical source, different logical partition 1 - SAME BUFFER
    LogicalPartition lPartition = new LogicalPartition((short)1);
    buf1 = _eventBufferMult.getDbusEventBufferAppendable(lSource, lPartition);
    // should be the same buffer
    assertNotNull(buf1, "cannot get buffer by lSource " + lSource + ";lPartition =" + lPartition);
    assertTrue(buf1 == buf, "buf and buf1 should be the same buffer");

    // logical source, different logical partition 2 - DIFFERENT BUFFER
    lPartition = new LogicalPartition((short)2);
    buf1 = _eventBufferMult.getDbusEventBufferAppendable(lSource, lPartition);
    assertNotNull(buf1, "cannot get buffer by lSource " + lSource + ";lPartition =" + lPartition);
    assertTrue(buf1 != buf, "buf and buf1 should not be the same buffer");

    // logical source, different logical partition 12 - DIFFERENT BUFFER (same as for lsr=2,lp=2)
    DbusEventBufferAppendable buf2 = _eventBufferMult.getDbusEventBufferAppendable(12);
    assertNotNull(buf2, "cannot get buffer by lSourceid " + 12);
    assertTrue(buf2 != buf, "buf and buf2 should not be the same buffer");
    assertTrue(buf1 == buf2, "buf2 and buf1 should be the same buffer");

  }

  @Test
  public void testReset() throws Exception {
    createBufMult();
    addEvents();
    Integer pPartitionId = 100;
    PhysicalPartition pPartition = new PhysicalPartition(pPartitionId, "multBufferTest1");

    _eventBufferMult.resetBuffer(pPartition, 108L);
    DbusEventBufferAppendable eventBuffer = _eventBufferMult.getDbusEventBufferAppendable(pPartition);

    Assert.assertTrue(eventBuffer.empty());
    pPartition = new PhysicalPartition(pPartitionId, "unknownBuffer");
    boolean caughtException = false;
    try
    {
      _eventBufferMult.resetBuffer(pPartition, -1L);
    }
    catch(BufferNotFoundException e)
    {
      caughtException = true;
    }
    assertTrue(caughtException);
  }

  @Test
  public void testConstructFilters() throws Exception
  {
    TestSetup t = new TestSetup();

    //test single Physical Partition subscription
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(new PhysicalPartition(100, "multBufferTest1"));

    DbusFilter filter1 = t._eventBuffer.constructFilters(Arrays.asList(sub1));
    assertNotNull(filter1);
    assertTrue(filter1 instanceof PhysicalPartitionDbusFilter);
    PhysicalPartitionDbusFilter ppfilter1 = (PhysicalPartitionDbusFilter)filter1;
    assertEquals(ppfilter1.getPhysicalPartition(), new PhysicalPartition(100, "multBufferTest1"));
    assertNull(ppfilter1.getNestedFilter());

    DatabusSubscription sub2 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(new PhysicalPartition(101, "multBufferTest2"));

    //test two Physical Partition subscriptions
    DbusFilter filter2 = t._eventBuffer.constructFilters(Arrays.asList(sub1, sub2));
    assertNotNull(filter2);
    assertTrue(filter2 instanceof ConjunctionDbusFilter);
    ConjunctionDbusFilter conjFilter2 = (ConjunctionDbusFilter)filter2;
    boolean hasPP100 = false;
    boolean hasPP101 = false;
    assertEquals(conjFilter2.getFilterList().size(), 2);
    for (DbusFilter f: conjFilter2.getFilterList())
    {
      assertTrue(f instanceof PhysicalPartitionDbusFilter);
      PhysicalPartitionDbusFilter ppf = (PhysicalPartitionDbusFilter)f;
      if (ppf.getPhysicalPartition().getId() == 100) hasPP100 = true;
      else if (ppf.getPhysicalPartition().getId() == 101) hasPP101 = true;
      else fail("unknown physical partition filter:" + ppf.getPhysicalPartition());
    }
    assertTrue(hasPP100);
    assertTrue(hasPP101);

    //test a subcription with a logical source
    DatabusSubscription sub3 =
        DatabusSubscription.createSimpleSourceSubscription(new LogicalSource(2, "srcName2"));

    DbusFilter filter3 = t._eventBuffer.constructFilters(Arrays.asList(sub3));
    assertNotNull(filter3);
    assertTrue(filter3 instanceof PhysicalPartitionDbusFilter);
    PhysicalPartitionDbusFilter ppfilter3 = (PhysicalPartitionDbusFilter)filter3;
    assertEquals(ppfilter3.getPhysicalPartition(), PhysicalPartition.ANY_PHYSICAL_PARTITION);
    DbusFilter ppfilter3_child = ppfilter3.getNestedFilter();
    assertNotNull(ppfilter3_child);
    assertTrue(ppfilter3_child instanceof LogicalSourceAndPartitionDbusFilter);
    LogicalSourceAndPartitionDbusFilter lsourceFilter3 = (LogicalSourceAndPartitionDbusFilter)ppfilter3_child;
    LogicalSourceAndPartitionDbusFilter.LogicalPartitionDbusFilter lpartFilter3_1 =
        lsourceFilter3.getSourceFilter(2);
    assertNotNull(lpartFilter3_1);
    assertTrue(lpartFilter3_1.isAllPartitionsWildcard());

    //test a subcription with a physical and logical partition
    DatabusSubscription sub4 =
        new DatabusSubscription(PhysicalSource.MASTER_PHISYCAL_SOURCE,
                                new PhysicalPartition(101, "multBufferTest2"),
                                new LogicalSourceId(new LogicalSource(2, "srcName2"), (short)2)
                                );

    DbusFilter filter4 = t._eventBuffer.constructFilters(Arrays.asList(sub4));
    assertNotNull(filter4);
    assertTrue(filter4 instanceof PhysicalPartitionDbusFilter);
    PhysicalPartitionDbusFilter ppfilter4 = (PhysicalPartitionDbusFilter)filter4;
    assertEquals(ppfilter4.getPhysicalPartition(), new PhysicalPartition(101, "multBufferTest2"));
    DbusFilter ppfilter4_child = ppfilter4.getNestedFilter();
    assertNotNull(ppfilter4_child);
    assertTrue(ppfilter4_child instanceof LogicalSourceAndPartitionDbusFilter);
    LogicalSourceAndPartitionDbusFilter lsourceFilter4 = (LogicalSourceAndPartitionDbusFilter)ppfilter4_child;
    LogicalSourceAndPartitionDbusFilter.LogicalPartitionDbusFilter lpartFilter4_1 =
        lsourceFilter4.getSourceFilter(2);
    assertNotNull(lpartFilter4_1);
    assertTrue(lpartFilter4_1.getPartitionsMask().contains(2));
  }

  @Test
  public void testSubscriptionStream() throws Exception
  {
    TestSetup t = new TestSetup();

    PhysicalPartition pp100 = new PhysicalPartition(100, "multBufferTest1");
    PhysicalPartitionKey pk1 = new PhysicalPartitionKey(pp100);
    PhysicalPartition pp101 = new PhysicalPartition(101, "multBufferTest2");
    PhysicalPartitionKey pk2 = new PhysicalPartitionKey(pp101);

    //generate events in pp100
    byte [] schema = "abcdefghijklmnop".getBytes();
    DbusEventBufferAppendable buf100 =
        t._eventBuffer.getDbusEventBufferAppendable(pp100);
    buf100.startEvents();
    assertTrue(buf100.appendEvent(new DbusEventKey(1), (short)100, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)1,
                                  schema, new byte[100], false, null));
    assertTrue(buf100.appendEvent(new DbusEventKey(10), (short)100, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)1,
                                  schema, new byte[100], false, null));
    assertTrue(buf100.appendEvent(new DbusEventKey(11), (short)100, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)1,
                                  schema, new byte[100], false, null));
    assertTrue(buf100.appendEvent(new DbusEventKey(2), (short)100, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    buf100.endEvents(100,  null);
    buf100.startEvents();
    assertTrue(buf100.appendEvent(new DbusEventKey(3), (short)100, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    assertTrue(buf100.appendEvent(new DbusEventKey(4), (short)100, (short)1,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    buf100.endEvents(200, null);

    //generate events in pp100
    DbusEventBufferAppendable buf101 =
        t._eventBuffer.getDbusEventBufferAppendable(pp101);
    buf101.startEvents();
    assertTrue(buf101.appendEvent(new DbusEventKey(51), (short)101, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)11,
                                  schema, new byte[100], false, null));
    assertTrue(buf101.appendEvent(new DbusEventKey(52), (short)101, (short)0,
                                  System.currentTimeMillis() * 1000000, (short)12,
                                  schema, new byte[100], false, null));
    assertTrue(buf101.appendEvent(new DbusEventKey(53), (short)101, (short)2,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    buf101.endEvents(120,  null);
    buf101.startEvents();
    assertTrue(buf101.appendEvent(new DbusEventKey(54), (short)101, (short)2,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    assertTrue(buf101.appendEvent(new DbusEventKey(55), (short)101, (short)2,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    assertTrue(buf101.appendEvent(new DbusEventKey(56), (short)101, (short)2,
                                  System.currentTimeMillis() * 1000000, (short)2,
                                  schema, new byte[100], false, null));
    buf101.endEvents(200,  null);

    //initialization
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(new PhysicalPartition(100, "multBufferTest1"));

    DbusFilter filter1 = t._eventBuffer.constructFilters(Arrays.asList(sub1));
    assertNotNull(filter1);

    CheckpointMult cpMult1 = new CheckpointMult();
    Checkpoint cp100 = new Checkpoint();
    cp100.init();
    cp100.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp100.setWindowScn(10L);
    cp100.setWindowOffset(-1);
    cpMult1.addCheckpoint(pp100, cp100);

    String[] pnames = {"multBufferTest1:100","multBufferTest2:101"};
    StatsCollectors<DbusEventsStatisticsCollector> statsColls1 = createStats(pnames);
    DbusEventsStatisticsCollector statsCol1 = statsColls1.getStatsCollector("multBufferTest1:100");
    DbusEventsStatisticsCollector statsCol2 = statsColls1.getStatsCollector("multBufferTest2:101");

    //read an entire buffer
    DbusEventBufferBatchReadable reader1 =
        t._eventBuffer.getDbusEventBufferBatchReadable(cpMult1, Arrays.asList(pk1), statsColls1);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int eventsRead = reader1.streamEvents(false, 1000000, Channels.newChannel(baos),
                                          Encoding.BINARY, filter1);
    assertEquals(eventsRead, 8); //4 events + 1 eop + 2 events + 1 eop
    assertEquals(statsColls1.getStatsCollector("multBufferTest1:100").getTotalStats().getNumSysEvents(), 2);
    assertEquals(statsColls1.getStatsCollector("multBufferTest1:100").getTotalStats().getNumDataEvents(), 6);

    baos.reset();
    statsCol1.reset(); statsCol2.reset();

    //read from two buffers, filtering out one
    cpMult1 = new CheckpointMult();
    cp100.init();
    cp100.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp100.setWindowScn(10L);
    cp100.setWindowOffset(-1);
    cpMult1.addCheckpoint(pp100, cp100);
    reader1 = t._eventBuffer.getDbusEventBufferBatchReadable(cpMult1, Arrays.asList(pk1, pk2),
                                                             statsColls1);

    eventsRead = reader1.streamEvents(false, 1000000, Channels.newChannel(baos),
                                      Encoding.BINARY, filter1);
    assertEquals(eventsRead, 10); //4 events + 1 eop + 1 eop from the other buffer + 2 events +
                                  //1 eop + 1 eop from the other buffer
    assertEquals(statsColls1.getStatsCollector("multBufferTest1:100").getTotalStats().getNumSysEvents(), 2);
    assertEquals(statsColls1.getStatsCollector("multBufferTest1:100").getTotalStats().getNumDataEvents(), 6);

    baos.reset();
    statsCol1.reset();

    //read from one buffer and one source partition
    DatabusSubscription sub2 =
        new DatabusSubscription(PhysicalSource.MASTER_PHISYCAL_SOURCE,
                                new PhysicalPartition(101, "multBufferTest2"),
                                new LogicalSourceId(new LogicalSource(2, "srcName2"), (short)2)
                                );

    DbusFilter filter2 = t._eventBuffer.constructFilters(Arrays.asList(sub2));
    assertNotNull(filter2);

    CheckpointMult cpMult2 = new CheckpointMult();
    Checkpoint cp101 = new Checkpoint();
    cp101.init();
    cp101.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp101.setWindowScn(10L);
    cp101.setWindowOffset(-1);
    cpMult2.addCheckpoint(pp101, cp101);
    DbusEventBufferBatchReadable reader2 =
        t._eventBuffer.getDbusEventBufferBatchReadable(cpMult2, Arrays.asList(pk2), statsColls1);

    eventsRead = reader2.streamEvents(false, 1000000, Channels.newChannel(baos),
                                      Encoding.BINARY, filter2);
    assertEquals(eventsRead, 6); //1 events + 1 eop + 3events + 1 eop

    baos.reset();
    statsCol1.reset();statsCol2.reset();

    //read all partitions for a source
    DatabusSubscription sub3 =
        new DatabusSubscription(PhysicalSource.MASTER_PHISYCAL_SOURCE,
                                PhysicalPartition.ANY_PHYSICAL_PARTITION,
                                LogicalSourceId.createAllPartitionsWildcard(new LogicalSource(2, "srcName2"))
                                );

    DbusFilter filter3 = t._eventBuffer.constructFilters(Arrays.asList(sub3));
    assertNotNull(filter3);


    CheckpointMult cpMult3 = new CheckpointMult();
    cp100.init();
    cp100.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp100.setWindowScn(10L);
    cp100.setWindowOffset(-1);
    cpMult1.addCheckpoint(pp100, cp100);
    cp101.init();
    cp101.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    cp101.setWindowScn(10L);
    cp101.setWindowOffset(-1);
    cpMult2.addCheckpoint(pp101, cp101);
    DbusEventBufferBatchReadable reader3 =
        t._eventBuffer.getDbusEventBufferBatchReadable(cpMult3, Arrays.asList(pk1, pk2), statsColls1);
    eventsRead = reader3.streamEvents(false, 1000000, Channels.newChannel(baos),
                                      Encoding.BINARY, filter3);
    assertEquals(eventsRead, 11); //1 events + 1 eop + 1 events + 1 eop + 2 events + 1 eop + 3 events + 1 eop
    assertEquals(statsColls1.getStatsCollector("multBufferTest1:100").getTotalStats().getNumSysEvents(), 2);
    assertEquals(statsColls1.getStatsCollector("multBufferTest2:101").getTotalStats().getNumSysEvents(), 2);
    assertEquals(statsColls1.getStatsCollector("multBufferTest1:100").getTotalStats().getNumDataEventsFiltered(), 3);
    assertEquals(statsColls1.getStatsCollector("multBufferTest2:101").getTotalStats().getNumDataEventsFiltered(), 4);

    baos.reset();
    statsCol1.reset(); statsCol2.reset();
  }

  @Test
  public void addRemoveBuffers() throws IOException, ScnNotFoundException, InvalidConfigException, DatabusException, OffsetNotFoundException {
    createBufMult();
    addEvents();
    Set<Integer> srcIds = new HashSet<Integer>(2);
    srcIds.add(1);

    // total expected events from source 1 is 5
    batchReading(srcIds, 5);
    PhysicalSourceStaticConfig pConfig = convertToPhysicalSourceConfig(_configSource3).build();
    LOG.info("one more buffer for " + pConfig);
    DbusEventBuffer buf = _eventBufferMult.addNewBuffer(pConfig, _config);
    buf.start(100);

    // add events to the new buffer
    byte [] schema = "ABCDEFGHIJKLMNOP".getBytes();
    for(int i=100; i<110; i++) {
      if(i%2 == 0)
        buf.startEvents();
      assertTrue(buf.appendEvent(new DbusEventKey(i),
                                 pConfig.getPhysicalPartition().getId().shortValue(),
                                 (short)0, // logical source id
                                 System.currentTimeMillis(),
                                 (short)(i<105?21:22),
                                 schema,
                                 (""+i).getBytes(), false, null));
      if(i%2 == 1)
        buf.endEvents(i);
    }

    // read from one old and one new buffer
    srcIds.clear();
    srcIds.add(12);
    srcIds.add(22);
    // total expected events 10 - 5 from source 12, and 5 from source 22
    batchReading(srcIds, 10);

    // remove a buf
    pConfig = convertToPhysicalSourceConfig(_configSource2).build();
    _eventBufferMult.removeBuffer(pConfig);
    _eventBufferMult.deallocateRemovedBuffers(true);
    srcIds.clear();
    srcIds.add(11);
    boolean failed = false;
    try {
      batchReading(srcIds, 5);
      failed = false; // didn't fail
    } catch (java.lang.AssertionError ae) {
      // expected
      failed = true;
    }
    Assert.assertTrue(failed, "should've failed for src 11, since the buffer was removed"+pConfig);
  }

  @Test
  public void addRemoveBufferMapping() throws InvalidConfigException, DatabusException {
    createBufMult();

    PhysicalSourceStaticConfig pConfig = convertToPhysicalSourceConfig(_configSource2).build();
    PhysicalPartition pPart = _eventBufferMult.getPhysicalPartition(11);
    Assert.assertNotNull(pPart);
    DbusEventBuffer buf = _eventBufferMult.getDbusEventBuffer(pConfig.getSources()[0].getLogicalSource());
    Assert.assertNotNull(buf);
    DbusEventBuffer buf1 = _eventBufferMult.getOneBuffer(pPart);
    Assert.assertNotNull(buf1);
    Assert.assertTrue(buf == buf1, "same references to the buffer");

    // now remove the buffer and the mapping
    _eventBufferMult.removeBuffer(pConfig);
    _eventBufferMult.deallocateRemovedBuffers(true);

    // same should fail
    pPart = _eventBufferMult.getPhysicalPartition(11);
    Assert.assertNull(pPart);
    buf = _eventBufferMult.getDbusEventBuffer(pConfig.getSources()[0].getLogicalSource());
    Assert.assertNull(buf);
    buf1 = _eventBufferMult.getOneBuffer(pPart);
    Assert.assertNull(buf1);


  }

}
