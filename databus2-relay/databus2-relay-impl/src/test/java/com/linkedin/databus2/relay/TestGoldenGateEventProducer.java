package com.linkedin.databus2.relay;

import static org.testng.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RateControl;
import com.linkedin.databus.core.util.RateMonitor.MockRateMonitor;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics.TransactionInfo;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.ggParser.XmlStateMachine.ColumnsState;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState.DBUpdateImage;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionState;
import com.linkedin.databus2.producers.db.EventSourceStatisticsIface;
import com.linkedin.databus2.producers.gg.GGEventGenerationFactory;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.MissingValueBehavior;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaId;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

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



public class TestGoldenGateEventProducer {
  public static Logger LOG = Logger.getLogger(TestGoldenGateEventProducer.class);

  private final String avroSchema =
    "{" +
    "\"type\" : \"record\"," +
    "\"name\" : \"user\"," +
    "\"namespace\" : \"my.example\"," +
    "\"fields\" : [{\"name\" : \"name\", \"type\" : \"string\"}]" +
    "}";

  private final String avroSchema2 =
      "{" +
      "\"type\" : \"record\"," +
      "\"name\" : \"user\"," +
      "\"namespace\" : \"my.example\"," +
      "\"fields\" : [{\"name\" : \"name1\", \"type\" : \"string\"}," +
      "              {\"name\" : \"name2\", \"type\" : \"string\"}]" +
      "}";

  private final String sourceAvroSchema = "{"
  + "  \"name\" : \"Shortlinks_V3\",  "
  + "   \"doc\" : \"Auto-generated Avro schema for SHORTLINKS. Generated at May 21, 2013 12:05:14 PM PDT\","
  + " \"type\" : \"record\", "
  + "   \"meta\" : \"dbFieldName=SHORTLINKS;dbFieldType=SHORTLINKS;pk=linkCode;\","
  + "   \"fields\" : [ {"
  + "     \"name\" : \"linkCode\", "
  + "     \"type\" : [ \"string\", \"null\" ],"
  + "     \"meta\" : \"dbFieldName=LINK_CODE;dbFieldPosition=0;dbFieldType=VARCHAR2;\""
  + "   }, {"
  + "     \"name\" : \"ggModiTs\","
  + "     \"type\" : [ \"long\", \"null\" ],"
  + "     \"meta\" : \"dbFieldName=GG_MODI_TS;dbFieldPosition=8;dbFieldType=TIMESTAMP;\""
  + "   }, {"
  + "     \"name\" : \"ggStatus\","
  + "     \"type\" : [ \"string\", \"null\" ],"
  + "     \"meta\" : \"dbFieldName=GG_STATUS;dbFieldPosition=9;dbFieldType=VARCHAR2;\""
  + "   } ],"
  + "   \"namespace\" : \"com.linkedin.events.sourc\""
  + " }";


  private final String SCNPATTERN = "SCNPATTERN";
  private final String _transactionPattern = ""
  + "<transaction timestamp=\"2013-07-29:13:26:15.000000\">\n"
  + "<dbupdate table=\"part1.source1\" type=\"insert\">\n"
  + "    <columns>\n"
  + "      <column name=\"LINK_CODE\" key=\"true\">100</column>\n"
  + "      <column name=\"GG_MODI_TS\">2013-07-28:13:26:15.208130000</column>\n"
  + "      <column name=\"GG_STATUS\">o</column>\n"
  + "    </columns>\n"
  + "    <tokens>\n"
  + "      <token name=\"TK-XID\">4.24.94067</token>\n"
  + "      <token name=\"TK-CSN\">" + SCNPATTERN + "</token>\n"
  + "      <token name=\"TK-UNAME\">SOURCE1</token>\n"
  + "    </tokens>\n"
  + "  </dbupdate>\n"
  + "  <dbupdate table=\"part1.source2\" type=\"insert\">\n"
  + "    <columns>\n"
  + "      <column name=\"LINK_CODE\" key=\"true\">101</column>\n"
  + "      <column name=\"GG_MODI_TS\">2013-07-29:13:26:15.212166000</column>\n"
  + "      <column name=\"GG_STATUS\">o</column>\n"
  + "    </columns>\n"
  + "    <tokens>\n"
  + "      <token name=\"TK-XID\">4.24.94067</token>\n"
  + "      <token name=\"TK-CSN\">" + SCNPATTERN + "</token>\n"
  + "      <token name=\"TK-UNAME\">SOURCE2</token>\n"
  + "    </tokens>\n"
  + "  </dbupdate>\n"
  + "</transaction>";

  private final int _transactionPatternSize = _transactionPattern.length() - 7*2; // -7 to compensate diff in length betwee "SCNPATTERN" and length of actuall scn (length("200"))

  static {
    TestUtil.setupLogging(Level.INFO);
  }

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    String rootDirPath = System.getProperty(TestGoldenGateEventProducer.class
        .getName() + ".rootDir");
    // set up logging
    TestUtil.setupLoggingWithTimestampedFile(true,
        "/tmp/TestGoldenGateEventProducer_", ".log", Level.INFO);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
  }

  @Test
  public void testRegexParsing() throws Exception
  {
    //Set SCN

    Pattern rexpr = Pattern.compile("(<token\\s+name=\"TK-CSN\"\\s*>\\s*([0-9]+)\\s*</token>)");

    String source1 = "<token name=\"TK-CSN\">1234</token><token name=\"TK-CSN\">1234</token>";
    Matcher result1 = rexpr.matcher(source1);
    while (result1.find())
    {
        String m = result1.group(2);
        long newScn =  Long.parseLong(m);
        Assert.assertEquals(newScn, 1234);
    }

    String source2 = "<token   name=\"TK-CSN\"   >  1234  </token><token   name=\"TK-CSN\" >1234 </token>";
    Matcher result2 = rexpr.matcher(source2);
    while (result2.find())
    {
        String m = result2.group(2);
        long newScn =  Long.parseLong(m);
        Assert.assertEquals(newScn, 1234);
    }

  }

  private void bar(Long[] l)
  {
    l[0] = 5L;
  }

  @Test
  public void testPoorMansPassingByReference()
  {
    Long l = 2L;
    Long[] al = new Long[1];
    al[0] = l;
    bar(al);
    Assert.assertEquals(5L, al[0].longValue());
  }

  /**
   * test collection of parser stats, especially lag between parsed and added files
   * @throws Exception
   */
  @Test
  public void testGGParserStats() throws Exception
  {
    short [] sourceIds = new short [] { 505, 506 };
    String [] sourceNames = new String [] { "source1", "source2"};


    // setup trail Files directory
    File ggTrailDir = new File("/tmp/ggTrailTestFiles");
    if(ggTrailDir.exists()) {
      FileUtils.cleanDirectory(ggTrailDir);
    }
    if(!ggTrailDir.exists()) {
      ggTrailDir.mkdir();
    }

    // configure phisical source
    String uri =  "gg://" + ggTrailDir.getAbsolutePath() + ":x3";
    PhysicalSourceStaticConfig pssc = buildSimplePssc(sourceIds, sourceNames, uri);
    LOG.info("Uri=" + uri);

    // create schema
    Schema s = Schema.parse(sourceAvroSchema);
    VersionedSchema vs = new VersionedSchema(new VersionedSchemaId("source1",(short)3), s, null);

    // mock for schema registry
    SchemaRegistryService srs = EasyMock.createMock(SchemaRegistryService.class);
    EasyMock.expect(srs.fetchLatestVersionedSchemaBySourceName("source1")).andReturn(vs).anyTimes();
    EasyMock.expect(srs.fetchLatestVersionedSchemaBySourceName("source2")).andReturn(vs).anyTimes();
    EasyMock.expect(srs.fetchLatestVersionedSchemaBySourceName(null)).andReturn(vs);

    // mock for MaxSCNReadWriter
    MaxSCNReaderWriter mscn = EasyMock.createMock(MaxSCNReaderWriter.class);
    EasyMock.expect(mscn.getMaxScn()).andReturn((long)-2).atLeastOnce();
    mscn.saveMaxScn(EasyMock.anyLong());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(mscn);
    EasyMock.replay(srs);

    int totalTransWritten = 0;
    int totalFilesWritten = 0;

    // buffer
    DbusEventBufferAppendable mb = createBufMult(pssc);

    // start GG producer
    GoldenGateEventProducer gg = new GoldenGateEventProducer(pssc, srs, mb,
                                                             null, mscn);

    //create first 2 files
    addToTrailFile(new File(ggTrailDir.getAbsolutePath() + "/x301"), 100, 4);
    addToTrailFile(new File(ggTrailDir.getAbsolutePath() + "/x302"), 200, 4);
    totalTransWritten = 8;
    totalFilesWritten = 2;

    // get hold of parser stats object
    final GGParserStatistics ggParserStats = gg.getParserStats();

    // all should be 0
    Assert.assertEquals(0, ggParserStats.getNumFilesParsed());
    Assert.assertEquals(0, ggParserStats.getNumFilesAdded());
    Assert.assertEquals(0, ggParserStats.getFilesLag());
    Assert.assertEquals(0, ggParserStats.getTimeLag());
    Assert.assertEquals(0, ggParserStats.getBytesLag());


    try {
      LOG.info("starting event producer");
      gg.start(-2); // -2 here does nothing. actual setting happens thru the mock of MaxSCNReadWriter
      // let it parse first files

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return ggParserStats.getNumFilesParsed() == 2 &&
              (8*_transactionPatternSize ==  ggParserStats.getNumBytesTotalParsed());
        }
      }, "First two files parsed", 2000, LOG);

      // stats in the interim
      Assert.assertEquals(2, ggParserStats.getNumFilesParsed());
      Assert.assertEquals(2, ggParserStats.getNumFilesAdded());
      Assert.assertEquals(0, ggParserStats.getFilesLag());
      Assert.assertEquals(0, ggParserStats.getTimeLag());
      Assert.assertEquals(0, ggParserStats.getBytesLag());

      Assert.assertEquals(totalTransWritten*_transactionPatternSize, ggParserStats.getNumBytesTotalParsed());


      gg.pause();

      // the file will get parsed but not processed
      addToTrailFile(new File(ggTrailDir.getAbsolutePath() + "/x303"), 300, 4);
      totalTransWritten += 4;
      totalFilesWritten ++;

      TestUtil.sleep(2000); // to get more then a ms lag time
      addToTrailFile(new File(ggTrailDir.getAbsolutePath() + "/x304"), 400, 4);


      totalTransWritten += 4;
      totalFilesWritten ++;

      TestUtil.sleep(6000); // to guarantee we picked up stats update (stats are updated every 5 seconds)


      // now we should be 2 files behind. parser thread gets paused AFTER it start processing the file
      // so the actuall value will be 1 file behind
      int lagFiles =1; //303(already started being parsed), only 304 is behind
      long lagBytes = 1*4*_transactionPatternSize; // 1 file, 4 transactions each
      /*
      Assert.assertEquals(totalFilesWritten-1, ggParserStats.getNumFilesParsed());
      Assert.assertEquals(totalFilesWritten, ggParserStats.getNumFilesAdded());
      Assert.assertEquals(lagFiles, ggParserStats.getFilesLag()); // because 303 got parsed

      // we added 4 files and parsed 3  , so the diff should be 1 file size (4 trasactions in 1 file)
      Assert.assertEquals(lagBytes, ggParserStats.getBytesLag());
      Assert.assertTrue(ggParserStats.getTimeLag()>0);
*/


      gg.unpause();
      TestUtil.sleep(5000);
      // now we should catchup
      Assert.assertEquals(4, ggParserStats.getNumFilesParsed());
      Assert.assertEquals(4, ggParserStats.getNumFilesAdded());
      Assert.assertEquals(0, ggParserStats.getFilesLag());
      Assert.assertEquals(0, ggParserStats.getTimeLag());
      Assert.assertEquals(0, ggParserStats.getBytesLag());

      // append to a file
      LOG.info("pausing again");
      gg.pause();
      addToTrailFile(new File(ggTrailDir.getAbsolutePath() + "/x304"), 410, 4);
      totalTransWritten += 4;

      TestUtil.sleep(1000);
      addToTrailFile(new File(ggTrailDir.getAbsolutePath() + "/x304"), 420, 4);
      totalTransWritten += 4;


      TestUtil.sleep(2000);
      gg.unpause();

      TestUtil.sleep(5500);
      // should be still up
      Assert.assertEquals(4, ggParserStats.getNumFilesParsed());
      Assert.assertEquals(4, ggParserStats.getNumFilesAdded());
      Assert.assertEquals(0, ggParserStats.getFilesLag());
      Assert.assertEquals(0, ggParserStats.getTimeLag());
      Assert.assertEquals(0, ggParserStats.getBytesLag());

      // assert the stats
      int totalFilesSize = totalTransWritten*_transactionPatternSize;
      Assert.assertEquals((totalFilesSize/totalFilesWritten), ggParserStats.getAvgFileSize());
      Assert.assertEquals(true, ggParserStats.getAvgParseTransactionTimeNs()>0);
      Assert.assertEquals("part1", ggParserStats.getPhysicalSourceName());
      Assert.assertEquals(totalFilesSize/totalTransWritten, ggParserStats.getAvgTransactionSize());
      Assert.assertEquals(423, ggParserStats.getMaxScn());
      Assert.assertEquals(totalTransWritten*2, ggParserStats.getNumTotalEvents()); // 2 events per transaction
      Assert.assertEquals(totalTransWritten, ggParserStats.getNumTransactionsTotal());
      Assert.assertEquals(totalTransWritten, ggParserStats.getNumTransactionsWithEvents());
      Assert.assertEquals(0, ggParserStats.getNumTransactionsWithoutEvents());
      Assert.assertEquals(true, ggParserStats.getTimeSinceLastAccessMs()>0);
      Assert.assertEquals(totalTransWritten*_transactionPatternSize, ggParserStats.getNumBytesTotalParsed());

    } finally {
      gg.shutdown();
    }
    return;
  }

  // testing add trail file
  public void testWriter() throws IOException {
    addToTrailFile(new File("/tmp/bbb"), 20, 3);
    addToTrailFile(new File("/tmp/bbb"), 30, 4);
  }

  private void addToTrailFile(File file, long startingSCN, int numTransactions) throws IOException {
    long lenBefore = file.length();

    BufferedWriter writer = new BufferedWriter( new FileWriter( file , true ) );
    for(int i=0; i<numTransactions; i++) {
      String tr = _transactionPattern.replaceAll(SCNPATTERN, String.valueOf(startingSCN++));
      writer.write(tr);
    }

    writer.close();
    Assert.assertEquals(_transactionPatternSize * numTransactions, file.length() - lenBefore);
  }


  private PhysicalSourceStaticConfig buildSimplePssc(short [] sourceIds, String [] sourceNames, String uri) throws InvalidConfigException
  {
    String partitionFunction = "constant:1";

    String pPartName = "part1";
    short pPartId = 0;

    PhysicalSourceConfig pc = new PhysicalSourceConfig(pPartName, uri, pPartId);
    for(int i=0; i<sourceIds.length; i++) {
      LogicalSourceConfig lc = new LogicalSourceConfig();
      lc.setId(sourceIds[i]);
      lc.setName(sourceNames[i]);
      lc.setPartitionFunction(partitionFunction);
      lc.setUri(pPartName + "." + sourceNames[i]); // this format is expected by GG
      pc.addSource(lc);
    }


    return pc.build();
  }

  @Test
  public void testAddEventToBuffer() throws InvalidConfigException,
      UnsupportedKeyException, DatabusException
  {
    // No rate control
    long rate = 0;
    PhysicalSourceStaticConfig pssc = buildPssc(rate, 0L);
    long scn = 10;
    DbusEventBuffer mb = (DbusEventBuffer)createBufMult(pssc);

    GoldenGateEventProducer gg = new GoldenGateEventProducer(pssc, null, mb,
        null, null);
    List<TransactionState.PerSourceTransactionalUpdate> dbUpdates = new ArrayList<TransactionState.PerSourceTransactionalUpdate>(
        10);
    int sourceId = 505;
    HashSet<DBUpdateImage> db = new HashSet<DBUpdateImage>();

    Object key = new String("name");
    Schema.Type keyType = Schema.Type.RECORD;
    ColumnsState.KeyPair kp = new ColumnsState.KeyPair(key, keyType);
    ArrayList<ColumnsState.KeyPair> keyPairs = new ArrayList<ColumnsState.KeyPair>(
        1);
    keyPairs.add(kp);

    Schema s = Schema.parse(avroSchema);
    GenericRecord gr = new GenericData.Record(s);
    gr.put("name", "phani");

    DBUpdateImage dbi = new DBUpdateImage(keyPairs, scn, gr, s,
        DbUpdateState.DBUpdateImage.OpType.INSERT,false);
    db.add(dbi);
    TransactionState.PerSourceTransactionalUpdate dbUpdate = new TransactionState.PerSourceTransactionalUpdate(
        sourceId, db);
    dbUpdates.add(dbUpdate);

    long timestamp = System.nanoTime();
    gg.addEventToBuffer(dbUpdates, new TransactionInfo(0, 0, timestamp, scn));
    Assert.assertEquals(gg.getRateControl().getNumSleeps(), 0);
    DbusEventIterator iter  = mb.acquireIterator("test");
    int count = 0;
    long eventTs = 0;
    while(iter.hasNext()) {
      DbusEvent e = iter.next();
      if(count==1) { // first event prev control event
        eventTs = e.timestampInNanos();
      }

      count ++;
    }
     Assert.assertEquals("Event timestamp in Ns", timestamp, eventTs);
    Assert.assertEquals("Got events " , 3, count);

    return;
  }

  @Test
  public void testGGProducerStats() throws InvalidConfigException,
  UnsupportedKeyException, DatabusException
  {
    short [] sourceIds = new short [] { 505, 506 };
    String [] sourceNames = new String [] { "source1", "source2"};
    PhysicalSourceStaticConfig pssc = buildSimplePssc(sourceIds, sourceNames,  "gg:///tmp:xxx");
    long scn = 10;

    DbusEventBufferAppendable mb = createBufMult(pssc);

    // start producer
    GoldenGateEventProducer gg = new GoldenGateEventProducer(pssc, null, mb,
                                                             null, null);
    // generates the updates
    List<TransactionState.PerSourceTransactionalUpdate> dbUpdates = generateUpdates(sourceIds[0], scn);
    List<TransactionState.PerSourceTransactionalUpdate> dbUpdates1 = generateUpdates(sourceIds[1], scn+1);

    long timestamp = System.currentTimeMillis() * DbusConstants.NUM_NSECS_IN_MSEC;
    gg.addEventToBuffer(dbUpdates, new TransactionInfo(0, 0, timestamp, scn));
    gg.addEventToBuffer(dbUpdates1, new TransactionInfo(0, 0, timestamp+1, scn+1));

    for(EventSourceStatisticsIface si : gg.getSources()) {
      EventSourceStatistics ss = si.getStatisticsBean();
      LOG.info(si.getSourceName() + ": scn=" + ss.getMaxScn() +
                         ",averageSize=" + ss.getAvgEventSerializedSize()  +
                         ",numErrors="+ ss.getNumErrors() +
                         ",totalEvents=" + ss.getNumTotalEvents() +
                         ",averageFactTime=" + ss.getAvgEventFactoryTimeMillisPerEvent() +
                         ",timeSinceDb=" + ss.getTimeSinceLastDBAccess());

      if(si.getSourceId() == 505) {
        Assert.assertEquals(6, ss.getAvgEventSerializedSize());
        Assert.assertEquals(0, ss.getNumErrors());
        Assert.assertEquals(1, ss.getNumTotalEvents());
        Assert.assertEquals(0, ss.getAvgEventFactoryTimeMillisPerEvent());
        Assert.assertEquals(0, ss.getAvgEventFactoryTimeMillisPerEvent()); // we are not really reading
        Assert.assertEquals(10, ss.getMaxScn());
      }
      if(si.getSourceId() == 506) {
        Assert.assertEquals(6, ss.getAvgEventSerializedSize());
        Assert.assertEquals(0, ss.getNumErrors());
        Assert.assertEquals(1, ss.getNumTotalEvents());
        Assert.assertEquals(0, ss.getAvgEventFactoryTimeMillisPerEvent());
        Assert.assertEquals(0, ss.getAvgEventFactoryTimeMillisPerEvent()); // we are not really reading
        Assert.assertEquals(11, ss.getMaxScn());
      }
      if(si.getSourceId() == GoldenGateEventProducer.GLOBAL_SOURCE_ID) {
        Assert.assertEquals(6, ss.getAvgEventSerializedSize());
        Assert.assertEquals(0, ss.getNumErrors());
        Assert.assertEquals(2, ss.getNumTotalEvents());
        Assert.assertEquals(0, ss.getAvgEventFactoryTimeMillisPerEvent());
        Assert.assertEquals(0, ss.getAvgEventFactoryTimeMillisPerEvent()); // we are not really reading
        Assert.assertEquals(11, ss.getMaxScn());
      }
    }
    long approximateTimeSinceLastTransactionMs = System.currentTimeMillis() - timestamp/DbusConstants.NUM_NSECS_IN_MSEC;
    long diff = gg.getParserStats().getTimeSinceLastTransactionMs() - approximateTimeSinceLastTransactionMs;
    Assert.assertTrue("time diff is too big:" + diff, diff>= 0 && diff <30); //somewhat close and IN MS (not NS)
    return;
  }
  //aux method to generate updates
  private List<TransactionState.PerSourceTransactionalUpdate> generateUpdates(short sourceId, long scn)
      throws DatabusException {
    List<TransactionState.PerSourceTransactionalUpdate> dbUpdates = new ArrayList<TransactionState.PerSourceTransactionalUpdate>(
        10);
    HashSet<DBUpdateImage> db = new HashSet<DBUpdateImage>();

    Object key = new String("name");
    Schema.Type keyType = Schema.Type.RECORD;
    ColumnsState.KeyPair kp = new ColumnsState.KeyPair(key, keyType);
    ArrayList<ColumnsState.KeyPair> keyPairs = new ArrayList<ColumnsState.KeyPair>(
        1);
    keyPairs.add(kp);

    Schema s = Schema.parse(avroSchema);
    GenericRecord gr = new GenericData.Record(s);
    gr.put("name", "phani");

    DBUpdateImage dbi = new DBUpdateImage(keyPairs, scn, gr, s,
                                          DbUpdateState.DBUpdateImage.OpType.INSERT,false);
    db.add(dbi);
    TransactionState.PerSourceTransactionalUpdate dbUpdate = new TransactionState.PerSourceTransactionalUpdate(
                                                                                                               sourceId, db);
    dbUpdates.add(dbUpdate);
    return dbUpdates;
  }

  @Test
  public void testAddEventToBufferRateControl1() throws InvalidConfigException,
  UnsupportedKeyException, DatabusException, NoSuchFieldException, IllegalAccessException
  {
	  // Test with throttle value less than, equal to and greater than number of events ( when rate == 1)
	  // Number of sleeps must be min(numEvents, throttleDurationInSecs
	  testAddEventToBufferRateControl(3);
	  testAddEventToBufferRateControl(5);
	  testAddEventToBufferRateControl(7);
  }

  private void testAddEventToBufferRateControl(long throttleDurationInSecs) throws InvalidConfigException,
      UnsupportedKeyException, DatabusException, NoSuchFieldException, IllegalAccessException
  {

    // 1 event per second required. Send 5 events. Must have 4 sleeps.
    long rate = 1;
    int numEvents = 5;

    PhysicalSourceStaticConfig pssc = buildPssc(rate, throttleDurationInSecs);
    long scn = 10;
    DbusEventBufferAppendable mb = createBufMult(pssc);

    GoldenGateEventProducer gg = new GoldenGateEventProducer(pssc, null, mb,
        null, null);

    // enable if want to run with mocked timer
    // run_with_mock_timer(gg);
    int sourceId = 505;
    HashSet<DBUpdateImage> db = new HashSet<DBUpdateImage>();

    // name1 is the only key
    ColumnsState.KeyPair kp1 = new ColumnsState.KeyPair(new String("name1"), Schema.Type.RECORD);
    ArrayList<ColumnsState.KeyPair> keyPairs = new ArrayList<ColumnsState.KeyPair>(
        numEvents);
    keyPairs.add(kp1);

    Schema s = Schema.parse(avroSchema2);

    GenericRecord gr1 = new GenericData.Record(s);
    gr1.put("name1", "phani1");
    gr1.put("name2", "boris1");

    GenericRecord gr2 = new GenericData.Record(s);
    gr2.put("name1", "phani2");
    gr2.put("name2", "boris2");

    GenericRecord gr3 = new GenericData.Record(s);
    gr3.put("name1", "phani3");
    gr3.put("name2", "boris3");

    GenericRecord gr4 = new GenericData.Record(s);
    gr4.put("name1", "phani4");
    gr4.put("name2", "boris4");

    GenericRecord gr5 = new GenericData.Record(s);
    gr5.put("name1", "phani5");
    gr5.put("name2", "boris5");


    DBUpdateImage dbi1 = new DBUpdateImage(keyPairs, scn, gr1, s,
        DbUpdateState.DBUpdateImage.OpType.INSERT, false);
    DBUpdateImage dbi2 = new DBUpdateImage(keyPairs, scn, gr2, s,
        DbUpdateState.DBUpdateImage.OpType.INSERT, false);
    DBUpdateImage dbi3 = new DBUpdateImage(keyPairs, scn, gr3, s,
        DbUpdateState.DBUpdateImage.OpType.INSERT, false);
    DBUpdateImage dbi4 = new DBUpdateImage(keyPairs, scn, gr4, s,
        DbUpdateState.DBUpdateImage.OpType.INSERT, false);
    DBUpdateImage dbi5 = new DBUpdateImage(keyPairs, scn, gr5, s,
        DbUpdateState.DBUpdateImage.OpType.INSERT, false);

    db.add(dbi1);
    db.add(dbi2);
    db.add(dbi3);
    db.add(dbi4);
    db.add(dbi5);
    // For a given transaction, and logical source : only 1 update ( the last one succeeds )
    Assert.assertEquals(1,db.size());

    // Generate 5 transactions with the same update
    for (int i=0; i< numEvents;i++)
    {
      List<TransactionState.PerSourceTransactionalUpdate> dbUpdates = new ArrayList<TransactionState.PerSourceTransactionalUpdate>(
                10);
      TransactionState.PerSourceTransactionalUpdate dbUpdate = new TransactionState.PerSourceTransactionalUpdate(
          sourceId, db);
      dbUpdates.add(dbUpdate);

      long timestamp = 60;
      gg.addEventToBuffer(dbUpdates, new TransactionInfo(0, 0, timestamp, scn));
      scn++;
    }

    // It may not sleep the very first time as 1 second may have elapsed from when the rate control got started to when event in
    // getting inserted. Subsequently, expect rate control to kick in
    long numSleeps = Math.min(numEvents,throttleDurationInSecs);
    Assert.assertEquals(gg.getRateControl().getNumSleeps(),numSleeps);
    gg.getRateControl().resetNumSleeps();
    return;
  }


  private PhysicalSourceStaticConfig buildPssc(long rate, long throttleDuration) throws InvalidConfigException
  {
    String name = "anet";
    short id = 505, partition = 0;
    String uri = "gg:///mnt/dbext/x1";
    String resourceKey = "test";
    String partitionFunction = "constant:1";
    boolean skipInfinityScn = false;
    String queryHints = null;
    LogicalSourceStaticConfig[] sources = new LogicalSourceStaticConfig[1];
    LogicalSourceStaticConfig lssc = new LogicalSourceStaticConfig(id, name,
        uri, partitionFunction, partition, skipInfinityScn, queryHints,
        queryHints, queryHints);
    sources[0] = lssc;
    String role = "MASTER";
    long slowSourceQueryThreshold = 0L;
    long restartScnOffset = 0L;
    BackoffTimerStaticConfigBuilder bsc = new BackoffTimerStaticConfigBuilder();
    ChunkingType ct = ChunkingType.NO_CHUNKING;
    long txnsPerChunk = 0L;
    long scnChunkSize = 0L;
    long chunkedScnThreshold = 0L;
    long maxScnDelayMs = 0L;
    long eventRatePerSec = rate;
    long eventRateThrottleDuration = throttleDuration;
    int largestEventSizeInBytes = 10240;
    long largestWindowSizeInBytes = 10240;
    DbusEventBuffer.Config cfgBuilder = new DbusEventBuffer.Config();
    cfgBuilder.setMaxSize(10 * 1024 * 1024);
    cfgBuilder.setScnIndexSize(2 * 1024 * 1024);
    cfgBuilder.setAllocationPolicy("MMAPPED_MEMORY");

    DbusEventBuffer.StaticConfig dbusEventBuffer = cfgBuilder.build();

    boolean errorOnMissingFields = true;
    String xmlVersion = "1.0";
    String xmlEncoding = "";
    String fieldName = "";
    String regex = "";
    ReplicationBitSetterStaticConfig replicationBitSetter = new ReplicationBitSetterStaticConfig(
        ReplicationBitSetterStaticConfig.SourceType.NONE, fieldName, regex, MissingValueBehavior.STOP_WITH_ERROR);
    PhysicalSourceStaticConfig pssc = new PhysicalSourceStaticConfig(name, id,
        uri, resourceKey, sources, role, slowSourceQueryThreshold,
        restartScnOffset, bsc.build(), ct, txnsPerChunk, scnChunkSize,
        chunkedScnThreshold, maxScnDelayMs, eventRatePerSec, eventRateThrottleDuration, dbusEventBuffer,
        largestEventSizeInBytes, largestWindowSizeInBytes,
        errorOnMissingFields, xmlVersion, xmlEncoding, replicationBitSetter);
    return pssc;
  }

  /**
   * Creates a DbusBufMult
   */
  private DbusEventBufferAppendable createBufMult(PhysicalSourceStaticConfig pssc) throws InvalidConfigException {
    DbusEventBuffer.StaticConfig config = null;

    if(config == null) {
      try {
        DbusEventBuffer.Config cfgBuilder = new DbusEventBuffer.Config();
        cfgBuilder.setMaxSize(10*1024*1024);
        cfgBuilder.setScnIndexSize(2*1024*1024);
        cfgBuilder.setAllocationPolicy("MMAPPED_MEMORY");
        config = cfgBuilder.build();
      }
      catch (InvalidConfigException e) {
        fail("invalid configuration" ,e);
      }
    }

    PhysicalSourceStaticConfig [] pConfigs = new PhysicalSourceStaticConfig[1];
    pConfigs[0] = pssc;

    DbusEventBufferMult eventBufferMult = new DbusEventBufferMult(pConfigs, config, new DbusEventV2Factory());
    for(DbusEventBuffer b : eventBufferMult.bufIterable()) {
      b.start(1);
    }
    DbusEventBufferAppendable buf = eventBufferMult.getDbusEventBufferAppendable(505);
    return buf;
  }

  /**
   * Creates a mock DbusEventBufMult
   */
  private DbusEventBufferAppendable createMockBufMult()
  {
    long scn = 10;
    DbusEventBufferAppendable deba = EasyMock
        .createMock(DbusEventBufferAppendable.class);
    deba.startEvents();
    EasyMock.expectLastCall().andAnswer(new IAnswer()
    {
      @Override
      public Object answer()
      {
        return null;
      }
    });
    DbusEventKey dek = EasyMock.createNiceMock(DbusEventKey.class);
    EasyMock.expect(
        deba.appendEvent(dek, EasyMock.createNiceMock(DbusEventInfo.class),
            null)).andReturn(Boolean.TRUE);

    deba.endEvents(EasyMock.eq(scn), null);
    EasyMock.expectLastCall().andAnswer(new IAnswer()
    {
      @Override
      public Object answer()
      {
        return null;
      }
    });
    EasyMock.replay(deba);
    return deba;
  }

  private void run_with_mock_timer(GoldenGateEventProducer gg)
  throws NoSuchFieldException,IllegalAccessException
  {
	  RateControl rc = gg.getRateControl();

	  MockRateMonitor mrm = new MockRateMonitor("mock");
	  mrm.setNanoTime(0L);
	  mrm.start();

	  Field field = rc.getClass().getDeclaredField("_ra");
	  field.setAccessible(true);
	  field.set(rc, mrm);
  }

  @Test
  public void testTimeStampFactoryMethod()
      throws DatabusException
  {

    long timeStamp = 1373438353567L;
    //Milliseconds test
    Assert.assertEquals(timeStamp, GGEventGenerationFactory.ggTimeStampStringToMilliSeconds("2013-07-10:06:39:13.567000000"));
    //Nano seconds test
    Assert.assertEquals(timeStamp* DbusConstants.NUM_NSECS_IN_MSEC, GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:39:13.567000000"));


    //Micro seconds as input test
    Assert.assertEquals(timeStamp* DbusConstants.NUM_NSECS_IN_MSEC, GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:39:13.567000"));
    //Milliseconds as input test
    Assert.assertEquals(timeStamp* DbusConstants.NUM_NSECS_IN_MSEC, GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:39:13.567"));
  }

  @Test(expectedExceptions = DatabusException.class)
  public void testTimeStampFactoryMethodInvalidFormat()
      throws DatabusException
  {
    GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:39:13");
  }

  @Test(expectedExceptions = DatabusException.class)
  public void testTimeStampFactoryMethod2()
      throws DatabusException
  {
    long ts1 = GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:39:13");

    long ts2 = GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:39:10");
    Assert.assertEquals(ts1-ts2, 3*DbusConstants.NUM_NSECS_IN_SEC);

    long ts3 = GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:06:36:13");
    Assert.assertEquals(ts1-ts3, 3 * 60 * DbusConstants.NUM_NSECS_IN_SEC);

    long ts4 = GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-10:03:39:13");
    Assert.assertEquals(ts1-ts4, 3 * 60 * 60 * DbusConstants.NUM_NSECS_IN_SEC);

    long ts5 = GGEventGenerationFactory.ggTimeStampStringToNanoSeconds("2013-07-07:06:39:13");
    Assert.assertEquals(ts1-ts5, 3 * 60 * 60 * 24 * DbusConstants.NUM_NSECS_IN_SEC);
  }

}


