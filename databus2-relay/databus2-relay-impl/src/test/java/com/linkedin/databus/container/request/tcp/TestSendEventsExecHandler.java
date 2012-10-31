package com.linkedin.databus.container.request.tcp;

import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.UnknownPartitionException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.DummyPipelineFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestSendEventsExecHandler extends ContainerTestsBase
{
  interface MockRelay
  {
    Map<String, Short> getSourceSchemaVersionMap();
  };
  SchemaRegistryService _schemaRegistry;
  Map<Short, String> _allVersionSchemas1;
  Map<String, Short> _versionMap = new HashMap<String, Short>();
  volatile long _now, _realNow;
  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Override
  @BeforeClass
  public void setUp() throws Exception
  {
    DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;

    _allVersionSchemas1 = new HashMap<Short, String>();
    _allVersionSchemas1.put((short)1, "");
    super.setUp();

    _schemaRegistry = EasyMock.createMock(SchemaRegistryService.class);
    
    DatabusException de = new DatabusException("");
    String md5 = "1234567890123456";
    SchemaId schemaId = new SchemaId(md5.getBytes());
    EasyMock.expect(_schemaRegistry.fetchSchemaIdForSourceNameAndVersion((String)EasyMock.anyObject(),EasyMock.eq(1))).andReturn(schemaId).anyTimes();
    EasyMock.expect(_schemaRegistry.fetchSchemaIdForSourceNameAndVersion((String)EasyMock.anyObject(),EasyMock.eq(2))).andThrow(de).times(5);
    EasyMock.expect(_schemaRegistry.fetchSchemaIdForSourceNameAndVersion((String)EasyMock.anyObject(),EasyMock.eq(2))).andReturn(schemaId).anyTimes();
    EasyMock.expect(_schemaRegistry.fetchSchemaIdForSourceNameAndVersion((String)EasyMock.anyObject(),EasyMock.anyInt())).andReturn(schemaId).anyTimes();
    EasyMock.expect(_schemaRegistry.fetchAllSchemaVersionsByType((String)EasyMock.anyObject())).andReturn(_allVersionSchemas1).anyTimes();
    EasyMock.replay(_schemaRegistry);

  }

  @Test
  public void testSendSingleEventHappyPath() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult(1);

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(1), 666L);

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 null, null,null,_schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(101, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(101, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])
    StartSendEventsRequest req1 = StartSendEventsRequest.createV1(1);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());


    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(2, PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    srcBuffer.start(1);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 2, (short)1, System.currentTimeMillis() * 1000000,
                          (short)101, new byte[16], new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 2, (short)1, System.currentTimeMillis() * 1000000,
                         (short)102, new byte[16], new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 2, (short)1, System.currentTimeMillis() * 1000000,
                         (short)103, new byte[16], new byte[100], false, null);
    srcBuffer.endEvents(123L, null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //send first event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    DbusEvent evt = eventIter.next();

    int evtSize = evt.size();
    SendEventsRequest sendEvents1 =
        SendEventsRequest.createV1(667, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    int eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1, "correct events red num:" + maxTransSize);

    //compare events
    DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 2, PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);
    final DbusEventIterator resIter = eventBuffer.acquireIterator("resIterator");
    Assert.assertTrue(resIter.hasNext(), "has result event");
    DbusEvent resEvt = resIter.next();

    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), evt.srcId());

    //send the second event
    respAggregator.clear();

    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    evt = eventIter.next();

    evtSize = evt.size();
    SendEventsRequest sendEvents2 =
        SendEventsRequest.createV1(668, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send2WriteFuture = sendEvents2.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send2WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1, "correct events red num:" + maxTransSize);

    //compare events
    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            resIter.awaitUninterruptibly();
            return true;
          }
        }, "has more events", 1000, null);

    Assert.assertTrue(resIter.hasNext(), "has result event");
    resEvt = resIter.next();

    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), evt.srcId());

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  public void testSendSingleEventHappyPathV3() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null,null, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(102, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(102, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newSrc = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newSrc.getId(), newSrc);
    }

    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());


    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(1,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    srcBuffer.start(1);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)2, new byte[16], new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)3, new byte[16], new byte[100], false, null);
    srcBuffer.endEvents(123L, null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //send first event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    DbusEvent evt = eventIter.next();

    int evtSize = evt.size();
    SendEventsRequest sendEvents1 =
        SendEventsRequest.createV3(667, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    int eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1);

    //compare events
    DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1, "psource");
    final DbusEventIterator resIter = eventBuffer.acquireIterator("resIterator");

    Assert.assertTrue(resIter.hasNext(), "has result event");
    DbusEvent resEvt = resIter.next();
    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)101);

    //send the second event
    respAggregator.clear();

    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    evt = eventIter.next();

    evtSize = evt.size();
    SendEventsRequest sendEvents2 =
        SendEventsRequest.createV3(668, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send2WriteFuture = sendEvents2.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send2WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1, "correct events red num:" + maxTransSize);

    //compare events
    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            resIter.awaitUninterruptibly();
            return true;
          }
        }, "has more events", 1000, null);

    Assert.assertTrue(resIter.hasNext(), "has result event");
    resEvt = resIter.next();

    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)102);

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  public void testSendSingleEventV3NoSources() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null,null, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(103, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(103, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();

    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 10);
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, BinaryProtocol.RESULT_ERR_INVALID_REQ_PARAM);

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  public void testMultiBufferEvent() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg =
        SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null,null, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(1040, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(1040, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(100);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newPair = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newPair.getId(), newPair);
    }

    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());


    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(1, "name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    srcBuffer.start(1);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1,
                          System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[510], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1,
                          System.currentTimeMillis() * 1000000,
                         (short)2, new byte[16], new byte[50], false, null);
    srcBuffer.endEvents(123L,  null);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(10), (short) 10, (short)10,
                          System.currentTimeMillis() * 1000000,
                         (short)2, new byte[16], new byte[50], false, null);
    srcBuffer.endEvents(321L,  null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //send first event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    DbusEvent evt = eventIter.next();

    int evtSize = evt.size();

    //write bytes [0-100)
    ByteBuffer b = evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER).slice();
    b.position(0).limit(100);
    ByteBuffer part1 = b.slice().order(BinaryProtocol.BYTE_ORDER);

//    if (true) {
//      ChannelBuffer buf = ChannelBuffers.wrappedBuffer(b);
//      System.err.println("event size: " + evtSize + " buffer size: " + buf.readableBytes());
//      System.err.println(ChannelBuffers.hexDump(buf));
//    }
    SendEventsRequest sendEvents1 = SendEventsRequest.createV3(667, part1);

    final ChannelFuture send1WriteFuture_1 = sendEvents1.writeToChannelAsBinary(clientChannel);
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture_1.isDone();}
    }, "write completed", 1000, null);

    //write bytes [100-200)
    b.position(100).limit(200);
    ByteBuffer part2 = b.slice().order(BinaryProtocol.BYTE_ORDER);

    final ChannelFuture send1WriteFuture_2 = clientChannel.write(ChannelBuffers.wrappedBuffer(part2));
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture_2.isDone();}
    }, "write completed", 1000, null);

    //write the rest of the bytes
    b.position(200).limit(b.capacity());
    ByteBuffer part3 = b.slice().order(BinaryProtocol.BYTE_ORDER);

    final ChannelFuture send1WriteFuture_3 = clientChannel.write(ChannelBuffers.wrappedBuffer(part3));
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture_3.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    int eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1);

    //compare events
    DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1, "psource");
    final DbusEventIterator resIter = eventBuffer.acquireIterator("resIterator");

    Assert.assertTrue(resIter.hasNext(), "has result event");
    DbusEvent resEvt = resIter.next();
    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)101);

    //send the second event
    respAggregator.clear();

    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    evt = eventIter.next();

    evtSize = evt.size();
    SendEventsRequest sendEvents2 =
        SendEventsRequest.createV3(668, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send2WriteFuture = sendEvents2.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send2WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1, "correct events red num:" + maxTransSize);

    //compare events
    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            resIter.awaitUninterruptibly();
            return true;
          }
        }, "has more events", 1000, null);

    Assert.assertTrue(resIter.hasNext(), "has result event");
    resEvt = resIter.next();

    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)102);

    //send the third event for an invalid partition
    respAggregator.clear();

    PhysicalPartition pPartition10 = new PhysicalPartition(10, "name");
    DbusEventBuffer srcBuffer10 = new DbusEventBuffer(_defaultBufferConf, pPartition10);

    srcBuffer10.start(1);
    srcBuffer10.startEvents();
    srcBuffer10.appendEvent(new DbusEventKey(10), (short) 10, (short)10,
                          System.currentTimeMillis() * 1000000,
                         (short)2, new byte[16], new byte[50], false, null);
    srcBuffer10.endEvents(321L,  null);

    eventIter = srcBuffer10.acquireIterator("srcIterator");

    //skip over eow
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    evt = eventIter.next();
    Assert.assertTrue(evt.isEndOfPeriodMarker());

    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    evt = eventIter.next();

    Assert.assertEquals(evt.sequence(), 321L);
    Assert.assertEquals(evt.physicalPartitionId(), (short)10);

    evtSize = evt.size();
    SendEventsRequest sendEvents3 =
        SendEventsRequest.createV3(668, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send3WriteFuture = sendEvents3.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send3WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 1);
    Assert.assertEquals(response1.getByte(0), BinaryProtocol.RESULT_ERR_UNKNOWN_PARTITION);
    //validate error response
    int lengthRest = response1.getInt(1);
    Assert.assertEquals(lengthRest, response1.readableBytes() - 5);
    short classNameLen = response1.getShort(5);
    Assert.assertTrue(classNameLen > 0);
    byte[] classNameBytes = new byte[classNameLen];
    response1.getBytes(5 + 2, classNameBytes);
    String className = new String(classNameBytes);
    Assert.assertEquals(className, UnknownPartitionException.class.getName());
    short msgLen = response1.getShort(5 + 2 + classNameLen);
    Assert.assertTrue(msgLen > 0);
    Assert.assertEquals(5 + 2 + classNameLen + 2 + msgLen, response1.readableBytes());
    byte[] msgBytes = new byte[msgLen];
    response1.getBytes(5 + 2 + classNameLen + 2, msgBytes);
    String msg = new String(msgBytes);

    //resend same event
    respAggregator.clear();
    sendEvents3 =
        SendEventsRequest.createV3(668, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send3WriteFuture2 = sendEvents3.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send3WriteFuture2.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 1);
    Assert.assertEquals(response1.getByte(0), BinaryProtocol.RESULT_ERR_UNKNOWN_PARTITION);
    //validate error response
    lengthRest = response1.getInt(1);
    Assert.assertEquals(lengthRest, response1.readableBytes() - 5);
    classNameLen = response1.getShort(5);
    Assert.assertTrue(classNameLen > 0);
    classNameBytes = new byte[classNameLen];
    response1.getBytes(5 + 2, classNameBytes);
    className = new String(classNameBytes);
    Assert.assertEquals(className, UnknownPartitionException.class.getName());
    msgLen = response1.getShort(5 + 2 + classNameLen);
    Assert.assertTrue(msgLen > 0);
    Assert.assertEquals(5 + 2 + classNameLen + 2 + msgLen, response1.readableBytes());
    msgBytes = new byte[msgLen];
    response1.getBytes(5 + 2 + classNameLen + 2, msgBytes);
    msg = new String(msgBytes);


    //clean up
    srvConn.stop();
    clientConn.stop();
  }

  @Test
  public void testMultiBufferEventUnknownPartition() throws Exception
  {
    //initialization
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg =
        SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null,null, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(1040, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(1040, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(100);} catch (InterruptedException ie){};

    //Send Handshake

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newPair = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newPair.getId(), newPair);
    }

    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());


    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(10, "psource");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    srcBuffer.start(1);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 10, (short)10,
                          System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[510], false, null);
    srcBuffer.appendEvent(new DbusEventKey(2), (short) 10, (short)10,
                          System.currentTimeMillis() * 1000000,
                         (short)2, new byte[16], new byte[50], false, null);
    srcBuffer.endEvents(123L,  null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //send first event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    DbusEvent evt = eventIter.next();

    //write bytes [0-100)
    ByteBuffer b = evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER).slice();
    b.position(0).limit(100);
    ByteBuffer part1 = b.slice().order(BinaryProtocol.BYTE_ORDER);

    SendEventsRequest sendEvents1 = SendEventsRequest.createV3(667, part1);

    final ChannelFuture send1WriteFuture_1 = sendEvents1.writeToChannelAsBinary(clientChannel);
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture_1.isDone();}
    }, "write completed", 1000, null);

    //write bytes [100-200)
    b.position(100).limit(200);
    ByteBuffer part2 = b.slice().order(BinaryProtocol.BYTE_ORDER);

    final ChannelFuture send1WriteFuture_2 = clientChannel.write(ChannelBuffers.wrappedBuffer(part2));
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture_2.isDone();}
    }, "write completed", 1000, null);

    //write the rest of the bytes
    b.position(200).limit(b.capacity());
    ByteBuffer part3 = b.slice().order(BinaryProtocol.BYTE_ORDER);

    final ChannelFuture send1WriteFuture_3 = clientChannel.write(ChannelBuffers.wrappedBuffer(part3));
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture_3.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    int responseLen = response1.readableBytes();
    Assert.assertTrue(response1.readableBytes() > 1);
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, BinaryProtocol.RESULT_ERR_UNKNOWN_PARTITION);
    int respLength = response1.readInt();
    Assert.assertEquals(1 + 4 + respLength, responseLen);
    short errClassLen = response1.readShort();
    Assert.assertEquals(errClassLen, UnknownPartitionException.class.getName().length());
    byte[] errClassBytes = new byte[errClassLen];
    response1.readBytes(errClassBytes);
    String errClassName = new String(errClassBytes);
    Assert.assertEquals(errClassName, UnknownPartitionException.class.getName());
    short messageLen = response1.readShort();
    Assert.assertEquals(2 + errClassLen + 2 + messageLen, respLength);
    byte[] messageBytes = new byte[messageLen];
    response1.readBytes(messageBytes);
    String message = new String(messageBytes);
    UnknownPartitionException upe = new UnknownPartitionException(pPartition);
    Assert.assertEquals(message, upe.getMessage());

    //clean up
    srvConn.stop();
    clientConn.stop();
  }

  @Test
  public void testGetStats() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    relayConfigBuilder.getEventBuffer().setMaxSize(1000000);

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    DbusEventsStatisticsCollector eventStats = new AggregatedDbusEventsStatisticsCollector(
    		relayConfig.getContainer().getId(), "test", true, false,
    		null);

    StatsCollectors<DbusEventsStatisticsCollector> eventStatsColls = new StatsCollectors<DbusEventsStatisticsCollector>(eventStats);
    StatsCollectors<DbusEventsStatisticsCollector> eventStatsColls2 = new StatsCollectors<DbusEventsStatisticsCollector>(null);



    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, eventStatsColls, eventStatsColls2, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(105, 1000);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(105, 1000);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newSrc = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newSrc.getId(), newSrc);
    }

    // create an event and write it to the channel
    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());

    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(1,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    srcBuffer.start(1);
    int numWins = 100;
    for (int i = 1; i <= numWins; ++i)
    {
      srcBuffer.startEvents();
      srcBuffer.appendEvent(new DbusEventKey(i), (short)1, (short)i,
                            System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[5], false, null);
      srcBuffer.appendEvent(new DbusEventKey(i + 1), (short)1, (short)(i + 1),
                            System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[5], false, null);
      srcBuffer.endEvents(100L * i,  null);
    }

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //compare events
    //DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1);

    //send many event
    for (int i = 1; i <= 3 * numWins; ++i)
    {
      Assert.assertTrue(eventIter.hasNext(), "iterator has event");
      DbusEvent evt = eventIter.next();
      pushOneEvent(clientChannel, evt, respAggregator);
    }

    Assert.assertTrue(!eventIter.hasNext());

    Assert.assertEquals(eventStatsColls.getStatsCollector().getTotalStats().getNumDataEvents(), 2 * numWins);
    Assert.assertEquals(eventStatsColls.getStatsCollector().getTotalStats().getNumSysEvents(), numWins);

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  public void testGetPPartStats() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    relayConfigBuilder.getEventBuffer().setMaxSize(1000000);

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    DbusEventsStatisticsCollector eventStats =
        new AggregatedDbusEventsStatisticsCollector(relayConfig.getContainer().getId(), "test", true, false,
                                          null);

    StatsCollectors<DbusEventsStatisticsCollector> eventStatsColl = new StatsCollectors<DbusEventsStatisticsCollector>(eventStats);

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, eventStatsColl, null, _schemaRegistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(1060, 1000);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(1060, 1000);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newSrc = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newSrc.getId(), newSrc);
    }

    // create an event and write it to the channel
    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());

    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition1 = new PhysicalPartition(1, "psource");
    DbusEventBuffer srcBuffer1 = new DbusEventBuffer(_defaultBufferConf, pPartition1);

    PhysicalPartition pPartition2 = new PhysicalPartition(2, "psource");
    DbusEventBuffer srcBuffer2 = new DbusEventBuffer(_defaultBufferConf, pPartition2);

    int winNum = 2 * 100;
    srcBuffer1.start(1);
    srcBuffer2.start(1);
    for (int i = 1; i <= winNum; ++i)
    {
      srcBuffer1.startEvents();
      srcBuffer1.appendEvent(new DbusEventKey(i), (short)1, (short)1,
                            System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[5], false, null);
      srcBuffer1.endEvents(100L * i,  null);

      srcBuffer2.startEvents();
      srcBuffer2.appendEvent(new DbusEventKey(i), (short)2, (short)2,
                             System.currentTimeMillis() * 1000000,
                             (short)2, new byte[16], new byte[5], false, null);
      srcBuffer2.appendEvent(new DbusEventKey(i + 1), (short)2, (short)2,
                            System.currentTimeMillis() * 1000000,
                            (short)3, new byte[16], new byte[5], false, null);
      srcBuffer2.endEvents(100L * i + 1,  null);
    }

    DbusEventIterator eventIter1 = srcBuffer1.acquireIterator("srcIterator1");
    DbusEventIterator eventIter2 = srcBuffer2.acquireIterator("srcIterator2");

    //skip over the first initialization event
    Assert.assertTrue(eventIter1.hasNext(), "iterator has event");
    eventIter1.next();
    Assert.assertTrue(eventIter2.hasNext(), "iterator has event");
    eventIter2.next();

    //compare events
    //DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1);

    //send many events
    for (int i = 1; i <= winNum; ++i)
    {
      //data event
      Assert.assertTrue(eventIter1.hasNext(), "iterator has event");
      DbusEvent evt = eventIter1.next();
      pushOneEvent(clientChannel, evt, respAggregator);

      //end-of-window
      Assert.assertTrue(eventIter1.hasNext(), "iterator has event");
      DbusEvent eow1 = eventIter1.next();
      pushOneEvent(clientChannel, eow1, respAggregator);

      //data event
      Assert.assertTrue(eventIter2.hasNext(), "iterator has event");
      DbusEvent evt2 = eventIter2.next();
      pushOneEvent(clientChannel, evt2, respAggregator);

      //data event
      Assert.assertTrue(eventIter2.hasNext(), "iterator has event");
      DbusEvent evt3 = eventIter2.next();
      pushOneEvent(clientChannel, evt3, respAggregator);

      //end-of-window
      Assert.assertTrue(eventIter2.hasNext(), "iterator has event");
      DbusEvent eow2 = eventIter2.next();
      pushOneEvent(clientChannel, eow2, respAggregator);
    }

    Assert.assertTrue(!eventIter1.hasNext());
    Assert.assertTrue(!eventIter2.hasNext());

    Assert.assertEquals(eventStatsColl.getStatsCollector().getTotalStats().getNumDataEvents(), 3 * winNum);
    Assert.assertEquals(eventStatsColl.getStatsCollector().getTotalStats().getNumSysEvents(), 2 * winNum);

    HashSet<String> ppartNames = new HashSet<String>(Arrays.asList(pPartition1.toSimpleString(),
                                                                   pPartition2.toSimpleString()));
    for (String ppartName: eventStatsColl.getStatsCollectorKeys())
    {
      Assert.assertTrue(ppartNames.contains(ppartName), "check physical partition name:" + ppartName);
    }

    DbusEventsTotalStats p1Stats = eventStatsColl.getStatsCollector(pPartition1.toSimpleString()).getTotalStats();
    Assert.assertEquals(p1Stats.getNumDataEvents(), winNum);
    Assert.assertEquals(p1Stats.getNumSysEvents(), winNum);

    DbusEventsTotalStats p2Stats = eventStatsColl.getStatsCollector(pPartition2.toSimpleString()).getTotalStats();
    Assert.assertEquals(p2Stats.getNumDataEvents(), winNum * 2);
    Assert.assertEquals(p2Stats.getNumSysEvents(), winNum);

    srvConn.stop();
    clientConn.stop();
  }

  private void pushOneEvent(Channel clientChannel, DbusEvent evt,
                            DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator)
  {
    SendEventsRequest sendEvents1 =
        SendEventsRequest.createV3(667, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture.isDone();}
    }, "write completed", 1000, null);
    Assert.assertTrue(send1WriteFuture.isSuccess());

    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    int eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1);
  }


  @Test
  public void testSchemaVersionChange() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    // Create the relay which contains the schemaversion map
    HttpRelay relay = new HttpRelay(relayConfig, null);
    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(10), 666L);

    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(eventBufMult, relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null, null, _schemaRegistry, relay);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(102, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(102, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: relayConfig.getSourceIds())
    {
      LogicalSource newSrc = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newSrc.getId(), newSrc);
      relay.getSourceSchemaVersionMap().put(pair.getName(), (short)1);
    }

    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(10, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 666L);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());


    //send SendEvents command
    respAggregator.clear();

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(1,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);


    // schema registry always return 1 for now
    //EasyMock.replay(_schemaRegistry);


    srcBuffer.start(1);
    srcBuffer.startEvents();
    byte[] schemaId1 = new byte[16];
    byte[] schemaId2 = new byte[16];

    // schemaversion is stored on the first 2 bytes of the schemaId
    schemaId1[0] = 1;
    schemaId1[1] = 0;
    schemaId2[0] = 2;
    schemaId2[1] = 0;
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)1, schemaId1, new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)2, schemaId2, new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)3, schemaId2, new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)4, schemaId2, new byte[100], false, null);
    srcBuffer.appendEvent(new DbusEventKey(1), (short) 1, (short)1, System.currentTimeMillis() * 1000000,
                         (short)5, schemaId2, new byte[100], false, null);
    srcBuffer.endEvents(123L, null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //send first event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    DbusEvent evt = eventIter.next();

    int evtSize = evt.size();
    SendEventsRequest sendEvents1 =
        SendEventsRequest.createV3(667, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return send1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    int eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1);

    //compare events
    DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1, "psource");
    final DbusEventIterator resIter = eventBuffer.acquireIterator("resIterator");

    Assert.assertTrue(resIter.hasNext(), "has result event");
    DbusEvent resEvt = resIter.next();
    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)101);

    //send the second event with version 2
    respAggregator.clear();

    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    evt = eventIter.next();

    evtSize = evt.size();
    _now = new Date().getTime() + evtSize;

    SendEventsRequest sendEvents2 =
        SendEventsRequest.createV3(668, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

    // schema registry return 2 now
    class UpdateSchemaTask extends TimerTask
    {
      @Override
      public void run()
      {
        _allVersionSchemas1.put((short)2, "");
      }
    }

    new Timer().schedule(new UpdateSchemaTask(), 500);
    final ChannelFuture send2WriteFuture = sendEvents2.writeToChannelAsBinary(clientChannel);

    _realNow = new Date().getTime() + evtSize;
    Assert.assertTrue(_realNow - _now >= 500, "realNow="+_realNow + "; now=" + _now);
    Assert.assertTrue(send2WriteFuture.isDone());

    response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1+4+4, "correct response size");
    resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 4);
    eventsNum = response1.readInt();
    Assert.assertEquals(eventsNum, 1, "correct events red num:" + maxTransSize);

    //compare events
    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            resIter.awaitUninterruptibly();
            return true;
          }
        }, "has more events", 1000, null);

    Assert.assertTrue(resIter.hasNext(), "has result event");
    resEvt = resIter.next();

    Assert.assertTrue(resEvt.isValid(), "valid event");
    Assert.assertEquals(resEvt.size(), evtSize);
    Assert.assertEquals(resEvt.sequence(), evt.sequence());
    Assert.assertEquals(resEvt.key(), evt.key());
    Assert.assertEquals(resEvt.srcId(), (short)102);

    srvConn.stop();
    clientConn.stop();
  }

}
