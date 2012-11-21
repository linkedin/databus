package com.linkedin.databus.container.request.tcp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.tcp.ContainerTestsBase;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.container.DummyPipelineFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriterStaticConfig;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.VersionedSchemaSetBackedRegistryService;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

/** Unit tests for the execution handler for GetLastSequence commands */
public class TestGetLastSequenceExecHandler extends ContainerTestsBase
{

  @Override
  @BeforeClass
  public void setUp() throws Exception
  {
    //For Espresso compatibility
    DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;

    super.setUp();
  }

  @Test
  /** Tests the command with a single requested sourceid */
  public void testV1SingleSourceNoData() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(11, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(11, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])
    short[] req1SourceIds = {(short)101};
    GetLastSequenceRequest req1 = GetLastSequenceRequest.createV1(req1SourceIds);
    ChannelFuture writeFuture = req1.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 13);
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8);
    long seq = response1.readLong();
    Assert.assertEquals(seq, -1L, "correct sequence: " + seq);

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /**
   * Tests the command with a multiple sources. First two source use the same buffer so their SCNs
   * should be the same
   * */
  public void testV1MultiSourceSomeData() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult(1);
    DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1, PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);

    //generate some data in the even buffer
    eventBuffer.start(1);
    eventBuffer.startEvents();
    eventBuffer.appendEvent(new DbusEventKey(1), (short)0, (short)101, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.appendEvent(new DbusEventKey(1), (short)0, (short)102, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.appendEvent(new DbusEventKey(2), (short)102, (short)103, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.endEvents(123L, null);

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(12, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(12, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101, 102, 103, 104, 105])
    short[] req2SourceIds = {(short)101, (short)102, (short)103, (short)104, (short)105};
    GetLastSequenceRequest req2 = GetLastSequenceRequest.createV1(req2SourceIds);
    ChannelFuture writeFuture = req2.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 * 5, "correct response size: " +
                        response1.readableBytes());
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 * 5);
    for (int i = 0; i < 2; ++i)
    {
      long seq = response1.readLong();
      Assert.assertEquals(seq, 123L, "correct sequence: " + seq);
    }
    for (int i = 2; i < 5; ++i)
    {
      long seq = response1.readLong();
      Assert.assertEquals(seq, -1L, "correct sequence: " + seq);
    }

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /**
   * Tests the command with multiple sourceid. Some of the ids are invalid.
   * */
  public void testV1MultiSourceInvalidSrcId() throws Exception
  {
    DbusEventBufferMult eventBufMult = createOnePhysicalSourceEventBufferMult(1);
    DbusEventBuffer eventBuffer = getBufferForPartition(eventBufMult, 1, PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);

    //generate some data in the even buffer
    eventBuffer.start(1);
    eventBuffer.startEvents();
    eventBuffer.appendEvent(new DbusEventKey(1), (short)0, (short)101, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.appendEvent(new DbusEventKey(1), (short)0, (short)102, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.appendEvent(new DbusEventKey(2), (short)102, (short)103, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.endEvents(123L, null);

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(SchemaRegistryStaticConfig.RegistryType.EXISTING
                                                   .toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);

    relayConfigBuilder.setSourceName("101", "source1");

    relayConfigBuilder.getDataSources().getSequenceNumbersHandler()
                      .setType(MaxSCNReaderWriterStaticConfig.Type.IN_MEMORY.toString());

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(13, 500);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(13, 500);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101, 102, 103, 104, 105])
    short[] req2SourceIds = {(short)101, (short)102, (short)103, (short)104, (short)105};
    GetLastSequenceRequest req2 = GetLastSequenceRequest.createV1(req2SourceIds);
    ChannelFuture writeFuture = req2.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 * 5, "correct response size: " +
                        response1.readableBytes());
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 5 * 8);
    long seq0 = response1.readLong();
    Assert.assertEquals(seq0, 123L);
    long seq1 = response1.readLong();
    Assert.assertEquals(seq1, 123L);
    for (int i = 2; i < 5; ++i)
    {
      long seq = response1.readLong();
      Assert.assertEquals(seq, -1L, "correct sequence for source " + i + ": " + seq);
    }

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /** Tests the command with a single requested sourceid */
  public void testV2() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(SchemaRegistryStaticConfig.RegistryType.EXISTING
                                                   .toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);
    relayConfigBuilder.getSchemaRegistry().getFileSystem().setRefreshPeriodMs(0);


    relayConfigBuilder.setSourceName("101", "source1");
    relayConfigBuilder.setSourceName("102", "source2");
    relayConfigBuilder.setSourceName("103", "source3");
    relayConfigBuilder.setSourceName("104", "source4");
    relayConfigBuilder.setSourceName("105", "source5");

    relayConfigBuilder.getDataSources().getSequenceNumbersHandler()
                      .setType(MaxSCNReaderWriterStaticConfig.Type.IN_MEMORY.toString());

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(1), 1234L);

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(14, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(14, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequenceV2(10)
    GetLastSequenceRequest req1 = GetLastSequenceRequest.createV2(1);
    ChannelFuture writeFuture = req1.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8);
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, 1234L);

    srvConn.stop();
    clientConn.stop();
  }

}
