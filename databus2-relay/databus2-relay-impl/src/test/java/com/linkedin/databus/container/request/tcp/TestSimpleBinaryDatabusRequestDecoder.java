package com.linkedin.databus.container.request.tcp;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.request.tcp.ContainerTestsBase;
import com.linkedin.databus.container.request.tcp.SendEventsRequest;
import com.linkedin.databus.container.request.tcp.StartSendEventsRequest;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.container.DummyPipelineFactory;
import com.linkedin.databus2.core.container.DummyPipelineFactory.SimpleObjectCaptureHandler;
import com.linkedin.databus2.core.container.DummyPipelineFactory.SimpleResponseBytesAggregatorHandler;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.container.request.UnknownCommandException;
import com.linkedin.databus2.core.container.request.UnsupportedProtocolVersionException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriterStaticConfig;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.VersionedSchemaSetBackedRegistryService;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestSimpleBinaryDatabusRequestDecoder extends ContainerTestsBase
{
  public static final String RESPONSE_AGGREGATOR_NAME = "response aggregator";
  public static final String COMMAND_CAPTURE_NAME = "command capture";

  private static final int READ_TIMEOUT_MS = 500;

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

  @Override
  @BeforeClass
  public void setUp() throws Exception
  {
    DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;

    super.setUp();
  }

  @Test
  /** Send simple GetLastSequence commands and check if they have been parsed correctly */
  public void testDecodeGetLastSequenceV1HappyPath() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(
        SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());
    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);

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

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(1, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(1, 100);
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

    Channel connChannel = srvConn.getLastConnChannel();
    ChannelPipeline connPipeline = connChannel.getPipeline();
    SimpleObjectCaptureHandler cmdCapture = (SimpleObjectCaptureHandler)connPipeline.get(COMMAND_CAPTURE_NAME);

    Assert.assertTrue(cmdCapture != null, "has command capture");

    List<Object> cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 1, "single command");

    Assert.assertTrue(cmds.get(0) instanceof GetLastSequenceRequest, "found GetLastSequenceRequest");
    GetLastSequenceRequest getLastCmd = (GetLastSequenceRequest)cmds.get(0);

    Assert.assertEquals(getLastCmd.getProtocolVersion(), (byte)1, "correct protocol version");
    short[] srcIds = getLastCmd.getSourceIds();
    Assert.assertEquals(srcIds.length, 1, "one requested source");
    Assert.assertEquals(srcIds[0], 101, "one requested source");

    cmdCapture.clear();

    //Send GetLastSequence([101, 102, 103, 104, 105])
    short[] req2SourceIds = {(short)101, (short)102, (short)103, (short)104, (short)105};
    GetLastSequenceRequest req2 = GetLastSequenceRequest.createV1(req2SourceIds);
    writeFuture = req2.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 1, "one command");

    Assert.assertTrue(cmds.get(0) instanceof GetLastSequenceRequest,
                      "found GetLastSequenceRequest for ");
    GetLastSequenceRequest cmd = (GetLastSequenceRequest)cmds.get(0);

    Assert.assertEquals(cmd.getProtocolVersion(), (byte)1, "correct protocol version");
    short[] ids = cmd.getSourceIds();
    Assert.assertEquals(ids.length, 5, "five requested source");
    Assert.assertEquals(ids[0], 101, "source 0");
    Assert.assertEquals(ids[1], 102, "source 1");
    Assert.assertEquals(ids[2], 103, "source 2");
    Assert.assertEquals(ids[3], 104, "source 3");
    Assert.assertEquals(ids[4], 105, "source 4");

    cmdCapture.clear();

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /** Send simple GetLastSequence commands and check if they have been parsed correctly */
  public void testDecodeGetLastSequenceV2HappyPath() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(
        SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);

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

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(2, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(2, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])
    GetLastSequenceRequest req1 = GetLastSequenceRequest.createV2(10);
    ChannelFuture writeFuture = req1.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    Channel connChannel = srvConn.getLastConnChannel();
    ChannelPipeline connPipeline = connChannel.getPipeline();
    SimpleObjectCaptureHandler cmdCapture = (SimpleObjectCaptureHandler)connPipeline.get(COMMAND_CAPTURE_NAME);

    Assert.assertTrue(cmdCapture != null, "has command capture");

    List<Object> cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 1, "single command");

    Assert.assertTrue(cmds.get(0) instanceof GetLastSequenceRequest, "found GetLastSequenceRequest");
    GetLastSequenceRequest getLastCmd = (GetLastSequenceRequest)cmds.get(0);

    Assert.assertEquals(getLastCmd.getProtocolVersion(), (byte)2);
    Assert.assertEquals(getLastCmd.getServerId(), 10);

    cmdCapture.clear();

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /** Send invalid commands and expect and error returned by the server */
  public void testDecodeInvalidOp() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(
        SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);
    relayConfigBuilder.getSchemaRegistry().getFileSystem().setRefreshPeriodMs(0);
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
    boolean serverStarted = srvConn.startSynchronously(3, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(3, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    byte[] request1Bytes = {(byte)0xFF, (byte)1, (byte)1, (byte)0,
        (byte)101, (byte)0};
    ChannelBuffer request1 = ChannelBuffers.wrappedBuffer(BinaryProtocol.BYTE_ORDER,
                                                          request1Bytes);

    ChannelFuture writeFuture = clientChannel.write(request1);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    Channel connChannel = srvConn.getLastConnChannel();
    ChannelPipeline connPipeline = connChannel.getPipeline();
    SimpleObjectCaptureHandler cmdCapture = (SimpleObjectCaptureHandler)connPipeline.get(COMMAND_CAPTURE_NAME);

    Assert.assertTrue(cmdCapture != null, "has command capture");

    List<Object> cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 0, "no commands: " + cmds.size());

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    SimpleResponseBytesAggregatorHandler respAggregator =
        (SimpleResponseBytesAggregatorHandler)clientPipe.get(RESPONSE_AGGREGATOR_NAME);

    //verify that we get error response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 7, "enough bytes in response");
    Assert.assertEquals(response1.readByte(), BinaryProtocol.RESULT_ERR_UNKNOWN_COMMAND);
    int responseLen = response1.readInt();
    Assert.assertTrue(responseLen >=4, "response length OK");
    short classLen = response1.readShort();
    Assert.assertTrue(classLen >= 0 && classLen <= 100,
                      "exception class name length in [0, 100]:" + classLen);
    Assert.assertTrue(response1.readableBytes() > classLen,
                      "enough bytes for the exception class name in the response");
    byte[] errorClassName = new byte[classLen];
    response1.readBytes(errorClassName);
    String errorClassStr = new String(errorClassName);
    Assert.assertEquals(errorClassStr, UnknownCommandException.class.getName());
    Assert.assertTrue(response1.readableBytes() >= 2, "error message returned");
    short errorMsgLen = response1.readShort();
    Assert.assertTrue(errorMsgLen >= 0 && errorMsgLen <= 1000,
                      "error message length in [0, 1000]:" + errorMsgLen);
    Assert.assertTrue(response1.readableBytes() >= errorMsgLen,
    "enough bytes for the error message in the response");

    cmdCapture.clear();
    respAggregator.clear();

    //check if the next command is accepted fine

    //Send GetLastSequence([101])
    short[] req1SourceIds = {(short)101};
    GetLastSequenceRequest req1 = GetLastSequenceRequest.createV1(req1SourceIds);
    writeFuture = req1.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    Assert.assertTrue(cmdCapture != null, "has command capture");

    cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 1, "single command");

    Assert.assertTrue(cmds.get(0) instanceof GetLastSequenceRequest, "found GetLastSequenceRequest");
    GetLastSequenceRequest getLastCmd = (GetLastSequenceRequest)cmds.get(0);

    Assert.assertEquals(getLastCmd.getProtocolVersion(), (byte)1, "correct protocol version");
    short[] srcIds = getLastCmd.getSourceIds();
    Assert.assertEquals(srcIds.length, 1, "one requested source");
    Assert.assertEquals(srcIds[0], 101, "one requested source");

    cmdCapture.clear();
    respAggregator.clear();

    //Now send an invalid command again

    byte[] request3Bytes = {(byte)0x10, (byte)0xEF, (byte)0xBE, (byte)0xAD,
        (byte)0xDE};
    ChannelBuffer request3 = ChannelBuffers.wrappedBuffer(BinaryProtocol.BYTE_ORDER,
                                                          request3Bytes);

    writeFuture = clientChannel.write(request3);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    Assert.assertTrue(cmdCapture != null, "has command capture");

    cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 0, "no commands: " + cmds.size());

    //check the response

    //verify that we get error response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response2 = respAggregator.getBuffer();
    Assert.assertTrue(response2.readableBytes() > 7, "enough bytes in response");
    Assert.assertEquals(response2.readByte(), BinaryProtocol.RESULT_ERR_UNKNOWN_COMMAND);
    responseLen = response1.readInt();
    Assert.assertTrue(responseLen >= 4, "response length OK");
    classLen = response2.readShort();
    Assert.assertTrue(classLen >= 0 && classLen <= 100,
                      "exception class name length in [0, 100]:" + classLen);
    Assert.assertTrue(response2.readableBytes() > classLen,
                      "enough bytes for the exception class name in the response");
    errorClassName = new byte[classLen];
    response2.readBytes(errorClassName);
    Assert.assertEquals(new String(errorClassName), UnknownCommandException.class.getName());
    Assert.assertTrue(response2.readableBytes() >= 2, "error message returned");
    errorMsgLen = response2.readShort();
    Assert.assertTrue(errorMsgLen >= 0 && errorMsgLen <= 1000,
                      "error message length in [0, 1000]:" + errorMsgLen);
    Assert.assertTrue(response2.readableBytes() >= errorMsgLen,
    "enough bytes for the error message in the response");

    cmdCapture.clear();
    respAggregator.clear();

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /** Send simle GetLastSequence commands and check if they have been parsed correctly */
  public void testDecodeInvalidProtocolVersion() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(
        SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);

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

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand("getLastSequences", GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(4, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(4, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])
    short[] req1SourceIds = {(short)101};
    GetLastSequenceRequest req1 = new GetLastSequenceRequest((byte)15, req1SourceIds, -1);
    ChannelFuture writeFuture = req1.writeToChannelAsBinary(clientChannel);

    //sleep to let the request propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    Channel connChannel = srvConn.getLastConnChannel();
    ChannelPipeline connPipeline = connChannel.getPipeline();
    SimpleObjectCaptureHandler cmdCapture = (SimpleObjectCaptureHandler)connPipeline.get(COMMAND_CAPTURE_NAME);

    Assert.assertTrue(cmdCapture != null, "has command capture");

    List<Object> cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 0, "no commands: " + cmds.size());

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    SimpleResponseBytesAggregatorHandler respAggregator =
        (SimpleResponseBytesAggregatorHandler)clientPipe.get(RESPONSE_AGGREGATOR_NAME);

    //verify that we get error response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 7, "enough bytes in response: " +
                      response1.readableBytes());
    Assert.assertEquals(response1.readByte(), BinaryProtocol.RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION);
    int responseLen = response1.readInt();
    Assert.assertTrue(responseLen >=4, "response length OK");
    short classLen = response1.readShort();
    Assert.assertTrue(classLen >= 0 && classLen <= 100,
                      "exception class name length in [0, 100]:" + classLen);
    Assert.assertTrue(response1.readableBytes() > classLen,
                      "enough bytes for the exception class name in the response");
    byte[] errorClassNameBytes = new byte[classLen];
    response1.readBytes(errorClassNameBytes);
    String errorClassName = new String(errorClassNameBytes);
    Assert.assertEquals(errorClassName,
                        UnsupportedProtocolVersionException.class.getName(),
                        "UnsupportedProtocolVersionException returned: " + errorClassName);
    Assert.assertTrue(response1.readableBytes() >= 2, "error message returned");
    short errorMsgLen = response1.readShort();
    Assert.assertTrue(errorMsgLen >= 0 && errorMsgLen <= 1000,
                      "error message length in [0, 1000]:" + errorMsgLen);
    Assert.assertTrue(response1.readableBytes() >= errorMsgLen,
                       "enough bytes for the error message in the response");

    cmdCapture.clear();
    respAggregator.clear();

    srvConn.stop();
    clientConn.stop();
  }

  @Test
  /** Send simple StartSendEvents commands and check if they have been parsed correctly */
  public void testDecodeStartSendEventsHappyPath() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(
        SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);

    relayConfigBuilder.setSourceName("101", "source1");
    relayConfigBuilder.setSourceName("102", "source2");
    relayConfigBuilder.setSourceName("103", "source3");
    relayConfigBuilder.setSourceName("104", "source4");
    relayConfigBuilder.setSourceName("105", "source5");

    relayConfigBuilder.getDataSources().getSequenceNumbersHandler()
                      .setType(MaxSCNReaderWriterStaticConfig.Type.IN_MEMORY.toString());

    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();
    SourceIdNameRegistry sourcesReg = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(1), 1234L);

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 new SendEventsRequest.ExecHandlerFactory(eventBufMult,
                                                                          relayConfig,
                                                                          multiServerSeqHandler,
                                                                          sourcesReg,
                                                                          null, null, null, null));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(5, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(5, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    StartSendEventsRequest req1 = StartSendEventsRequest.createV1(1);
    final ChannelFuture req1Future = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return req1Future.isDone();
          }
        }, "write complete", 1000, null);

    Assert.assertEquals(req1Future.isSuccess(), true, "write successful");

    Channel connChannel = srvConn.getLastConnChannel();
    ChannelPipeline connPipeline = connChannel.getPipeline();
    DummyPipelineFactory.SimpleObjectCaptureHandler cmdCapture =
      (DummyPipelineFactory.SimpleObjectCaptureHandler)connPipeline.get(COMMAND_CAPTURE_NAME);

    Assert.assertTrue(cmdCapture != null, "has command capture");

    List<Object> cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 1, "one command: " + cmds.size());

    Assert.assertTrue(cmds.get(0) instanceof StartSendEventsRequest, "found StartSendEventsRequest");
    StartSendEventsRequest startSendEventsCmd = (StartSendEventsRequest)cmds.get(0);

    Assert.assertEquals(startSendEventsCmd.getProtocolVersion(), (byte)1, "correct protocol version");
    Assert.assertEquals(startSendEventsCmd.getServerId(), 1);

    cmdCapture.clear();
  }

  @Test
  /** Send simple SendEvents commands and check if they have been parsed correctly */
  public void testDecodeSendEventsHappyPath() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();
    DbusEventBuffer eventBuffer = new DbusEventBuffer(_defaultBufferConf);

    //generate some data in the even buffer
    eventBuffer.start(1);
    eventBuffer.startEvents();
    eventBuffer.appendEvent(new DbusEventKey(1), (short)0, (short)101, System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[100], false, null);
    eventBuffer.endEvents(123L,  null);

    /** Relay config param via system.properties */
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();
    relayConfigBuilder.getSchemaRegistry().setType(
        SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());

    ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", relayConfigBuilder);
    configLoader.loadConfig(System.getProperties());
    relayConfigBuilder.getContainer().setExistingMbeanServer(null);

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

    SourceIdNameRegistry srcRegistry = SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 new SendEventsRequest.ExecHandlerFactory(eventBufMult,
                                                                          relayConfig,
                                                                          multiServerSeqHandler,
                                                                          srcRegistry,
                                                                          null, null, null, null));
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 new SendEventsRequest.ExecHandlerFactory(eventBufMult,
                                                                          relayConfig,
                                                                          multiServerSeqHandler,
                                                                          srcRegistry,
                                                                          null, null, null,  null));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(6, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(6, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    DbusEventIterator iter = eventBuffer.acquireIterator("test1");
    Assert.assertTrue(iter.hasNext());
    DbusEvent event1 = iter.next();
    ByteBuffer event1Bytes = event1.getRawBytes();

    ChannelBuffer event1Buffer = ChannelBuffers.copiedBuffer(event1Bytes);


    SendEventsRequest req1 = new SendEventsRequest((byte)1, 9876L, event1Buffer, 0, event1.size());
    final ChannelFuture req1Future = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return req1Future.isDone();
          }
        }, "write complete", 1000, null);

    Assert.assertEquals(req1Future.isSuccess(), true, "write successful");

    Channel connChannel = srvConn.getLastConnChannel();
    ChannelPipeline connPipeline = connChannel.getPipeline();
    DummyPipelineFactory.SimpleObjectCaptureHandler cmdCapture = (DummyPipelineFactory.SimpleObjectCaptureHandler)connPipeline.get(COMMAND_CAPTURE_NAME);

    Assert.assertTrue(cmdCapture != null, "has command capture");

    List<Object> cmds = cmdCapture.getMessages();
    Assert.assertEquals(cmds.size(), 1, "one command: " + cmds.size());

    Assert.assertTrue(cmds.get(0) instanceof SendEventsRequest, "found SendEventsRequest");
    SendEventsRequest sendEventsCmd = (SendEventsRequest)cmds.get(0);

    Assert.assertEquals(sendEventsCmd.getProtocolVersion(), (byte)1, "correct protocol version");
    Assert.assertEquals(sendEventsCmd.getBinlogOffset(), 9876L);
    ChannelBuffer req1EventBuffer = sendEventsCmd.obtainEventBuffer();
    event1Buffer.readerIndex(0);
    req1EventBuffer.readerIndex(0);
    Assert.assertEquals(event1Buffer.readableBytes(), req1EventBuffer.readableBytes(),
                        "same event size");
    int cmpRes = event1Buffer.compareTo(req1EventBuffer);
    Assert.assertEquals(cmpRes, 0, "event bytes match");

    cmdCapture.clear();
  }

  @Test
  /** Tests how the decoder reacts if the command parser returns an error */
  public void testCommandParserError() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();
    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);
    SourceIdNameRegistry sourcesReg =
        SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 new SendEventsRequest.ExecHandlerFactory(eventBufMult,
                                                                          relayConfig,
                                                                          multiServerSeqHandler,
                                                                          sourcesReg,
                                                                          null, null, null, null));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(7, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(7, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //send a command with invalid version
    byte[] req1Bytes = new byte[]{StartSendEventsRequest.OPCODE, 0x66, 0x10, 0x20, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30};
    final ChannelFuture req1WriteFuture = clientChannel.write(ChannelBuffers.wrappedBuffer(req1Bytes));

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000000, null);


    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    SimpleResponseBytesAggregatorHandler respAggregator =
        (SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get response with RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 10);
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, BinaryProtocol.RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION);

  }

  @Test
  /** Tests how the decoder reacts if the command parser returns an exception */
  public void testCommandParserException() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();
    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, GetLastSequenceRequest.OPCODE,
                                 new GetLastSequenceRequest.BinaryParserFactory(),
                                 new GetLastSequenceRequest.ExecHandlerFactory(eventBufMult,
                                                                               multiServerSeqHandler));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(8, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(8, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //send a command with invalid version
    byte[] req1Bytes = new byte[]{GetLastSequenceRequest.OPCODE, 0x01, (byte)0xFF, (byte)0xFF,
                                   0x30, 0x30, 0x30, 0x30, 0x30, 0x30};
    final ChannelFuture req1WriteFuture = clientChannel.write(ChannelBuffers.wrappedBuffer(req1Bytes));

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000000, null);
    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    SimpleResponseBytesAggregatorHandler respAggregator =
        (SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get response with RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertTrue(response1.readableBytes() > 10);
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, BinaryProtocol.RESULT_ERR_INTERNAL_SERVER_ERROR);
  }

  @Test
  /** Tests the server timeouts in case of a command with incomplete data */
  public void testIncompleteDataTimeout() throws Exception
  {
    DbusEventBufferMult eventBufMult = createStandardEventBufferMult();
    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();
    HttpRelay.StaticConfig relayConfig = relayConfigBuilder.build();

    SequenceNumberHandlerFactory handlerFactory =
        relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();
    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);
    SourceIdNameRegistry sourcesReg =
        SourceIdNameRegistry.createFromIdNamePairs(relayConfig.getSourceIds());

    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 new SendEventsRequest.ExecHandlerFactory(eventBufMult,
                                                                          relayConfig,
                                                                          multiServerSeqHandler,
                                                                          sourcesReg,
                                                                          null, null, null, null));

    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = srvConn.startSynchronously(9, 100);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(9, 100);
    Assert.assertTrue(clientConnected, "client connected");

    Channel clientChannel = clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //send a command with invalid version
    byte[] req1Bytes = new byte[]{StartSendEventsRequest.OPCODE, 0x01, 0x10};
    final ChannelFuture req1WriteFuture = clientChannel.write(ChannelBuffers.wrappedBuffer(req1Bytes));

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000000, null);
    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    Thread.sleep(2 * READ_TIMEOUT_MS);

    Assert.assertTrue(! srvConn.getLastConnChannel().isConnected());
    Assert.assertTrue(!clientChannel.isConnected());
  }
}
