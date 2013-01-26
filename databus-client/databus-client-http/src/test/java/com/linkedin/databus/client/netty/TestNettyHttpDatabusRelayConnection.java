package com.linkedin.databus.client.netty;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestNettyHttpDatabusRelayConnection
{
  static final String SOURCE1_SCHEMA_STR =
      "{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";
  static final ExecutorService BOSS_POOL = Executors.newCachedThreadPool();
  static final ExecutorService IO_POOL = Executors.newCachedThreadPool();
  static final int SERVER_ADDRESS_ID = 14466;
  static final LocalAddress SERVER_ADDRESS = new LocalAddress(SERVER_ADDRESS_ID);
  static final Timer NETWORK_TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
  static final ChannelGroup TEST_CHANNELS_GROUP = new DefaultChannelGroup();
  static final long DEFAULT_READ_TIMEOUT_MS = 10000;
  static final long DEFAULT_WRITE_TIMEOUT_MS = 10000;
  static final String SOURCE1_NAME = "test.source1";
  static final ServerInfo RELAY_SERVER_INFO =
      new ServerInfo("testRelay", "master", new InetSocketAddress(SERVER_ADDRESS_ID), SOURCE1_NAME);
  static NettyHttpConnectionFactory CONN_FACTORY;
  static SimpleTestServerConnection _dummyServer;
  static DbusEventBuffer.StaticConfig _bufCfg;

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    TestUtil.setupLogging(true, null, Level.INFO);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    _dummyServer = new SimpleTestServerConnection(DbusEventV1.byteOrder,
                                                  SimpleTestServerConnection.ServerType.NIO);
    _dummyServer.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(new LoggingHandler(InternalLogLevel.DEBUG),
                                     new HttpServerCodec(),
                                     new LoggingHandler(InternalLogLevel.DEBUG),
                                     new SimpleObjectCaptureHandler());
        }
    });
    _dummyServer.start(SERVER_ADDRESS_ID);

    DatabusHttpClientImpl.Config clientCfgBuilder = new DatabusHttpClientImpl.Config();
    clientCfgBuilder.getContainer().setReadTimeoutMs(DEFAULT_READ_TIMEOUT_MS);
    clientCfgBuilder.getContainer().setWriteTimeoutMs(DEFAULT_WRITE_TIMEOUT_MS);

    CONN_FACTORY =
        new NettyHttpConnectionFactory(BOSS_POOL, IO_POOL, null, NETWORK_TIMER,
                                       clientCfgBuilder.getContainer().getWriteTimeoutMs(),
                                       clientCfgBuilder.getContainer().getReadTimeoutMs(),
                                       2,
                                       TEST_CHANNELS_GROUP);
    DbusEventBuffer.Config bufCfgBuilder = new DbusEventBuffer.Config();
    bufCfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    bufCfgBuilder.setMaxSize(100000);
    bufCfgBuilder.setScnIndexSize(128);
    bufCfgBuilder.setReadBufferSize(1);

    _bufCfg = bufCfgBuilder.build();
  }

  @AfterClass
  public void tearDownClass()
  {
    _dummyServer.stop();
    BOSS_POOL.shutdownNow();
    IO_POOL.shutdownNow();
  }

  @Test
  public void testHappyPath() throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testHappyPath");

    DbusEventBuffer buf = createSimpleBuffer();

    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerSourcesDisconnect() throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerSourcesDisconnect");
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerRegisterDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRegisterDisconnect");
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerRegisterDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerRegisterReqDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRegisterReqDisconnect");
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerRegisterReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerStreamDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerStreamDisconnect");

    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerStreamReqDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerStreamReqDisconnect");

    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerStreamReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerPartialDisconnect()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerPartialDisconnect");

    DbusEventBuffer buf = createSimpleBuffer();
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerPartialStreamIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }


  @Test
  public void testServerSourcesTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerSourcesTimeout");
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerSourcesReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerRegisterTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerRegisterTimeout");
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerStreamTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerStreamTimeout");
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerStreamReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  public void testServerPartialTimeout()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerPartialResponse");

    DbusEventBuffer buf = createSimpleBuffer();
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      runServerPartialStreamTimeoutIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  /**
   * <p>Test scenario
   * <p>
   *
   * <ul>
   *   <li> relay disconnected on /sources
   *   <li> relay timed out on /sources
   *   <li> relay disconnected on /sources again
   *   <li> relay timed out on /register
   *   <li> relay disconnected on /register
   *   <li> relay timed out on /register again
   *   <li> relay disconnected on /stream
   *   <li> relay disconnected on /stream again
   *   <li> relay succeeded on /stream
   *   <li> relay succeeded on /stream
   *   <li> relay timed out on partial /stream
   *   <li> relay succeeded on /stream
   *   <li> relay timed out on /stream
   *   <li> relay succeeded on /stream
   * </ul>
   */
  public void testServerFixedScenario()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerFixedScenario");

    DbusEventBuffer buf = createSimpleBuffer();
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      log.info("********* 1. relay disconnected on /sources ********");
      runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 2. relay timed out on /sources ********");
      runServerSourcesReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 3. relay disconnected on /sources ********");
      runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 4. relay timed out on /register ********");
      runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 5. relay disconnected on /register ********");
      runServerRegisterDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 6. relay timed out on /register ********");
      runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 7. relay disconnected on /stream ********");
      runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 8. relay disconnected on /stream ********");
      runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 9. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 10. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 11. relay timed out on partial /stream ********");
      runServerPartialStreamTimeoutIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 12. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
      log.info("********* 13. relay timed out on /stream ********");
      runServerStreamReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
      log.info("********* 14. relay success on /stream ********");
      runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  @Test
  /**
   * <p>Random sequence of test iterations
   */
  public void testServerRandomScenario()
      throws IOException, ScnNotFoundException, OffsetNotFoundException
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusRelayConnection.testServerFixedScenario");

    DbusEventBuffer buf = createSimpleBuffer();
    RelayConnectionCallback callback = RelayConnectionCallback.createAndStart("testHappyPath");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusRelayConnection conn =
        (NettyHttpDatabusRelayConnection)
        CONN_FACTORY.createRelayConnection(RELAY_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      final int iterNum = 15;
      Random rng = new Random();
      for (int i = 1; i<= iterNum; ++i )
      {
        int testId = rng.nextInt(10);
        switch (testId)
        {
        case 0:
        {
          log.info("======> step " + i + ": runHappyPathIteration");
          runHappyPathIteration(log, buf, callback, remoteExceptionHandler, conn);
          break;
        }
        case 1:
        {
          log.info("======> step " + i + ": runServerSourcesDisconnectIteration");
          runServerSourcesDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 2:
        {
          log.info("======> step " + i + ": runServerSourcesReadTimeoutIteration");
          runServerSourcesReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 3:
        {
          log.info("======> step " + i + ": runServerRegisterDisconnectIteration");
          runServerRegisterDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 4:
        {
          log.info("======> step " + i + ": runServerRegisterReqDisconnectIteration");
          runServerRegisterReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 5:
        {
          log.info("======> step " + i + ": runServerRegisterReadTimeoutIteration");
          runServerRegisterReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 6:
        {
          log.info("======> step " + i + ": runServerStreamDisconnectIteration");
          runServerStreamDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 7:
        {
          log.info("======> step " + i + ": runServerStreamReqDisconnectIteration");
          runServerStreamReqDisconnectIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 8:
        {
          log.info("======> step " + i + ": runServerStreamReadTimeoutIteration");
          runServerStreamReadTimeoutIteration(log, callback, remoteExceptionHandler, conn);
          break;
        }
        case 9:
        {
          log.info("======> step " + i + ": runServerPartialStreamTimeoutIteration");
          runServerPartialStreamTimeoutIteration(log, buf, callback, remoteExceptionHandler, conn);
          break;
        }
        default:
        {
          Assert.fail("step " + i + ": unknown test id: " + testId);
        }
        }
      }
    }
    finally
    {
      conn.close();
      callback.shutdown();
    }
  }

  private void runServerSourcesReadTimeoutIteration(final Logger log,
                                                   RelayConnectionCallback callback,
                                                   DummyRemoteExceptionHandler remoteExceptionHandler,
                                                   final NettyHttpDatabusRelayConnection conn)
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertEquals("/sources", msgReq.getUri());

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerRegisterReadTimeoutIteration(final Logger log,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);


    conn.requestRegister("1", msg);

    //verify server gets the /register request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerStreamReadTimeoutIteration(final Logger log,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerPartialStreamTimeoutIteration(final Logger log,
                                     DbusEventBuffer buf,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    ////// send back some partial response
    ChannelBuffer tmpBuf = NettyTestUtils.streamToChannelBuffer(buf, cp, 10000, null);
    _dummyServer.sendServerResponse(clientAddr, sourcesResp, 1000);
    _dummyServer.sendServerResponse(clientAddr, new DefaultHttpChunk(tmpBuf), 1000);

    //Trigger a read timeout
    TestUtil.sleep(DEFAULT_READ_TIMEOUT_MS + 100);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
}

  private void runServerSourcesDisconnectIteration(final Logger log,
                                                   RelayConnectionCallback callback,
                                                   DummyRemoteExceptionHandler remoteExceptionHandler,
                                                   final NettyHttpDatabusRelayConnection conn)
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertEquals("/sources", msgReq.getUri());

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerRegisterDisconnectIteration(final Logger log,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();
    final SocketAddress finalClientAddr = clientAddr;

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return _dummyServer.getChildChannel(finalClientAddr) != null;
      }
    }, "client connected", 100, log);

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);


    callback.clearLastMsg();
    objCapture.clear();
    conn.requestRegister("1", msg);

    //verify server gets the /register request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerRegisterReqDisconnectIteration(final Logger log,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    runHappyPathSources(log,
                            callback,
                            remoteExceptionHandler,
                            clientAddr,
                            objCapture);

    callback.clearLastMsg();
    objCapture.clear();

    serverChannel.close();

    conn.requestRegister("1", msg);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_REQUEST_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());
    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerStreamDisconnectIteration(final Logger log,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerStreamReqDisconnectIteration(final Logger log,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();

    serverChannel.close();

    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_REQUEST_ERROR,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runServerPartialStreamIteration(final Logger log,
                                     DbusEventBuffer buf,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send partial /stream
    callback.clearLastMsg();
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //////// verify server gets the /stream request
    HttpRequest msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    ////// send back some partial response
    ChannelBuffer tmpBuf = NettyTestUtils.streamToChannelBuffer(buf, cp, 10000, null);
    _dummyServer.sendServerResponse(clientAddr, sourcesResp, 1000);
    _dummyServer.sendServerResponse(clientAddr, new DefaultHttpChunk(tmpBuf), 1000);

    serverChannel.close();

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    Assert.assertEquals(1, callback.getAllMsgs().size());

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runHappyPathIteration(final Logger log,
                                     DbusEventBuffer buf,
                                     RelayConnectionCallback callback,
                                     DummyRemoteExceptionHandler remoteExceptionHandler,
                                     final NettyHttpDatabusRelayConnection conn) throws JsonGenerationException,
      JsonMappingException,
      IOException,
      ScnNotFoundException,
      OffsetNotFoundException
  {
    //connect to server and send /sources
    TestResponseProcessors.TestConnectionStateMessage msg = new TestResponseProcessors.TestConnectionStateMessage();
    conn.requestSources(msg);

    waitForServerConnection(conn, log);

    //introspect connection to server
    Channel channel = conn._channel;
    final SocketAddress clientAddr = channel.getLocalAddress();

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return null != _dummyServer.getChildChannel(clientAddr);
      }
    }, "client connection established", 1000, log);

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    //verify server gets the /source request
    HttpResponse sourcesResp =
        runHappyPathSources(log,
                                callback,
                                remoteExceptionHandler,
                                clientAddr,
                                objCapture);

    //send /register
    runHappyPathRegister(log,
                         callback,
                         remoteExceptionHandler,
                         conn,
                         msg,
                         clientAddr,
                         objCapture,
                         sourcesResp);

    //send /stream
    runHappyPathStream(log,
                       buf,
                       callback,
                       remoteExceptionHandler,
                       conn,
                       msg,
                       clientAddr,
                       objCapture,
                       sourcesResp);

    callback.clearLastMsg();
    objCapture.clear();
  }

  private void runHappyPathStream(final Logger log,
                                  DbusEventBuffer buf,
                                  RelayConnectionCallback callback,
                                  DummyRemoteExceptionHandler remoteExceptionHandler,
                                  final NettyHttpDatabusRelayConnection conn,
                                  TestResponseProcessors.TestConnectionStateMessage msg,
                                  SocketAddress clientAddr,
                                  SimpleObjectCaptureHandler objCapture,
                                  HttpResponse sourcesResp) throws ScnNotFoundException,
      OffsetNotFoundException,
      IOException
  {
    HttpRequest msgReq;
    objCapture.clear();
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    CheckpointMult cpm = new CheckpointMult();
    cpm.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    conn.requestStream("1", null, 1000, cpm, null, msg);


    //verify server gets the /stream request
    msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/stream"));

    //send back some response
    ChannelBuffer tmpBuf = NettyTestUtils.streamToChannelBuffer(buf, cp, 10000, null);
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, new DefaultHttpChunk(tmpBuf));

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());

    //wait for the readable byte channel to process the response and verify nothing has changed
    TestUtil.sleep(1000);
    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
  }

  private void runHappyPathRegister(final Logger log,
                                    RelayConnectionCallback callback,
                                    DummyRemoteExceptionHandler remoteExceptionHandler,
                                    final NettyHttpDatabusRelayConnection conn,
                                    TestResponseProcessors.TestConnectionStateMessage msg,
                                    SocketAddress clientAddr,
                                    SimpleObjectCaptureHandler objCapture,
                                    HttpResponse sourcesResp) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    HttpRequest msgReq;
    HttpChunk body;
    objCapture.clear();
    conn.requestRegister("1", msg);

    //verify server gets the /register request
    msgReq = captureRequest(objCapture);
    Assert.assertTrue(msgReq.getUri().startsWith("/register"));

    //send back some response
    RegisterResponseEntry entry = new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR);
    String responseStr = NettyTestUtils.generateRegisterResponse(entry);
    body = new DefaultHttpChunk(
        ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, body);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.REGISTER_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
  }

  private HttpResponse runHappyPathSources(final Logger log,
                                               RelayConnectionCallback callback,
                                               DummyRemoteExceptionHandler remoteExceptionHandler,
                                               SocketAddress clientAddr,
                                               SimpleObjectCaptureHandler objCapture)
  {
    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertEquals("/sources", msgReq.getUri());

    callback.clearLastMsg();
    objCapture.clear();

    //send back some response
    HttpResponse sourcesResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                       HttpResponseStatus.OK);
    sourcesResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    sourcesResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
    HttpChunk body =
        new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("[{\"id\":1,\"name\":\"test.source1\"}]".getBytes()));
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, body);

    waitForCallback(callback,
                    TestResponseProcessors.TestConnectionStateMessage.State.SOURCES_SUCCESS,
                    log);
    Assert.assertNull(remoteExceptionHandler.getLastException());
    callback.clearLastMsg();
    objCapture.clear();
    return sourcesResp;
  }

  private HttpRequest captureRequest(SimpleObjectCaptureHandler objCapture)
  {
    Object msgObj;
    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    HttpRequest msgReq = (HttpRequest)msgObj;
    return msgReq;
  }

  private DbusEventBuffer createSimpleBuffer()
  {
    DbusEventBuffer buf  = new DbusEventBuffer(_bufCfg);
    buf.start(0);
    buf.startEvents();
    buf.appendEvent(new DbusEventKey(1), (short)1, (short)1, System.nanoTime(), (short)1,
                    new byte[16], new byte[100], false, null);
    buf.appendEvent(new DbusEventKey(2), (short)1, (short)1, System.nanoTime(), (short)1,
                    new byte[16], new byte[100], false, null);
    buf.endEvents(10);
    return buf;
  }

  void waitForServerConnection(final NettyHttpDatabusRelayConnection conn, final Logger log)
  {
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return null != conn._channel && conn._channel.isConnected();
      }
    }, "waiting to connect to server", 100000, log);
  }

  void waitForCallback(final RelayConnectionCallback callback,
                       final TestResponseProcessors.TestConnectionStateMessage.State state,
                       final Logger log)
  {
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        TestResponseProcessors.TestConnectionStateMessage lastMsg = callback.getLastMsg();
        return null != lastMsg && lastMsg.getState().equals(state);
      }
    }, "waiting for state " + state, 1000, log);
  }

}

class RelayConnectionCallback extends AbstractActorMessageQueue
{
  TestResponseProcessors.TestConnectionStateMessage _lastMsg = null;
  List<TestResponseProcessors.TestConnectionStateMessage> _allMsgs =
      new ArrayList<TestResponseProcessors.TestConnectionStateMessage>();

  @Override
  protected boolean executeAndChangeState(Object message)
  {
    if (message instanceof TestResponseProcessors.TestConnectionStateMessage)
    {
      TestResponseProcessors.TestConnectionStateMessage m = (TestResponseProcessors.TestConnectionStateMessage)message;
      boolean success = true;
      _lastMsg = m;
      _allMsgs.add(m);

      return success;
    }
    else
    {
      return super.executeAndChangeState(message);
    }
  }

  public RelayConnectionCallback(String name)
  {
    super(name);
  }

  public TestResponseProcessors.TestConnectionStateMessage getLastMsg()
  {
    return _lastMsg;
  }

  public void clearLastMsg()
  {
    _lastMsg = null;
    _allMsgs.clear();
  }

  public static RelayConnectionCallback createAndStart(String name)
  {
    RelayConnectionCallback result = new RelayConnectionCallback(name);
    Thread runThread = new Thread(result, name + "-callback");
    runThread.setDaemon(true);
    runThread.start();

    return result;
  }

  public List<TestResponseProcessors.TestConnectionStateMessage> getAllMsgs()
  {
    return _allMsgs;
  }

}

class DummyRemoteExceptionHandler extends RemoteExceptionHandler
{
  Throwable _lastException = null;

  public DummyRemoteExceptionHandler()
  {
    super(null, null);
  }

  @Override
  public void handleException(Throwable remoteException) throws InvalidEventException,
      InterruptedException
  {
    _lastException = remoteException;
  }

  public Throwable getLastException()
  {
    return _lastException;
  }

  public void resetLastException()
  {
    _lastException = null;
  }

}
