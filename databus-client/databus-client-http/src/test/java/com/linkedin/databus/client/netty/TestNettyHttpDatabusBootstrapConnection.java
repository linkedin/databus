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
package com.linkedin.databus.client.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusBootstrapConnection;
import com.linkedin.databus.client.DatabusBootstrapConnectionStateMessage;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

/**
 * Unit tests for {@link NettyHttpDatabusBootstrapConnection}
 */
public class TestNettyHttpDatabusBootstrapConnection
{
  static NettyHttpConnectionFactory CONN_FACTORY;
  static SimpleTestServerConnection _dummyServer;
  static final int SERVER_ADDRESS_ID = 15466;
  static final long DEFAULT_READ_TIMEOUT_MS = 10000;
  static final long DEFAULT_WRITE_TIMEOUT_MS = 10000;
  static final ExecutorService BOSS_POOL = Executors.newCachedThreadPool();
  static final ExecutorService IO_POOL = Executors.newCachedThreadPool();
  static final Timer NETWORK_TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
  static int MAX_EVENT_VERSION = DbusEventFactory.DBUS_EVENT_V2;
  static final ChannelGroup TEST_CHANNELS_GROUP = new DefaultChannelGroup();
  static final String SOURCE1_NAME = "test.source1";
  static final ServerInfo BOOTSTRAP_SERVER_INFO =
      new ServerInfo("testBootstrap", "master", new InetSocketAddress(SERVER_ADDRESS_ID), SOURCE1_NAME);

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestNettyHttpDatabusBootstrapConnection_" ,
                                             ".log" , Level.INFO);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    _dummyServer = new SimpleTestServerConnection(new DbusEventV2Factory().getByteOrder(),
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
                                       clientCfgBuilder.getContainer().getBstReadTimeoutMs(),
                                       4, // protocolVersion
                                       MAX_EVENT_VERSION,
                                       TEST_CHANNELS_GROUP);
  }

  @Test
  /**
   * This is a unit test for DDSDBUS-3537. There is a lag between a network channel disconnect and the
   * state change in AbstractNettyHttpConnection. This can cause a race condition in various requestXXX objects which
   * check the state of the connection using the network channel. As a result, they may attempt to reconnect while
   * AbstractNettyHttpConnection is still in CONNECTED state which causes an error for an incorrect transition
   * CONNECTED -> CONNECTING.
   *
   *  The test simulates the above condition by injecting a handler in the client pipeline which artificially holds up
   *  the channelClosed message. As a result we can inject a request while the netty channel is disconnected but the
   *  AbstractNettyHttpConnection object has not detected this yet.
   */
  public void testServerBootstrapDisconnect() throws Exception
  {
    final Logger log = Logger.getLogger("TestNettyHttpDatabusBootstrapConnection.testServerBootstrapDisconnect");
    log.info("starting");

    log.info("setup the client");
    TestingConnectionCallback callback = TestingConnectionCallback.createAndStart("testServerSourcesDisconnect");
    DummyRemoteExceptionHandler remoteExceptionHandler = new DummyRemoteExceptionHandler();
    final NettyHttpDatabusBootstrapConnection conn =
        (NettyHttpDatabusBootstrapConnection)
        CONN_FACTORY.createConnection(BOOTSTRAP_SERVER_INFO, callback, remoteExceptionHandler);
    try
    {
      log.info("initial setup");
      final List<String> sourceNamesList = Arrays.asList(SOURCE1_NAME);
      final Checkpoint cp = Checkpoint.createOnlineConsumptionCheckpoint(0);
      BootstrapCheckpointHandler cpHandler = new BootstrapCheckpointHandler(sourceNamesList);
      cpHandler.createInitialBootstrapCheckpoint(cp, 0L);
      final DummyDatabusBootstrapConnectionStateMessage bstCallback =
          new DummyDatabusBootstrapConnectionStateMessage(log);

      log.info("process a normal startSCN which should establish the connection");
      sendStartScnHappyPath(conn, cp, bstCallback, SOURCE1_NAME, 100L, log);
      Assert.assertTrue(conn.isConnected());

      //wait for the response
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return null != bstCallback.getCheckpoint();
        }
      }, "wait for /startSCN response", 100, log);

      log.info("verify /startSCN response");
      final Checkpoint startScnCp = bstCallback.getCheckpoint();
      Assert.assertNotNull(startScnCp);
      Assert.assertEquals(100L, startScnCp.getBootstrapStartScn().longValue());

      log.info("instrument the client pipeline so that we can intercept and delay the channelClosed message");
      final Semaphore passMessage = new Semaphore(1);
      final CountDownLatch closeSent = new CountDownLatch(1);
      passMessage.acquire();
      conn._channel.getPipeline().addBefore("handler", "closeChannelDelay",
          new SimpleChannelHandler(){
            @Override
            public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
              closeSent.countDown();
              passMessage.acquire();
              try
              {
                super.channelClosed(ctx, e);
              }
              finally
              {
                passMessage.release();
              }
            }
      });

      final Channel serverChannel = getServerChannelForClientConn(conn);
      Thread asyncChannelClose = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          log.info("closing server channel");
          serverChannel.close();
          log.info("server channel: closed");
          closeSent.countDown();
        }
      }, "asyncChannelCloseThread");
      asyncChannelClose.setDaemon(true);

      Thread asyncBootstrapReq = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          conn.requestStream("1", null, 10000, startScnCp, bstCallback);
        }
      }, "asyncBootstrapReqThread");
      asyncBootstrapReq.setDaemon(true);

      log.info("simultaneously closing connection and sending /bootstrap request");
      bstCallback.reset();
      asyncChannelClose.start();
      Assert.assertTrue(closeSent.await(1000, TimeUnit.MILLISECONDS));
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return !conn._channel.isConnected();
        }
      }, "waiting for disconnect on the client side", 1000, log);
      Assert.assertEquals(AbstractNettyHttpConnection.State.CONNECTED, conn.getNetworkState());
      log.info("asynchronously sending /bootstrap");
      asyncBootstrapReq.start();

      log.info("letting channelClose get through");
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return bstCallback.isStreamRequestError();
        }
      }, "wait for streamRequestError callback", 1000, log);
      passMessage.release();
      log.info("finished");
    }
    finally
    {
      conn.close();
      callback.shutdown();
      log.info("cleaned");
    }
  }

  private Channel getServerChannelForClientConn(final AbstractNettyHttpConnection conn)
  {
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    return _dummyServer.getChildChannel(clientAddr);

  }

  private void sendStartScnHappyPath(final NettyHttpDatabusBootstrapConnection conn,
                                     Checkpoint cp,
                                     DummyDatabusBootstrapConnectionStateMessage bstCallback,
                                     String sourceNames,
                                     long startScn,
                                     Logger log) throws IOException,
      JsonGenerationException,
      JsonMappingException
  {
    //send startSCN()
    conn.requestStartScn(cp, bstCallback, sourceNames);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return null != conn._channel && conn._channel.isConnected();
      }
    }, "wait for client to connect", 1000, log);

    //introspect connection to server
    Channel channel = conn._channel;
    SocketAddress clientAddr = channel.getLocalAddress();

    Channel serverChannel = _dummyServer.getChildChannel(clientAddr);
    ChannelPipeline serverPipeline = serverChannel.getPipeline();
    SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

    Assert.assertTrue(objCapture.waitForMessage(1000, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);

    //verify we got a /startSCN call
    HttpRequest msgReq = (HttpRequest)msgObj;
    Assert.assertTrue(msgReq.getUri().startsWith("/startSCN"));

    //send back some response
    HttpResponse sourcesResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                       HttpResponseStatus.OK);
    sourcesResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    sourcesResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
    ObjectMapper objMapper = new ObjectMapper();
    HttpChunk body =
        new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(objMapper.writeValueAsBytes(String.valueOf(startScn))));
    NettyTestUtils.sendServerResponses(_dummyServer, clientAddr, sourcesResp, body);
  }

  static class DummyDatabusBootstrapConnectionStateMessage implements DatabusBootstrapConnectionStateMessage
  {
    final Logger _log;
    private boolean _streamRequestError;
    private boolean _streamResponseError;
    private boolean _startScnRequestError;
    private boolean _startScnResponseError;
    private Checkpoint _checkpoint;

    public DummyDatabusBootstrapConnectionStateMessage(Logger log)
    {
      _log = log;
      reset();
    }

    public void reset()
    {
      _streamRequestError = false;
      _streamResponseError = false;
      _startScnRequestError = false;
      _startScnResponseError = false;
      _checkpoint = null;
    }

    @Override
    public void switchToStreamRequestError()
    {
      _streamRequestError = true;
    }

    @Override
    public void switchToStreamResponseError()
    {
      _streamResponseError = true;
    }

    @Override
    public void switchToStreamSuccess(ChunkedBodyReadableByteChannel result)
    {
    }

    @Override
    public void switchToStartScnRequestError()
    {
      _startScnRequestError = true;
    }

    @Override
    public void switchToStartScnResponseError()
    {
      _startScnResponseError = true;
    }

    @Override
    public void switchToStartScnSuccess(Checkpoint cp,
                                        DatabusBootstrapConnection bootstrapConnection,
                                        ServerInfo serverInfo)
    {
      if (null != _log)
      {
        _log.info("switchToStartScnSuccess: checkpoint=" + cp);
      }
      _checkpoint = cp;
    }

    @Override
    public void switchToStartScnRequestSent()
    {
    }

    @Override
    public void switchToTargetScnRequestError()
    {
    }

    @Override
    public void switchToTargetScnResponseError()
    {
    }

    @Override
    public void switchToTargetScnSuccess()
    {
    }

    @Override
    public void switchToTargetScnRequestSent()
    {
    }

    @Override
    public void switchToBootstrapDone()
    {
    }

    /**
     * @return the streamRequestError
     */
    public boolean isStreamRequestError()
    {
      return _streamRequestError;
    }

    /**
     * @return the streamResponseError
     */
    public boolean isStreamResponseError()
    {
      return _streamResponseError;
    }

    /**
     * @return the startScnRequestError
     */
    public boolean isStartScnRequestError()
    {
      return _startScnRequestError;
    }

    /**
     * @return the startScnResponseError
     */
    public boolean isStartScnResponseError()
    {
      return _startScnResponseError;
    }

    public Checkpoint getCheckpoint()
    {
      return _checkpoint;
    }

  }
}
