package com.linkedin.databus.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.netty.GenericHttpResponseHandler;
import com.linkedin.databus.client.netty.GenericHttpResponseHandler.KeepAliveType;
import com.linkedin.databus.client.netty.HttpResponseProcessor;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestGenericHttpResponseHandler
{
  static final ExecutorService BOSS_POOL = Executors.newCachedThreadPool();
  static final ExecutorService IO_POOL = Executors.newCachedThreadPool();
  static final int SERVER_ADDRESS_ID = 14455;
  static final LocalAddress SERVER_ADDRESS = new LocalAddress(SERVER_ADDRESS_ID);
  static SimpleTestServerConnection _dummyServer;

  @BeforeClass
  public void setUpClass()
  {
    TestUtil.setupLogging(true, null, Level.OFF);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    _dummyServer = new SimpleTestServerConnection(ByteOrder.BIG_ENDIAN,
                                                  SimpleTestServerConnection.ServerType.NIO);
	_dummyServer.setPipelineFactory(new ChannelPipelineFactory() {
		@Override
		public ChannelPipeline getPipeline() throws Exception {
			return Channels.pipeline(new HttpServerCodec());
		}
	});
	_dummyServer.start(SERVER_ADDRESS_ID);
  }

  @AfterClass
  public void tearDownClass()
  {
    _dummyServer.stop();
    BOSS_POOL.shutdownNow();
    IO_POOL.shutdownNow();
  }

  @Test
  public void testHappyPathNoChunking()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathNoChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
    	channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
    	HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    	resp.setContent(null);
    	resp.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
    	sendServerResponse(clientAddr, resp, 1000);
    	final List<String> callbacks = respProcessor.getCallbacks();
    	TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 2 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
    	Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "finishResponse");
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testHappyPathWithCloseNoChunking()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathWithCloseNoChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setContent(null);
        resp.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
        sendServerResponse(clientAddr, resp, 1000);
        TestUtil.sleep(200);
        sendServerClose(clientAddr, -1);
        final List<String> callbacks = respProcessor.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 2 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "finishResponse");
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 2);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testReadTimeoutNoChunking()
  {
    final Logger log = Logger.getLogger("GenericHttpResponseHandler.testReadTimeoutNoChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));

        Channels.fireExceptionCaught(channel, new ReadTimeoutException());
        channel.close();

        final List<String> callbacks = respProcessor.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 2 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertTrue(callbacks.get(0).startsWith("channelException"));
        Assert.assertEquals(callbacks.get(1), "channelClosed");
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 2);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testHappyPathChunking()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));

        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes()));
        sendServerResponse(clientAddr, chunk1, 1000);

        HttpChunk chunk2 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk2".getBytes()));
        sendServerResponse(clientAddr, chunk2, 1000);

        sendServerResponse(clientAddr, new DefaultHttpChunkTrailer(), 1000);

        final List<String> callbacks = respProcessor.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return callbacks.size() == 5;
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertEquals(callbacks.get(2), "addChunk");
        Assert.assertEquals(callbacks.get(3), "addTrailer");
        Assert.assertEquals(callbacks.get(4), "finishResponse");
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 5);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testHappyPathWithCloseChunking()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathWithCloseChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes()));
        sendServerResponse(clientAddr, chunk1, 1000);

        HttpChunk chunk2 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk2".getBytes()));
        sendServerResponse(clientAddr, chunk2, 1000);

        sendServerResponse(clientAddr, new DefaultHttpChunkTrailer(), 1000);
        TestUtil.sleep(200);
        sendServerClose(clientAddr, -1);
        final List<String> callbacks = respProcessor.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 5 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertEquals(callbacks.get(2), "addChunk");
        Assert.assertEquals(callbacks.get(3), "addTrailer");
        Assert.assertEquals(callbacks.get(4), "finishResponse");
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 5);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testEarlyServerCloseChunking()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testEarlyServerCloseChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    Assert.assertTrue(channel.isConnected());
    final SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);

        TestUtil.assertWithBackoff(new ConditionCheck(){
          @Override
          public boolean check()
          {
            return null != _dummyServer.getChildChannel(clientAddr);
          }
        }, "make sure we have all tracking populated for client connection", 1000, log);

        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes()));
        sendServerResponse(clientAddr, chunk1, 1000);

        TestUtil.sleep(200);

        sendServerClose(clientAddr, -1);

        final List<String> callbacks = respProcessor.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 3 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertEquals(callbacks.get(2), "channelClosed");
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 3);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testReadTimeoutChunking()
  {
    final Logger log = Logger.getLogger("GenericHttpResponseHandler.testReadTimeoutChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes()));
        sendServerResponse(clientAddr, chunk1, 1000);
        final List<String> callbacks = respProcessor.getCallbacks();

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 2 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");

        Channels.fireExceptionCaught(channel, new ReadTimeoutException());
        channel.close();

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 4 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertTrue(callbacks.get(2).startsWith("channelException"));
        Assert.assertEquals(callbacks.get(3), "channelClosed");
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 4);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testErrorServerResponseChunking()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testErrorServerResponseChunking");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes()));
        sendServerResponse(clientAddr, chunk1, 1000);

        sendServerResponse(clientAddr, new DefaultHttpChunkTrailer(), 1000);

        final List<String> callbacks = respProcessor.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 4 == callbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertEquals(callbacks.get(2), "addTrailer");
        Assert.assertEquals(callbacks.get(3), "finishResponse");
        TestUtil.sleep(500);
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 4);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testRequestError()
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testRequestError");
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    Channel channel = createClientBootstrap(respProcessor);
    try
    {
      channel.close();
      channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));

      final List<String> callbacks = respProcessor.getCallbacks();
      TestUtil.sleep(500);
      //make sure that no new callbacks have showed up
      Assert.assertEquals(callbacks.size(), 0);
    }
    finally
    {
      channel.close();
    }
  }

  void sendServerResponse(SocketAddress clientAddr, Object response, long timeoutMillis)
  {
    Channel childChannel = _dummyServer.getChildChannel(clientAddr);
    Assert.assertNotEquals(childChannel, null);
    ChannelFuture writeFuture = childChannel.write(response);
    if (timeoutMillis > 0)
    {
      try
      {
        writeFuture.await(timeoutMillis);
      }
      catch (InterruptedException e)
      {
        //NOOP
      }
      Assert.assertTrue(writeFuture.isDone());
      Assert.assertTrue(writeFuture.isSuccess());
    }
  }

  void sendServerClose(SocketAddress clientAddr, long timeoutMillis)
  {
    Channel childChannel = _dummyServer.getChildChannel(clientAddr);
    Assert.assertNotEquals(childChannel, null);
    ChannelFuture closeFuture = childChannel.close();
    if (timeoutMillis > 0)
    {
      try
      {
        closeFuture.await(timeoutMillis);
      }
      catch (InterruptedException e)
      {
        //NOOP
      }
      Assert.assertTrue(closeFuture.isDone());
      Assert.assertTrue(closeFuture.isSuccess());
    }
  }

  Channel createClientBootstrap(HttpResponseProcessor respProcessor)
  {
    final GenericHttpResponseHandler responseHandler =
            new GenericHttpResponseHandler(respProcessor, KeepAliveType.KEEP_ALIVE);
        ClientBootstrap client = new ClientBootstrap(new NioClientSocketChannelFactory(BOSS_POOL, IO_POOL));
        client.setPipelineFactory(new ChannelPipelineFactory() {
    		@Override
    		public ChannelPipeline getPipeline() throws Exception {
    			return Channels.pipeline(
    			    new LoggingHandler(InternalLogLevel.DEBUG),
    			    new HttpClientCodec(),
    			    new LoggingHandler(InternalLogLevel.DEBUG),
    			    responseHandler);
    		}
    	});
    ChannelFuture connectFuture = client.connect(new InetSocketAddress("localhost", SERVER_ADDRESS_ID));
    try {
		connectFuture.await(1000);
	} catch (InterruptedException e) {
		//NOOP
	}
    Assert.assertTrue(connectFuture.isDone() && connectFuture.isSuccess());
    return connectFuture.getChannel();
  }

}

class TestHttpResponseProcessor implements HttpResponseProcessor
{
  private final List<String> _callbacks = new ArrayList<String>();
  private final Logger _log;

  public TestHttpResponseProcessor(Logger log)
  {
    _log = log;
  }

  public List<String> getCallbacks()
  {
    return _callbacks;
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
    if (null != _log) _log.info("startResponse");
    _callbacks.add("startResponse");
  }

  @Override
  public void addChunk(HttpChunk chunk) throws Exception
  {
    if (null != _log) _log.info("addChunk");
    _callbacks.add("addChunk");
  }

  @Override
  public void addTrailer(HttpChunkTrailer trailer) throws Exception
  {
    if (null != _log) _log.info("addTrailer");
    _callbacks.add("addTrailer");
  }

  @Override
  public void finishResponse() throws Exception
  {
    if (null != _log) _log.info("finishResponse");
    _callbacks.add("finishResponse");
  }

  @Override
  public void channelException(Throwable cause)
  {
    if (null != _log) _log.info("channelException: " + cause);
    _callbacks.add("channelException(" + cause.getClass().getSimpleName() + ")");
  }

  @Override
  public void channelClosed()
  {
    if (null != _log) _log.info("channelClosed");
    _callbacks.add("channelClosed");
  }

  public void clearCallbacks()
  {
    _callbacks.clear();
  }

}
