package com.linkedin.databus.client.netty;
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


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
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
import org.jboss.netty.channel.ChannelHandler;
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
import org.jboss.netty.handler.codec.http.HttpRequest;
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

import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ChannelCloseListener;
import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener;
import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener;
import com.linkedin.databus.client.netty.GenericHttpResponseHandler.KeepAliveType;
import com.linkedin.databus.client.netty.GenericHttpResponseHandler.MessageState;
import com.linkedin.databus2.core.DatabusException;
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
  static org.apache.log4j.Level _logLevel = org.apache.log4j.Level.INFO;

  @BeforeClass
  public void setUpClass()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestGenericHttpResponseHandler_",
                                             ".log", Level.INFO);
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
    _logLevel = org.apache.log4j.Level.INFO;
  }

  @AfterClass
  public void tearDownClass()
  {
    _dummyServer.stop();
    BOSS_POOL.shutdownNow();
    IO_POOL.shutdownNow();
  }

  void setListeners(GenericHttpResponseHandler responseHandler,
      HttpResponseProcessor respProcessor,
      SendRequestResultListener requestProcessor,
      ChannelCloseListener channelCloseProcessor
      ) throws DatabusException
  {
    responseHandler.setResponseProcessor(respProcessor);
    responseHandler.setRequestListener(requestProcessor);
    responseHandler.setCloseListener(channelCloseProcessor);
  }


  @Test
  public void testHappyPathNoChunking() throws DatabusException
  {

    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathNoChunking");

    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);

    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
      setListeners(responseHandler,respProcessor,requestListener,closeListener);
    	channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

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
    	final List<String> connectCallbacks = connectListener.getCallbacks();
      final List<String> requestCallbacks = requestListener.getCallbacks();
      final List<String> closeCallbacks = closeListener.getCallbacks();

      stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
    	Assert.assertEquals(callbacks.get(0), "startResponse");
    	Assert.assertEquals(callbacks.get(1), "finishResponse");
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testHappyPathWithCloseNoChunking() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathWithCloseNoChunking");

    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

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
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();

        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
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
  public void testReadTimeoutNoChunking() throws InterruptedException, DatabusException
  {
    final Logger log = Logger.getLogger("GenericHttpResponseHandler.testReadTimeoutNoChunking");

    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    log.info("start");
    log.setLevel(_logLevel);
    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        ChannelFuture writeFuture = channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                                         HttpMethod.GET, "/test"));
        Assert.assertTrue(writeFuture.await(1000));

        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

        Channels.fireExceptionCaught(channel, new ReadTimeoutException());
        channel.close();

        final List<String> callbacks = respProcessor.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            log.debug("# callbacks:" + callbacks + ";expecting 1");
            return 1 == callbacks.size() && 1==closeCallbacks.size();
          }
        }, "waiting for response processed", 5000, null);
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();


        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
        Assert.assertTrue(callbacks.get(0).startsWith("channelException"));
        //Assert.assertEquals(callbacks.get(1), "channelClosed");  // we don't get channelClosed after exception anymore
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 1);
    }
    finally
    {
      channel.close();
      log.info("end");
    }
  }

  @Test
  public void testHappyPathChunking() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathChunking");

    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes(Charset.defaultCharset())));
        sendServerResponse(clientAddr, chunk1, 1000);

        HttpChunk chunk2 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk2".getBytes(Charset.defaultCharset())));
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
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();


        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
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
  public void testHappyPathWithCloseChunking() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testHappyPathWithCloseChunking");

    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);

    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes(Charset.defaultCharset())));
        sendServerResponse(clientAddr, chunk1, 1000);

        HttpChunk chunk2 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk2".getBytes(Charset.defaultCharset())));
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
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();

        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
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
  public void testEarlyServerCloseChunking() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testEarlyServerCloseChunking");

    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);
    Assert.assertTrue(channel.isConnected());
    final SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

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

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes(Charset.defaultCharset())));
        sendServerResponse(clientAddr, chunk1, 1000);

        TestUtil.sleep(200);

        sendServerClose(clientAddr, -1);

        final List<String> callbacks = respProcessor.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 3 == callbacks.size() && 1==closeCallbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();


        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertTrue(callbacks.get(2).startsWith("channelException")); // we get Exception, no ChannelClose
        //make sure that no new callbacks have showed up
        Assert.assertEquals(callbacks.size(), 3);
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testReadTimeoutChunking() throws DatabusException
  {
    final Logger log = Logger.getLogger("GenericHttpResponseHandler.testReadTimeoutChunking");
    log.info("start");
    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 1000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes(Charset.defaultCharset())));
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

        final List<String> closeCallbacks = closeListener.getCallbacks();
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 3 == callbacks.size() && 1==closeCallbacks.size();
          }
        }, "waiting for response processed", 1000, null);
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();

        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertTrue(callbacks.get(2).startsWith("channelException"));
        //Assert.assertEquals(callbacks.get(3), "channelClosed"); // no more channelClosed after channel Exception
        //make sure that no new callbacks have showed up
        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);

        Assert.assertEquals(callbacks.size(), 3);
    }
    finally
    {
      channel.close();
      log.info("end");
    }
  }

  @Test
  public void testErrorServerResponseChunking() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testErrorServerResponseChunking");
    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    responseHandler.setConnectionListener(connectListener);
    Channel channel = createClientBootstrap(responseHandler);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
        setListeners(responseHandler,respProcessor,requestListener,closeListener);
        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
        //It seems that there is a race condition between the writeFuture succeeding
        //and the writeComplete message getting to the handler. Make sure that the
        //writeComplete has got to the handler before we do anything else with
        //the channel.
        final GenericHttpResponseHandler handler = getResponseHandler(channel);
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return handler._messageState.hasSentRequest();
          }
        }, "request sent", 1000, log);

        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 2000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes(Charset.defaultCharset())));
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
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();

        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
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
  public void testRequestError() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testRequestError");

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    //Need this call to set respProcessor without triggering erroneous check
    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(respProcessor,KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);
    responseHandler.setRequestListener(requestListener);
    responseHandler.setConnectionListener(connectListener);
    responseHandler.setCloseListener(closeListener);

    Channel channel = createClientBootstrap(responseHandler);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {

      HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
      sendServerResponse(clientAddr, resp, 2000);

      channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));

      final List<String> callbacks = respProcessor.getCallbacks();
      final List<String> connectCallbacks = connectListener.getCallbacks();
      final List<String> requestCallbacks = requestListener.getCallbacks();
      final List<String> closeCallbacks = closeListener.getCallbacks();

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return 1 == closeCallbacks.size();
        }
      }, "waiting for close channel callback", 1000, null);



      //make sure that no new callbacks have showed up
      stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
      Assert.assertEquals(connectCallbacks.get(0),"onConnectSuccess");
      Assert.assertEquals(requestCallbacks.size(),1);
      Assert.assertEquals(requestCallbacks.get(0),"onSendRequestFailure");
      Assert.assertEquals(callbacks.size(), 0);
      Assert.assertEquals(closeCallbacks.size(),1);
      Assert.assertEquals(closeCallbacks.get(0),"onChannelClose");
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testRepeatedReadSuccess() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testRestRepeatedReadSuccess");

    //global responseHandler;
    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(KeepAliveType.KEEP_ALIVE);
    responseHandler.getLog().setLevel(_logLevel);

    TestConnectListener connectListener = new TestConnectListener(log);
    responseHandler.setConnectionListener(connectListener);

    Channel channel = createClientBootstrap(responseHandler);
    SocketAddress clientAddr = channel.getLocalAddress();
    try
    {
      for (int i=0; i < 2;++i)
      {
        TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
        TestSendRequestListener requestListener = new TestSendRequestListener(log);
        TestCloseListener closeListener = new TestCloseListener(log);

        setListeners(responseHandler, respProcessor, requestListener,closeListener);

        channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));

        TestUtil.sleep(1000);

        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
        sendServerResponse(clientAddr, resp, 2000);

        HttpChunk chunk1 = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("chunk1".getBytes(Charset.defaultCharset())));
        sendServerResponse(clientAddr, chunk1, 1000);

        sendServerResponse(clientAddr, new DefaultHttpChunkTrailer(), 1000);


        final List<String> callbacks = respProcessor.getCallbacks();
        final List<String> connectCallbacks = connectListener.getCallbacks();
        final List<String> requestCallbacks = requestListener.getCallbacks();
        final List<String> closeCallbacks = closeListener.getCallbacks();

        TestUtil.sleep(500);
        stateSanityCheck(connectCallbacks,requestCallbacks,callbacks,closeCallbacks);
        Assert.assertEquals(1,connectCallbacks.size());
        Assert.assertEquals(connectCallbacks.get(0),"onConnectSuccess");
        Assert.assertEquals(requestCallbacks.size(),1);
        Assert.assertEquals(requestCallbacks.get(0),"onSendRequestSuccess");
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return callbacks.size() == 4;
          }
        }, "waiting for response processed", 2000, null);
        Assert.assertEquals(callbacks.get(0), "startResponse");
        Assert.assertEquals(callbacks.get(1), "addChunk");
        Assert.assertEquals(callbacks.get(2), "addTrailer");
        Assert.assertEquals(callbacks.get(3), "finishResponse");
      }
    }
    finally
    {
      channel.close();
    }
  }

  @Test
  public void testConnectFail() throws DatabusException
  {
    Logger log = Logger.getLogger("GenericHttpResponseHandler.testConnectFail");

    TestHttpResponseProcessor respProcessor = new TestHttpResponseProcessor(log);
    TestConnectListener connectListener = new TestConnectListener(log);
    TestSendRequestListener requestListener = new TestSendRequestListener(log);
    TestCloseListener closeListener = new TestCloseListener(log);

    //Need this call to set respProcessor without triggering erroneous check
    final GenericHttpResponseHandler responseHandler = new GenericHttpResponseHandler(respProcessor,KeepAliveType.KEEP_ALIVE);
    responseHandler.setRequestListener(requestListener);
    responseHandler.setConnectionListener(connectListener);
    responseHandler.setCloseListener(closeListener);

    //use port 0 to generate connect fail
    ChannelFuture channelFuture = createChannelFuture(responseHandler,0);
    Channel channel = channelFuture.getChannel();

    try
    {
      channel.write(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"));
      final List<String> respCallbacks = respProcessor.getCallbacks();
      final List<String> connectCallbacks = connectListener.getCallbacks();
      final List<String> requestCallbacks = requestListener.getCallbacks();
      final List<String> closeChannelCallbacks = closeListener.getCallbacks();

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return 1 == closeChannelCallbacks.size();
        }
      }, "waiting for close channel callback", 1000, null);

      //make sure that no new callbacks have showed up
      stateSanityCheck(connectCallbacks,requestCallbacks,respCallbacks,closeChannelCallbacks);
      Assert.assertEquals(connectCallbacks.size(), 1);
      Assert.assertEquals(connectCallbacks.get(0),"onConnectFailure");
      Assert.assertEquals(closeChannelCallbacks.size(),1);
      Assert.assertEquals(closeChannelCallbacks.get(0),"onChannelClose");
    }
    finally
    {
      channel.close();
    }
  }

  /**
   *
   * @param connectCallbacks
   * @param requestCallbacks
   * @param respCallbacks
   * @param closeChannelCallbacks
   * Invariants are tested to see if valid sequence of callbacks were called
   */
  static void stateSanityCheck(List<String> connectCallbacks,List<String> requestCallbacks,List<String> respCallbacks
      ,List<String> closeChannelCallbacks)
  {
    System.out.println("connectCallbacks:" + connectCallbacks);
    System.out.println("requestCallbacks:" + requestCallbacks);
    System.out.println("responseCallbacks:" + respCallbacks);
    System.out.println("closeChannelCallbacks:" + closeChannelCallbacks);

    int totalConnectCallbacks = connectCallbacks.size();
    int totalRequestCallbacks = requestCallbacks.size();
    int totalRespCallbacks = respCallbacks.size();
    int totalCloseChannelCallbacks = closeChannelCallbacks.size();
    Assert.assertTrue((totalConnectCallbacks+totalRequestCallbacks+totalRespCallbacks)>0);
    if (!errorOccurred(respCallbacks))
    {
      //Response was received
      Assert.assertTrue((totalConnectCallbacks > 0) && (totalConnectCallbacks <= totalRequestCallbacks));
      //check if any exception state exists
    }
    else
    {
      //Some error happened; so close channel had to be called
      Assert.assertEquals(totalCloseChannelCallbacks,1);
    }
  }

  /**
   *
   * @param respCallbacks : callbacks in response callback object
   * @return false if a non-empty callback list with no callback that has substring 'Exception' was passed; true if the list was empty or a callback
   * which contained the string 'Exception'.
   */
  private static boolean errorOccurred(List<String> respCallbacks)
  {
    if (respCallbacks.size() > 0)
    {
      for (String callback: respCallbacks)
      {
        if (callback.contains("Exception"))
        {
          return true;
        }
      }
      return false;
    }
    return true;
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

  static Channel createClientBootstrap(GenericHttpResponseHandler responseHandler)
  {
    return createClientBootstrap(responseHandler, SERVER_ADDRESS_ID);
  }

  static ChannelFuture createChannelFuture(final GenericHttpResponseHandler responseHandler, int port)
  {

    ClientBootstrap client = new ClientBootstrap(
        new NioClientSocketChannelFactory(BOSS_POOL, IO_POOL));
    client.setPipelineFactory(new ChannelPipelineFactory()
    {
      @Override
      public ChannelPipeline getPipeline() throws Exception
      {
        return Channels.pipeline(new LoggingHandler(InternalLogLevel.DEBUG),
            new HttpClientCodec(), new LoggingHandler(InternalLogLevel.DEBUG),
            responseHandler);
      }
    });
    final ChannelFuture connectFuture = client.connect(new InetSocketAddress(
        "localhost", port));
    return connectFuture;
  }

  static Channel createClientBootstrap(final GenericHttpResponseHandler responseHandler,int port)
  {
    final ChannelFuture connectFuture = createChannelFuture(responseHandler, port);
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return (connectFuture.isDone() && connectFuture.isSuccess() && responseHandler.getMessageState()==MessageState.REQUEST_WAIT);
      }
    }, "waiting for connect success", 2000, null);

	  Assert.assertTrue(connectFuture.isDone() && connectFuture.isSuccess());
    return connectFuture.getChannel();
  }

  static GenericHttpResponseHandler getResponseHandler(Channel clientChannel)
  {
    ChannelPipeline pipe = clientChannel.getPipeline();
    ChannelHandler handler = pipe.get("3");
    Assert.assertNotNull(handler, "unable to find handler. Did client pipeline factory change?");
    Assert.assertTrue(handler instanceof GenericHttpResponseHandler,
                      "expected GenericHttpResponseHandler; found: " + handler.getClass() +
                      ". Did  client pipeline factory change?");
    return (GenericHttpResponseHandler)handler;
  }

}



class TestCloseListener implements ChannelCloseListener
{
  private final List<String> _callbacks = new ArrayList<String>();
  private final Logger _log;

  public List<String> getCallbacks()
  {
    return _callbacks;
  }

  public void clear()
  {
    _callbacks.clear();
  }

  public TestCloseListener(Logger log)
  {
    _log=log;
  }

  @Override
  public void onChannelClose()
  {
    if (null != _log)
    _log.info("onChannelClose");
    _callbacks.add("onChannelClose");
  }

}


class TestConnectListener implements ConnectResultListener
{
  private final List<String> _callbacks = new ArrayList<String>();
  private final Logger _log;


  public TestConnectListener(Logger log)
  {
    _log=log;
  }

  public List<String> getCallbacks()
  {
    return _callbacks;
  }

  public void clear()
  {
    _callbacks.clear();
  }

  @Override
  public void onConnectSuccess(Channel channel)
  {
    if (null != _log)
      _log.info("onConnectSuccess");
    _callbacks.add("onConnectSuccess");

  }

  @Override
  public void onConnectFailure(Throwable cause)
  {
    if (null != _log)
      _log.info("onConnectFailure");
    _callbacks.add("onConnectFailure");
  }
}

class TestSendRequestListener implements SendRequestResultListener
{

  private final List<String> _callbacks = new ArrayList<String>();
  private final Logger _log;

  public TestSendRequestListener(Logger log)
  {
    _log=log;
  }

  public List<String> getCallbacks()
  {
    return _callbacks;
  }

  public void clear()
  {
    _callbacks.clear();
  }

  @Override
  public void onSendRequestSuccess(HttpRequest req)
  {
    if (null != _log)
      _log.info("onSendRequestSuccess");
    _callbacks.add("onSendRequestSuccess");
  }

  @Override
  public void onSendRequestFailure(HttpRequest req, Throwable cause)
  {
    if (null != _log)
      _log.info("onSendRequestFailure");
    _callbacks.add("onSendRequestFailure");
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

  public void clearCallbacks()
  {
    _callbacks.clear();
  }

}
