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


import static org.jboss.netty.channel.Channels.pipeline;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import com.linkedin.databus.core.test.netty.FooterAwareHttpChunkAggregator;
import com.linkedin.databus.core.test.netty.SimpleHttpResponseHandler;
import com.linkedin.databus2.core.container.netty.ChunkedBodyWritableByteChannel;

public class TestChunkedBodyWritableByteChannel
{
  public static final String MODULE = TestChunkedBodyWritableByteChannel.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private ServerBootstrap _serverBootstrap;
  private ClientBootstrap _clientBootstrap;
  private SimpleHttpResponseHandler _responseHandler;
  private LocalAddress _serverAddress;
  private Channel _serverChannel;
  private ServerThread _serverThread;
  private ClientThread _clientThread;

  static
  {
    BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%d{ISO8601} %c{2} [%p] %m%n"),
                                                    "System.err"));
    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Uncomment the following line to see the dumps of the data exchanges between the server and the
    //client
    //Logger.getRootLogger().setLevel(Level.DEBUG);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
  }

  @BeforeMethod
  public void setUp() throws Exception
  {
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    if (null != _clientThread)
    {
      _clientThread.stopClient();
      _clientThread.join();
    }
    if (null != _serverThread)
    {
      _serverThread.stopServer();
      _serverThread.join();
    }
    else
    {
      _serverChannel.close();
    }
    _serverBootstrap.releaseExternalResources();
    _clientBootstrap.releaseExternalResources();
  }

  @Test
  public void testWriteEmptyResponse()
  {
    LOG.info("Start: Testing empty response");

    setupClient();

    ArrayList<byte[]> chunks = new ArrayList<byte[]>();
    HashMap<String, String> headers = new HashMap<String, String>();
    HashMap<String, String> footers = new HashMap<String, String>();
    setupServer(HttpResponseStatus.OK,chunks, headers, footers);

    ChannelFuture connectFuture = _clientBootstrap.connect(_serverAddress);
    connectFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", connectFuture.isSuccess());

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/test");
    Channel requestChannel = connectFuture.getChannel();
    ChannelFuture writeFuture = requestChannel.write(request);

    writeFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", writeFuture.isSuccess());

    HttpResponse response = _responseHandler.getResponse();
    assertEquals("response code", HttpResponseStatus.OK, response.getStatus());

    byte[] responseBody = _responseHandler.getReceivedBytes();
    assertTrue("empty response", null == responseBody || responseBody.length == 0);
    LOG.info("Done: Testing empty response");
  }

  @Test
  public void testWriteHeadersEmptyBody()
  {
    LOG.info("Start: Testing headers with empty body");

    setupClient();

    ArrayList<byte[]> chunks = new ArrayList<byte[]>();
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");
    HashMap<String, String> footers = new HashMap<String, String>();
    setupServer(HttpResponseStatus.OK,chunks, headers, footers);

    ChannelFuture connectFuture = _clientBootstrap.connect(_serverAddress);
    connectFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", connectFuture.isSuccess());

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/test");
    Channel requestChannel = connectFuture.getChannel();
    ChannelFuture writeFuture = requestChannel.write(request);

    writeFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", writeFuture.isSuccess());

    HttpResponse response = _responseHandler.getResponse();
    assertEquals("response code", HttpResponseStatus.OK, response.getStatus());
    assertEquals("Checking header1 value", "value1", response.getHeader("header1"));
    assertEquals("Checking header2 value", "value2", response.getHeader("header2"));
    assertEquals("Checking header3 value", null, response.getHeader("header3"));

    byte[] responseBody = _responseHandler.getReceivedBytes();
    assertTrue("empty response", null == responseBody || responseBody.length == 0);
    LOG.info("Done: Testing headers with empty body");
  }

  @Test
  public void testWriteOneChunk()
  {
    LOG.info("Start: Testing headers with one chunk");

    setupClient();

    String chunk1 = "hello";
    ArrayList<byte[]> chunks = new ArrayList<byte[]>();
    chunks.add(chunk1.getBytes(Charset.defaultCharset()));

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");
    HashMap<String, String> footers = new HashMap<String, String>();
    setupServer(HttpResponseStatus.OK, chunks, headers, footers);

    ChannelFuture connectFuture = _clientBootstrap.connect(_serverAddress);
    connectFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", connectFuture.isSuccess());

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/test");
    Channel requestChannel = connectFuture.getChannel();
    ChannelFuture writeFuture = requestChannel.write(request);

    writeFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", writeFuture.isSuccess());

    HttpResponse response = _responseHandler.getResponse();
    assertEquals("response code", Integer.toString(HttpResponseStatus.OK.getCode()),
                 response.getHeader(ChunkedBodyWritableByteChannel.RESPONSE_CODE_FOOTER_NAME));
    assertEquals("Checking header1 value", "value1", response.getHeader("header1"));
    assertEquals("Checking header2 value", "value2", response.getHeader("header2"));

    byte[] responseBody = _responseHandler.getReceivedBytes();
    assertEquals("response length", chunk1.getBytes(Charset.defaultCharset()).length, responseBody.length);
    assertTrue("response content", Arrays.equals(chunk1.getBytes(Charset.defaultCharset()), responseBody));
    LOG.info("Done: Testing headers with one chunk");
  }

  @Test
  public void testWriteTwoChunks()
  {
    LOG.info("Start: Testing headers with one chunk");

    setupClient();

    String chunk1 = "hello";
    String chunk2 = "bye";
    ArrayList<byte[]> chunks = new ArrayList<byte[]>();
    chunks.add(chunk1.getBytes(Charset.defaultCharset()));
    chunks.add(chunk2.getBytes(Charset.defaultCharset()));

    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");
    HashMap<String, String> footers = new HashMap<String, String>();
    footers.put("footer1", "1value");
    footers.put("footer2", "2value");
    setupServer(HttpResponseStatus.OK, chunks, headers, footers);

    ChannelFuture connectFuture = _clientBootstrap.connect(_serverAddress);
    connectFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", connectFuture.isSuccess());

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/test");
    Channel requestChannel = connectFuture.getChannel();
    ChannelFuture writeFuture = requestChannel.write(request);

    writeFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", writeFuture.isSuccess());

    HttpResponse response = _responseHandler.getResponse();
    assertEquals("response code", Integer.toString(HttpResponseStatus.OK.getCode()),
                 response.getHeader(ChunkedBodyWritableByteChannel.RESPONSE_CODE_FOOTER_NAME));
    assertEquals("Checking header1 value", "value1", response.getHeader("header1"));
    assertEquals("Checking header2 value", "value2", response.getHeader("header2"));
    assertEquals("Checking footer1 value", "1value", response.getHeader("footer1"));
    assertEquals("Checking footer2 value", "2value", response.getHeader("footer2"));

    byte[] responseBody = _responseHandler.getReceivedBytes();
    assertEquals("response length", chunk1.getBytes(Charset.defaultCharset()).length + chunk2.getBytes(Charset.defaultCharset()).length,
                 responseBody.length);
    byte[] fullBody = new byte[chunk1.getBytes(Charset.defaultCharset()).length + chunk2.getBytes(Charset.defaultCharset()).length];
    System.arraycopy(chunk1.getBytes(Charset.defaultCharset()), 0, fullBody, 0, chunk1.getBytes(Charset.defaultCharset()).length);
    System.arraycopy(chunk2.getBytes(Charset.defaultCharset()), 0, fullBody, chunk1.getBytes(Charset.defaultCharset()).length, chunk2.getBytes(Charset.defaultCharset()).length);
    assertTrue("response content", Arrays.equals(fullBody, responseBody));
    LOG.info("Done: Testing headers with one chunk");
  }

  @Test
  public void testSetResponseCode()
  {
    LOG.info("Start: Testing response code with headers with empty body");

    setupClient();

    ArrayList<byte[]> chunks = new ArrayList<byte[]>();
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");
    HashMap<String, String> footers = new HashMap<String, String>();
    setupServer(HttpResponseStatus.BAD_GATEWAY, chunks, headers, footers);

    ChannelFuture connectFuture = _clientBootstrap.connect(_serverAddress);
    connectFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", connectFuture.isSuccess());

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/test");
    Channel requestChannel = connectFuture.getChannel();
    ChannelFuture writeFuture = requestChannel.write(request);

    writeFuture.awaitUninterruptibly(1, TimeUnit.SECONDS);
    assertTrue("connect succeeded", writeFuture.isSuccess());

    HttpResponse response = _responseHandler.getResponse();
    assertEquals("response code", HttpResponseStatus.BAD_GATEWAY, response.getStatus());
    assertEquals("Checking header1 value", "value1", response.getHeader("header1"));
    assertEquals("Checking header2 value", "value2", response.getHeader("header2"));
    assertEquals("Checking header3 value", null, response.getHeader("header3"));

    byte[] responseBody = _responseHandler.getReceivedBytes();
    assertTrue("empty response", null == responseBody || responseBody.length == 0);
    LOG.info("Done: Testing response code with headers with empty body");
  }

  private void setupServerThread(HttpResponseStatus responseCode, List<byte[]> responseChunks,
                                 Map<String, String> headers,
                                 Map<String, String> footers)
  {
    DummyHttpRequestHandler requestHandler = new DummyHttpRequestHandler(responseCode, responseChunks,
                                                                         headers, footers);
    _serverThread = new ServerThread(requestHandler);
    _serverThread.start();
  }

  private void setupServer(HttpResponseStatus responseCode, List<byte[]> responseChunks, Map<String, String> headers,
                           Map<String, String> footers)
  {
    DummyHttpRequestHandler requestHandler = new DummyHttpRequestHandler(responseCode, responseChunks, headers, footers);
    setupServer(requestHandler);
  }

  private void setupServer(DummyHttpRequestHandler requestHandler)
  {
    _serverBootstrap = new ServerBootstrap(new DefaultLocalServerChannelFactory());
    ChannelPipeline serverPipeline = pipeline();
    serverPipeline.addLast("server logger 1", new LoggingHandler("server logger 1", InternalLogLevel.DEBUG, true));
    serverPipeline.addLast("decoder", new HttpRequestDecoder());
    serverPipeline.addLast("encoder", new HttpResponseEncoder());
    serverPipeline.addLast("server loggger 5", new LoggingHandler("server logger 5", InternalLogLevel.DEBUG, true));
    serverPipeline.addLast("handler", requestHandler);
    _serverBootstrap.setPipeline(serverPipeline);

    _serverAddress = new LocalAddress(1);
    _serverChannel = _serverBootstrap.bind(_serverAddress);
  }

  private void setupClient()
  {
    _clientBootstrap = new ClientBootstrap(new DefaultLocalClientChannelFactory());

    _clientBootstrap.setPipelineFactory(new ChannelPipelineFactory()
    {
      @Override
      public ChannelPipeline getPipeline() throws Exception
      {
        ChannelPipeline clientPipeline = pipeline();

        clientPipeline.addLast("client logger 1", new LoggingHandler("client logger 1", InternalLogLevel.DEBUG, true));
        clientPipeline.addLast("codec", new HttpClientCodec());
        clientPipeline.addLast("aggregator", new FooterAwareHttpChunkAggregator(1000000));
        _responseHandler = new SimpleHttpResponseHandler();
        clientPipeline.addLast("handler", _responseHandler);
        clientPipeline.addLast("client logger 5", new LoggingHandler("client logger 5", InternalLogLevel.DEBUG, true));
        return clientPipeline;
      }
    });
  }

  private void setupClientThread()
  {
    _clientThread = new ClientThread();
    _clientThread.start();
  }

  class ServerThread extends Thread
  {
    private AtomicBoolean _stop;

    public ServerThread(DummyHttpRequestHandler requestHandler)
    {
      super("server thread");
      setupServer(requestHandler);
    }

    public void stopServer()
    {
      _stop.set(true);
      _stop.notifyAll();
    }

    @Override
    public void start()
    {
      super.start();
    }

    @Override
    public void run()
    {
      LOG.info("Server thread started");
      _stop = new AtomicBoolean(false);

      while (! _stop.get())
      {
        try {_stop.wait();} catch (InterruptedException ie) {}
      }

      _serverChannel.close();
      LOG.info("Server thread stopped");
    }

  }

  class ClientThread extends Thread
  {
    private AtomicBoolean _stop;

    public ClientThread()
    {
      setupClient();
    }

    @Override
    public void start()
    {
      super.start();
    }

    public void stopClient()
    {
      _stop.set(true);
      _stop.notifyAll();
    }

    @Override
    public void run()
    {
      LOG.info("Client thread started");
      _stop = new AtomicBoolean(false);

      while (! _stop.get())
      {
        try {this.wait();} catch (InterruptedException ie) {}
      }

      LOG.info("Client thread stopped");
    }

  }

}

class DummyHttpRequestHandler extends SimpleChannelUpstreamHandler
{
  private final List<byte[]> _responseChunks;
  private final Map<String, String> _headers;
  private final Map<String, String> _footers;
  private final HttpResponseStatus _responseCode;

  public DummyHttpRequestHandler(HttpResponseStatus responseCode, List<byte[]> responseChunks,
                                 Map<String, String> headers,
                                 Map<String, String> footers)
  {
    _responseChunks = responseChunks;
    _headers = headers;
    _footers = footers;
    _responseCode = responseCode;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    //HttpRequest request = (HttpRequest) e.getMessage();
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

    ChunkedBodyWritableByteChannel writeChannel = new ChunkedBodyWritableByteChannel(e.getChannel(),
                                                                                     response);
    for (String key: _headers.keySet())
    {
      writeChannel.setMetadata(key, _headers.get(key));
    }

    for (byte[] chunk: _responseChunks)
    {
      writeChannel.write(ByteBuffer.wrap(chunk));
    }

    writeChannel.setResponseCode(_responseCode);

    for (String key: _footers.keySet())
    {
      writeChannel.setMetadata(key, _footers.get(key));
    }

    writeChannel.close();
  }

}


