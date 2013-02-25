package com.linkedin.databus.client;
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


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

public class TestChunkedBodyReadableByteChannel
{
  public static final String MODULE = TestChunkedBodyReadableByteChannel.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final long ONE_MINUTE_IN_MS = 60000;

  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

  @Test
  public void testSmallNonChunkedRead()
  {
    ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    String body = "Hello Kitty!";

    HttpResponseReplayer responseReplayer = new HttpResponseReplayer(channel, body, null);
    SimpleStringChannelReader responseReader = new SimpleStringChannelReader(channel);

    Thread replayerThread = new Thread(responseReplayer);
    Thread readerThread = new Thread(responseReader);

    replayerThread.start();
    readerThread.start();

    boolean replayerDone = joinThreadWithExpoBackoff(replayerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(replayerDone);

    boolean readerDone = joinThreadWithExpoBackoff(readerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(readerDone);

    String response = responseReader.getResponse();
    Assert.assertEquals(response, body);
  }

  @Test
  public void testBigNonChunkedRead()
  {
    ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    StringBuilder megabyte = new StringBuilder(1000000);
    while (megabyte.length() < 1000000)
    {
      megabyte.append("TeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeSt");
    }

    StringBuilder body = new StringBuilder(11000000);
    for (int i = 0; i < 10; ++i)
    {
      body.append(megabyte);
    }

    HttpResponseReplayer responseReplayer = new HttpResponseReplayer(channel, body.toString(), null);
    SimpleStringChannelReader responseReader = new SimpleStringChannelReader(channel);

    Thread replayerThread = new Thread(responseReplayer);
    Thread readerThread = new Thread(responseReader);

    replayerThread.start();
    readerThread.start();

    boolean replayerDone = joinThreadWithExpoBackoff(replayerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(replayerDone);

    boolean readerDone = joinThreadWithExpoBackoff(readerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(readerDone);

    String response = responseReader.getResponse();
    Assert.assertEquals(body.toString(), response);
  }

  @Test
  public void testSmallChunkedRead()
  {
    ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    String chunk = "Hello Kitty!";

    HttpResponseReplayer responseReplayer =
        new HttpResponseReplayer(channel, null, new String[]{chunk, chunk, chunk});
    SimpleStringChannelReader responseReader = new SimpleStringChannelReader(channel);

    Thread replayerThread = new Thread(responseReplayer);
    Thread readerThread = new Thread(responseReader);

    replayerThread.start();
    readerThread.start();

    boolean replayerDone = joinThreadWithExpoBackoff(replayerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(replayerDone);

    boolean readerDone = joinThreadWithExpoBackoff(readerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(readerDone);

    String response = responseReader.getResponse();
    Assert.assertEquals(chunk + chunk + chunk, response);
  }

  @Test
  public void testBigChunkedRead()
  {
    ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    StringBuilder megabyte = new StringBuilder(1000000);
    while (megabyte.length() < 1000000)
    {
      megabyte.append("TeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeSt");
    }

    StringBuilder chunkBuilder = new StringBuilder(2200000);
    for (int i = 0; i < 2; ++i)
    {
      chunkBuilder.append(megabyte);
    }
    String chunk = chunkBuilder.toString();

    HttpResponseReplayer responseReplayer = new HttpResponseReplayer(channel, null, new String[]{chunk, chunk});
    SimpleStringChannelReader responseReader = new SimpleStringChannelReader(channel);

    Thread replayerThread = new Thread(responseReplayer);
    Thread readerThread = new Thread(responseReader);

    replayerThread.start();
    readerThread.start();

    boolean replayerDone = joinThreadWithExpoBackoff(replayerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(replayerDone);

    boolean readerDone = joinThreadWithExpoBackoff(readerThread, ONE_MINUTE_IN_MS);
    Assert.assertTrue(readerDone);

    String response = responseReader.getResponse();
    Assert.assertEquals(chunk + chunk, response);
  }

  @Test
  public void testExtraBigChunkedRead()
  {
    ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    StringBuilder megabyte = new StringBuilder(1000000);
    while (megabyte.length() < 1000000)
    {
      megabyte.append("TeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeSt");
    }

    StringBuilder chunkBuilder = new StringBuilder(5200000);
    for (int i = 0; i < 5; ++i)
    {
      chunkBuilder.append(megabyte);
    }
    String chunk = chunkBuilder.toString();
    String chunk2 = "Hello there.";

    HttpResponseReplayer responseReplayer = new HttpResponseReplayer(channel, null, new String[]{chunk2, chunk});
    SimpleStringChannelReader responseReader = new SimpleStringChannelReader(channel);

    Thread replayerThread = new Thread(responseReplayer, "replayer");
    Thread readerThread = new Thread(responseReader, "reader");

    replayerThread.start();
    readerThread.start();

    boolean replayerDone = joinThreadWithExpoBackoff(replayerThread, 10 * ONE_MINUTE_IN_MS);
    Assert.assertTrue(replayerDone);

    boolean readerDone = joinThreadWithExpoBackoff(readerThread, 10 * ONE_MINUTE_IN_MS);
    Assert.assertTrue(readerDone);

    String response = responseReader.getResponse();
    Assert.assertEquals(chunk2 + chunk, response);
  }

  @Test
  /** Block the writer because of running out of buffer space and check it times out eventually */
  public void testUnblockWriteOnClose()
  {
    ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    StringBuilder megabyte = new StringBuilder(1000000);
    while (megabyte.length() < 1000000)
    {
      megabyte.append("TeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeSt");
    }

    StringBuilder chunkBuilder = new StringBuilder(5200000);
    for (int i = 0; i < 5; ++i)
    {
      chunkBuilder.append(megabyte);
    }
    String chunk = chunkBuilder.toString();
    String chunk2 = "Hello there.";

    HttpResponseReplayer responseReplayer = new HttpResponseReplayer(channel, null, new String[]{chunk2, chunk});

    Thread replayerThread = new Thread(responseReplayer, "replayer");

    replayerThread.start();

    TestUtil.sleep(ChunkedBodyReadableByteChannel.MAX_CHUNK_SPACE_WAIT_MS / 2);
    Assert.assertTrue(replayerThread.isAlive());

    Assert.assertTrue(joinThreadWithExpoBackoff(replayerThread,
                                                ChunkedBodyReadableByteChannel.MAX_CHUNK_SPACE_WAIT_MS));
  }

  @Test
  /** make sure the reader does not hang if the channel is closed while it is reading. */
  public void testUnblockReadOnPrematureClose() throws IOException
  {
    final ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    StringBuilder kilobyte = new StringBuilder(1000);
    while (kilobyte.length() < 1000)
    {
      kilobyte.append("TeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeStTeSt");
    }

    final int chunkNum = 10000;
    String[] chunks = new String[chunkNum];
    for (int i = 0; i < chunkNum; ++i) chunks[i] = kilobyte.toString();

    HttpResponseReplayer responseReplayer = new HttpResponseReplayer(channel, null, chunks);

    Thread replayerThread = new Thread(responseReplayer);
    //a flag if the read is finished
    final AtomicBoolean out = new AtomicBoolean(false);

    //start a thread waiting for data on the channel
    final Thread readerThread = new Thread(new Runnable()
      {

        @Override
        public void run()
        {
          ByteBuffer tmp = ByteBuffer.allocate(10 * 1024 * 1024);
          try
          {
            channel.read(tmp);
            out.set(true);
          }
          catch (IOException ioe)
          {
            out.set(true);
          }
        }
      });
    readerThread.setDaemon(true);

    replayerThread.start();
    readerThread.start();

    TestUtil.sleep(5);

    channel.close();

    boolean replayerDone = joinThreadWithExpoBackoff(replayerThread, 30000);
    Assert.assertTrue(replayerDone);

    boolean readerDone = joinThreadWithExpoBackoff(readerThread, 30000);
    Assert.assertTrue(readerDone);

  }

  @Test
  public void testUnblockReadOnClose() throws Exception
  {
    final ChunkedBodyReadableByteChannel channel = new ChunkedBodyReadableByteChannel();

    //a flag if the read is finished
    final AtomicBoolean out = new AtomicBoolean(false);

    //start a thread waiting for data on the channel
    final Thread readerThread = new Thread(new Runnable()
      {

        @Override
        public void run()
        {
          ByteBuffer tmp = ByteBuffer.allocate(100);
          try
          {
            channel.read(tmp);
            out.set(true);
          }
          catch (IOException ioe)
          {
            out.set(true);
          }
        }
      });
    readerThread.setDaemon(true);
    readerThread.start();

    //Wait for the reader thread to start
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() { return readerThread.isAlive();}
    }, "reader thread started", 100, null);


    //Wait a bit to make sure we are blocked on the read call
    TestUtil.sleep(10);

    //Close the channel
    channel.close();

    //Expect the reader to unblock
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() { return out.get(); }
    }, "reader unblocked", 1000, null);

  }

  private static boolean joinThreadWithExpoBackoff(Thread thread, long totalTimeoutMs)
  {
    boolean success = false;
    long startTimeNano = System.nanoTime();
    long timeUsed = (System.nanoTime() - startTimeNano)/1000000;
    long exponentialTimeout = 1;

    try
    {
      while (!success && timeUsed < totalTimeoutMs)
      {
        long nextTimeout = Math.min(totalTimeoutMs - timeUsed, exponentialTimeout);
        thread.join(nextTimeout);
        success = !thread.isAlive();
        timeUsed = (System.nanoTime() - startTimeNano)/1000000;
        exponentialTimeout *= 2;
      }
    }
    catch (InterruptedException ie) {}


    return success;
  }

}

class SimpleStringChannelReader implements Runnable
{
  private final ReadableByteChannel _channel;
  private final BufferedReader _responseReader;
  private final StringBuffer _stringBuffer;

  public SimpleStringChannelReader(ReadableByteChannel channel)
  {
    _channel = channel;
    _responseReader = new BufferedReader(Channels.newReader(_channel, "UTF-8"));
    _stringBuffer = new StringBuffer();
  }

  @Override
  public void run()
  {
    try
    {
      String line = null;
      while (null != (line = _responseReader.readLine()))
      {
        _stringBuffer.append(line);
      }
    }
    catch (IOException ioe)
    {
      TestChunkedBodyReadableByteChannel.LOG.error("read error", ioe);
    }
  }

  public String getResponse()
  {
    return _stringBuffer.toString();
  }

}

class HttpResponseReplayer implements Runnable
{
  public static final Logger LOG = Logger.getLogger(HttpResponseReplayer.class);

  private final ChunkedBodyReadableByteChannel _channel;
  private final HttpResponse _response;
  private final HttpChunk[] _chunks;

  public HttpResponseReplayer(ChunkedBodyReadableByteChannel channel, String responseBody,
                              String[] responseChunks)
  {
    _channel = channel;
    _response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    if (null != responseBody && responseBody.length() > 0)
    {
      ChannelBuffer bodyBuffer = ChannelBuffers.wrappedBuffer(responseBody.getBytes());

      _response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, bodyBuffer.writableBytes());
      _response.setContent(bodyBuffer);
      _response.setChunked(false);
    }
    else
    {
      _response.setChunked(true);
      _response.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
    }

    if (null != responseChunks && responseChunks.length > 0)
    {
      _chunks = new HttpChunk[responseChunks.length];
      for (int i = 0; i < responseChunks.length; ++i)
      {
        _chunks[i] = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(responseChunks[i].getBytes()));
      }
    }
    else
    {
      _chunks = null;
    }
  }

  @Override
  public void run()
  {
    try
    {
      _channel.startResponse(_response);

      if (null != _chunks)
      {
        for (HttpChunk chunk: _chunks)
        {
          _channel.addChunk(chunk);
        }
      }

      _channel.addTrailer(HttpChunk.LAST_CHUNK);
    }
    catch (TimeoutException e)
    {
      LOG.error("timeout sending chunks", e);
    }
  }

}

