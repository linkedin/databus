package com.linkedin.databus2.core.container;
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


import java.nio.channels.ClosedChannelException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.handler.timeout.WriteTimeoutException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.ExceptionListenerTestHandler;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestMessageReader;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestExtendedWriteTimeoutHandler
{
  static final long CONNECT_TIMEOUT_MS = 1000;

  @Test
  public void testClientSimpleRequestResponse()
  {
    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEventV1.byteOrder);
    srvConn.setPipelineFactory(new SimpleServerPipelineFactory());
    boolean serverStarted = srvConn.startSynchronously(101, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(serverStarted, "server started");

    final SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEventV1.byteOrder);
    clientConn.setPipelineFactory(new SimpleClientPipelineFactoryWithSleep(200));
    boolean clientConnected = clientConn.startSynchronously(101, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(clientConnected, "client connected");

    //hook in to key places in the server pipeline
    ChannelPipeline lastSrvConnPipeline = srvConn.getLastConnChannel().getPipeline();
    SimpleTestMessageReader srvMsgReader = (SimpleTestMessageReader)lastSrvConnPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler srvExceptionListener =
        (ExceptionListenerTestHandler)lastSrvConnPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //hook in to key places in the client pipeline
    ChannelPipeline clientPipeline = clientConn.getChannel().getPipeline();
    final ExceptionListenerTestHandler clientExceptionListener =
        (ExceptionListenerTestHandler)clientPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //System.err.println("Current thread: " + Thread.currentThread());

    //send a request in a separate thread because the client will intentionally block to simulate
    //a timeout
    final ChannelBuffer msg = ChannelBuffers.wrappedBuffer("hello".getBytes());
    Thread sendThread1 = new Thread(new Runnable()
        {
          @Override
          public void run()
          {
            //System.err.println(Thread.currentThread().toString() + ": sending message");
            clientConn.getChannel().write(msg);
          }
        }, "send msg thread");
    sendThread1.start();

    //wait for the request to propagate
    //System.err.println(Thread.currentThread().toString() + ": waiting for 10");
    try {Thread.sleep(50);} catch (InterruptedException ie){};
    //System.err.println(Thread.currentThread().toString() + ": done Waiting for 10");

    //make sure the server has not received the message
    Assert.assertNull(srvExceptionListener.getLastException(), "no server errors");
    Assert.assertNull(clientExceptionListener.getLastException(), "no errors yet");
    Assert.assertTrue(!"hello".equals(srvMsgReader.getMsg()), "message not read yet");

    //System.err.println("Waiting for 300");
    //wait for the write timeout
    try {Thread.sleep(300);} catch (InterruptedException ie){};
    //System.err.println("Done Waiting for 300");

    //the client should have timed out and closed the connection

    TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return null != clientExceptionListener.getLastException();
          }
        }, "client error", 1000, null);

    Assert.assertTrue(null != clientExceptionListener.getLastException(),
                      "client error");
    Assert.assertTrue(clientExceptionListener.getLastException() instanceof ClosedChannelException
                      || clientExceptionListener.getLastException() instanceof WriteTimeoutException,
                      "client error test");
    Assert.assertTrue(! lastSrvConnPipeline.getChannel().isConnected(), "client has disconnected");
    Assert.assertTrue(! clientPipeline.getChannel().isConnected(), "closed connection to server");

  }

}

class SleepingDownstreamHandler extends SimpleChannelDownstreamHandler
{
  private volatile int _sleepDuration;

  public SleepingDownstreamHandler(int sleepDuration)
  {
    _sleepDuration = sleepDuration;
  }

  public void setSleepDuration(int sleepDuration)
  {
    _sleepDuration = sleepDuration;
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    int curSleep = _sleepDuration;
    if (curSleep > 0)
    {
      long startTs = System.currentTimeMillis();
      long now;
      while ((now = System.currentTimeMillis()) - startTs < curSleep)
      {
        long t = curSleep - (now - startTs);
        //System.err.println(Thread.currentThread().toString() + ": sleeping for " + t);
        try
        {
          Thread.sleep(t);
        }
        catch (InterruptedException ie){}
      }
      //System.err.println(Thread.currentThread().toString() + ": done sleeping");
    }
    super.writeRequested(ctx, e);
  }

}

class SimpleClientPipelineFactoryWithSleep extends SimpleClientPipelineFactory
{
  private final int _defaultSleep;

  public SimpleClientPipelineFactoryWithSleep(int defaultSleep)
  {
    _defaultSleep = defaultSleep;
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline result = super.getPipeline();;

    result.addBefore(ExceptionListenerTestHandler.class.getSimpleName(),
                     ExtendedWriteTimeoutHandler.class.getSimpleName(),
                    new ExtendedWriteTimeoutHandler("client write timeout", null, 100, true));
    result.addFirst(SleepingDownstreamHandler.class.getName(),
                    new SleepingDownstreamHandler(_defaultSleep));

    return result;
  }

}
