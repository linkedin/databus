package com.linkedin.databus.core.test.netty;
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


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpResponse;


public class SimpleHttpResponseHandler extends SimpleChannelUpstreamHandler
{
  private byte[] _receivedBytes = null;
  private HttpResponse _response = null;
  private Lock _lock = new ReentrantLock();
  private Condition _hasResponseCondition = _lock.newCondition();
  private boolean _hasResponse = false;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    _response = (HttpResponse) e.getMessage();
    ChannelBuffer content = _response.getContent();
    _receivedBytes = new byte[content.readableBytes()];
    content.getBytes(0, _receivedBytes);
    _lock.lock();
    try
    {
      _hasResponse = true;
      _hasResponseCondition.signalAll();
    }
    finally
    {
      _lock.unlock();
    }
  }

  public void awaitResponseUninterruptedly()
  {
    _lock.lock();
    try
    {
      while (!_hasResponse) _hasResponseCondition.awaitUninterruptibly();
    }
    finally
    {
      _lock.unlock();
    }
  }

  public boolean awaitResponseUninterruptedly(long time, TimeUnit unit)
  {
    _lock.lock();
    try
    {
      boolean done = false;
      while (!done)
      {
        try
        {
          _hasResponseCondition.await(time, unit);
          done = true;
        }
        catch (InterruptedException ie) {}
      }

      return _hasResponse;
    }
    finally
    {
      _lock.unlock();
    }
  }

  public byte[] getReceivedBytes()
  {
    return _receivedBytes;
  }

  public HttpResponse getResponse()
  {
    return _response;
  }

}
