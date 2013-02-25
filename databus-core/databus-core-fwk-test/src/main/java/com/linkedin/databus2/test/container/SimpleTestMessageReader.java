package com.linkedin.databus2.test.container;
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


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/** Simple channel handler that records the last string it got over the channel. Meant for unit
 * tests. */
public class SimpleTestMessageReader extends SimpleChannelUpstreamHandler
{
  private String _msg;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    Object msgObj = e.getMessage();
    if (msgObj instanceof ChannelBuffer)
    {
      ChannelBuffer msgBuffer = (ChannelBuffer)msgObj;
      _msg = new String(msgBuffer.array());
    }
    super.messageReceived(ctx, e);
  }

  public String getMsg()
  {
    return _msg;
  }

}
