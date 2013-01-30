package com.linkedin.databus2.core.container.request;
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
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;

import com.linkedin.databus.core.util.JsonUtils;

public abstract class SimpleDatabusResponse implements IDatabusResponse
{
  protected final byte _protocolVersion;

  public SimpleDatabusResponse(byte protocolVersion)
  {
    _protocolVersion = protocolVersion;
  }

  public abstract ChannelBuffer serializeToBinary();

  @Override
  public void writeToChannelAsBinary(ChannelHandlerContext ctx, ChannelFuture future)
  {
    ChannelFuture realFuture = (null != future) ? future : Channels.future(ctx.getChannel());

    ChannelBuffer serializedResponse = serializeToBinary();
    DownstreamMessageEvent e = new DownstreamMessageEvent(ctx.getChannel(), realFuture,
                                                          serializedResponse,
                                                          ctx.getChannel().getRemoteAddress());
    ctx.sendDownstream(e);
  }

  public byte getProtocolVersion()
  {
    return _protocolVersion;
  }

  @Override
  public String toJsonString(boolean pretty)
  {
    return JsonUtils.toJsonStringSilent(this, pretty);
  }

  @Override
  public String toString()
  {
	//default somewhat expensive implementation
    return toJsonString(false);
  }

}
