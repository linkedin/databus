package com.linkedin.databus2.core.container.request.codec;
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


import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.request.SimpleDatabusRequest;

/**
 * Intercepts event messages with {@link SimpleDatabusRequest} payload and converts them to binary
 * representation */
public class SimpleBinaryDatabusRequestEncoder extends OneToOneEncoder
{
  private final ExtendedReadTimeoutHandler _readTimeoutHandler;

  /**
   * Constructor
   * @param readTimeoutHandler     the timeout handler waiting for a response from the server; if
   *                               null, no timeout will be enforced;
   */
  public SimpleBinaryDatabusRequestEncoder(ExtendedReadTimeoutHandler readTimeoutHandler)
  {
    _readTimeoutHandler = readTimeoutHandler;
  }

  @Override
  protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception
  {
    Object result = msg;

    if (msg instanceof SimpleDatabusRequest)
    {
      SimpleDatabusRequest req = (SimpleDatabusRequest)msg;
      result = req.toBinaryChannelBuffer();

      if (null != _readTimeoutHandler) _readTimeoutHandler.start(ctx);
    }

    return result;
  }

}
