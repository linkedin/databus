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


import org.jboss.netty.channel.Channel;

/**
 * A help class that can let other classes inspect the internal state of a
 * {@link NettyHttpDatabusRelayConnection} instance. To be used for debugging purposes only.
 */
public class NettyHttpDatabusRelayConnectionInspector
{
  private final NettyHttpDatabusRelayConnection _conn;

  public NettyHttpDatabusRelayConnectionInspector(NettyHttpDatabusRelayConnection conn)
  {
    _conn = conn;
  }

  public Channel getChannel()
  {
    return _conn._channel;
  }
  public GenericHttpResponseHandler getHandler() {
    return _conn.getHandler();

  }
  public GenericHttpResponseHandler.MessageState getResponseHandlerMessageState() {
    return getHandler()._messageState;
  }
}
