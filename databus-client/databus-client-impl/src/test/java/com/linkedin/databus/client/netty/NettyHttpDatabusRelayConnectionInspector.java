package com.linkedin.databus.client.netty;

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
}
