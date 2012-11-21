package com.linkedin.databus2.core.container.request.codec;

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
