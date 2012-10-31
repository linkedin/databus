package com.linkedin.databus2.test.container;

import java.nio.ByteOrder;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * A simple handler for testing purposes. It aggregates all ChannelBuffer responses into a single
 * channel buffer. */
public class SimpleResponseBytesAggregatorHandler extends SimpleChannelUpstreamHandler
{
  private final ChannelBuffer _buffer;

  public SimpleResponseBytesAggregatorHandler(ByteOrder byteOrder)
  {
    _buffer = new DynamicChannelBuffer(byteOrder, 1000);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    if (e.getMessage() instanceof ChannelBuffer)
    {
      ChannelBuffer buf = (ChannelBuffer)e.getMessage();
      _buffer.writeBytes(buf);
    }

    super.messageReceived(ctx, e);
  }

  public ChannelBuffer getBuffer()
  {
    return _buffer;
  }

  public void clear()
  {
    _buffer.clear();
  }

}
