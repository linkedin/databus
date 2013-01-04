package com.linkedin.databus2.core.container.request;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;


public class SimpleBinaryDatabusResponseEncoder extends SimpleChannelDownstreamHandler
{

  @Override
  public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception
  {
    if (evt instanceof MessageEvent)
    {
      Object msg = ((MessageEvent)evt).getMessage();
      if (msg instanceof IDatabusResponse)
      {
        ((IDatabusResponse)msg).writeToChannelAsBinary(ctx, null);
      }
      else
      {
        super.handleDownstream(ctx, evt);
      }
    }
    else
    {
      super.handleDownstream(ctx, evt);
    }
  }

}
