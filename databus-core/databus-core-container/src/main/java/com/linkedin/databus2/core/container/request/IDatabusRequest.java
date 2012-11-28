package com.linkedin.databus2.core.container.request;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;

public interface IDatabusRequest
{

  public byte getProtocolVersion();
  public void sendDownstreamAsBinary(ChannelHandlerContext ctx, ChannelFuture future);
  public ChannelFuture writeToChannelAsBinary(Channel channel);
}
