package com.linkedin.databus2.core.container.request;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;

public interface IDatabusResponse
{
  public void writeToChannelAsBinary(ChannelHandlerContext ctx, ChannelFuture future);
  public String toJsonString(boolean pretty);
}
