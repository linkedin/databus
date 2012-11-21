package com.linkedin.databus2.core.container.request;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DownstreamMessageEvent;

import com.linkedin.databus.core.util.JsonUtils;

public abstract class SimpleDatabusRequest implements IDatabusRequest
{
  protected final byte _protocolVersion;

  public SimpleDatabusRequest(byte protocolVersion)
  {
    _protocolVersion = protocolVersion;
  }

  public abstract ChannelBuffer toBinaryChannelBuffer();

  @Override
  public byte getProtocolVersion()
  {
    return _protocolVersion;
  }

  @Override
  public void sendDownstreamAsBinary(ChannelHandlerContext ctx, ChannelFuture future)
  {
    ChannelBuffer buffer = toBinaryChannelBuffer();
    ctx.sendDownstream(new DownstreamMessageEvent(ctx.getChannel(), future, buffer,
                                                  ctx.getChannel().getRemoteAddress()));
  }

  @Override
  public ChannelFuture writeToChannelAsBinary(Channel channel)
  {
    ChannelBuffer buffer = toBinaryChannelBuffer();
    return channel.write(buffer);
  }

  public String toJsonString(boolean pretty)
  {
    return JsonUtils.toJsonStringSilent(this, pretty);
  }

  @Override
  public String toString()
  {
    return toJsonString(false);
  }

}
