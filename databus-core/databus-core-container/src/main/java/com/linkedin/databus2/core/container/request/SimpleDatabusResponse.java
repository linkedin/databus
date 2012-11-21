package com.linkedin.databus2.core.container.request;

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
