package com.linkedin.databus2.test.container;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/** Simple channel handler that records the last string it got over the channel. Meant for unit
 * tests. */
public class SimpleTestMessageReader extends SimpleChannelUpstreamHandler
{
  private String _msg;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    Object msgObj = e.getMessage();
    if (msgObj instanceof ChannelBuffer)
    {
      ChannelBuffer msgBuffer = (ChannelBuffer)msgObj;
      _msg = new String(msgBuffer.array());
    }
    super.messageReceived(ctx, e);
  }

  public String getMsg()
  {
    return _msg;
  }

}
