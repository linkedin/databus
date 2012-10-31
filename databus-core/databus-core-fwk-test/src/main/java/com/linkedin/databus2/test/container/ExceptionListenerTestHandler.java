package com.linkedin.databus2.test.container;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/** Simple test helper class that detects the last exception in the channel */
public class ExceptionListenerTestHandler extends SimpleChannelHandler
{

  private volatile Throwable _lastException;

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
  {
    _lastException = e.getCause();
  }

  public Throwable getLastException()
  {
    return _lastException;
  }

  public void clearException()
  {
    _lastException = null;
  }

}
