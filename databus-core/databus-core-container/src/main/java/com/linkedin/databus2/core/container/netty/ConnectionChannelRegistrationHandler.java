package com.linkedin.databus2.core.container.netty;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.WriteTimeoutException;

/**
 * A class that takes of registering a channel in a channel group so it can be closed as part of the
 * entire group.
 *
 * @author cbotev
 *
 */
public class ConnectionChannelRegistrationHandler extends SimpleChannelHandler
{
  public final String MODULE = ConnectionChannelRegistrationHandler.class.getName();
  public final Logger LOG = Logger.getLogger(MODULE);

  private final ChannelGroup _channelGroup;

  public ConnectionChannelRegistrationHandler(ChannelGroup channelGroup)
  {
    super();
    _channelGroup = channelGroup;
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    super.channelOpen(ctx, e);
    if (null != _channelGroup)
    {
      _channelGroup.add(ctx.getChannel());
      if (LOG.isDebugEnabled())
      {
        LOG.debug("channel registered: " + ctx.getChannel().getRemoteAddress());
      }
    }
    //LOG.debug("channel endianness:" + e.getChannel().getConfig().getBufferFactory().getDefaultOrder());
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("channel closed: " + ctx.getChannel().getRemoteAddress());
    }
    _channelGroup.remove(ctx.getChannel());
    super.channelClosed(ctx, e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
  {
    Throwable err = e.getCause();
    if (err instanceof OutOfMemoryError) _channelGroup.close(); //very bad

    boolean logFull = true;
    if (err instanceof IOException)
    {
      if (err instanceof ClosedChannelException)
      {
        //ignore
        logFull = false;
      }
      else if (null == err.getMessage())
      {
        logFull = true;
      }
      else if (err.getMessage().contains("Connection reset"))
      {
        //ignore
        logFull = false;
      }
    }
    else if (err instanceof WriteTimeoutException)
    {
      logFull = false;
    }
    else if (err instanceof ReadTimeoutException)
    {
      logFull = false;
    }

    if (logFull)
    {
      String message = err.getMessage();
      if (null == message) message = err.getClass().getSimpleName();
      LOG.error("Unhandled network exception for peer " + ctx.getChannel().getRemoteAddress() +
                ": " + message, err);
    }
    else if (LOG.isDebugEnabled())
    {
      String message = err.getMessage();
      if (null == message) message = err.getClass().getSimpleName();
      LOG.error("Unhandled network exception for peer " + ctx.getChannel().getRemoteAddress() +
                "): " + message);
    }

    if (ctx.getChannel().isOpen()) ctx.getChannel().close();
  }

  @Override
  public void disconnectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (LOG.isDebugEnabled()) LOG.debug("channel close requested:" + e.getChannel().getRemoteAddress());
    super.disconnectRequested(ctx, e);
  }

  @Override
  public void closeRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (LOG.isDebugEnabled()) LOG.debug("channel disconnect requested:" + e.getChannel().getRemoteAddress());
    super.closeRequested(ctx, e);
  }

}
