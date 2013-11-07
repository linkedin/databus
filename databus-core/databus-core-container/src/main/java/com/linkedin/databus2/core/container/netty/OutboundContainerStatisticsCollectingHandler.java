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


import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;

public class OutboundContainerStatisticsCollectingHandler extends SimpleChannelHandler
{
  public static final String MODULE = OutboundContainerStatisticsCollectingHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  //FIXME add a configurable property
  private static final int MAX_UNMERGED_EVENTS_NUM = 100;

  private final ContainerStatisticsCollector _statsCollector;
  private ContainerStatisticsCollector _connStatsCollector;
  private int _unmergedEventsNum;

  public OutboundContainerStatisticsCollectingHandler(ContainerStatisticsCollector statsCollector)
  {
    _statsCollector = statsCollector;
    _connStatsCollector = null;
  }

  private void mergeConnStats()
  {
    _statsCollector.merge(_connStatsCollector);
    _connStatsCollector.reset();
    _unmergedEventsNum = 0;
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (null != _statsCollector)
    {
      Object value = e.getValue();

      String client = null;

      if (value instanceof InetSocketAddress)
      {
        InetSocketAddress inetAddress = (InetSocketAddress)value;
        client = inetAddress.getAddress().isLoopbackAddress() ?
            "localhost" :
            inetAddress.getAddress().getHostAddress();
      }
      else
      {
        client = e.getValue().toString().replaceAll("[^0-9a-zA-Z_.-]", "_");
      }

      _connStatsCollector = _statsCollector.createForClientConnection(client);
      _connStatsCollector.registerOutboundConnectionOpen();
      _unmergedEventsNum = 1;
    }

    super.channelConnected(ctx, e);
  }

  @Override
  public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (null != _statsCollector)
    {
      if (null == _connStatsCollector)
      {
        _statsCollector.registerOutboundConnectionClose();
      }
      else
      {
        _connStatsCollector.registerOutboundConnectionClose();
        mergeConnStats();
      }
    }

    super.channelDisconnected(ctx, e);
  }

  @Override
  public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception
  {
    if (null != _statsCollector)
    {
      //FIXME change collector interface to take a long
      if (null == _connStatsCollector)
      {
        _statsCollector.addOutboundResponseSize((int)e.getWrittenAmount());
      }
      else
      {
        _connStatsCollector.addOutboundResponseSize((int)e.getWrittenAmount());

        if (_unmergedEventsNum >= MAX_UNMERGED_EVENTS_NUM)
        {
          mergeConnStats();
        }
        else
        {
          ++_unmergedEventsNum;
        }
      }
    }

    super.writeComplete(ctx, e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
  {
    if (null != _statsCollector)
    {
      if (null == _connStatsCollector) _statsCollector.registerOutboundConnectionError(e.getCause());
      else
      {
        _connStatsCollector.registerOutboundConnectionError(e.getCause());

        if (_unmergedEventsNum >= MAX_UNMERGED_EVENTS_NUM)
        {
          mergeConnStats();
        }
        else
        {
          ++_unmergedEventsNum;
        }
      }
    }
    super.exceptionCaught(ctx, e);
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (null != _connStatsCollector)
    {
      mergeConnStats();
      _connStatsCollector.unregisterMBeans();
      _connStatsCollector = null;
    }
    super.channelClosed(ctx, e);
  }

}
