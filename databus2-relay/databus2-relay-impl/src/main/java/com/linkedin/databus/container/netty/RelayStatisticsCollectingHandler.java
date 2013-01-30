package com.linkedin.databus.container.netty;
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
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.netty.DatabusRequestExecutionHandler;
import com.linkedin.databus2.core.container.netty.HttpRequestHandler;
import com.linkedin.databus2.core.container.request.DatabusRequest;

/**
 * A class that listens for DatabusRequest messages sent over the pipeline and assigns connection-
 * specific relay stats collectors.
 *
 * The handler should be added to the pipeline before {@link DatabusRequestExecutionHandler} and
 * after {@link HttpRequestHandler}
 *
 * @author cbotev
 *
 */
public class RelayStatisticsCollectingHandler extends SimpleChannelHandler
{
  public static final String MODULE = RelayStatisticsCollectingHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final HttpRelay _relay;
  private DbusEventsStatisticsCollector _outEventStatsCollector;
  private DbusEventsStatisticsCollector _connOutEventStatsCollector;
  private HttpStatisticsCollector _outHttpStatsCollector;
  private HttpStatisticsCollector _connOutHttpStatsCollector;
  private DatabusRequest _latestDbusRequest = null;

  public RelayStatisticsCollectingHandler(HttpRelay relay)
  {
    _relay = relay;
    _outEventStatsCollector = _relay.getOutboundEventStatisticsCollector();
    _outHttpStatsCollector = _relay.getHttpStatisticsCollector();

    _connOutEventStatsCollector = null;
  }

  private boolean shouldMerge(MessageEvent me)
  {
    return ((me.getMessage() instanceof HttpChunkTrailer) ||
            (me.getMessage() instanceof HttpResponse));
  }

  private void mergePerConnStats()
  {
    if (null != _connOutEventStatsCollector)
    {
      _outEventStatsCollector.merge(_connOutEventStatsCollector);
      _connOutEventStatsCollector.reset();
    }

    if (null != _connOutHttpStatsCollector)
    {
      _outHttpStatsCollector.merge(_connOutHttpStatsCollector);
      _connOutHttpStatsCollector.reset();
    }
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (null != _outEventStatsCollector || null != _outHttpStatsCollector)
    {
      //Opening a new connection
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
        client = e.getValue().toString();
      }

      if (null != _outEventStatsCollector)
      {
        _connOutEventStatsCollector = _outEventStatsCollector.createForPeerConnection(client);
      }
      if (null != _outHttpStatsCollector)
      {
        _connOutHttpStatsCollector = _outHttpStatsCollector.createForClientConnection(client);
      }
    }

    super.channelConnected(ctx, e);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    if (null != _outEventStatsCollector || null != _outHttpStatsCollector )
    {
      if (e.getMessage() instanceof DatabusRequest)
      {
        _latestDbusRequest = (DatabusRequest)e.getMessage();
        if (null != _outEventStatsCollector)
        {
          _latestDbusRequest.getParams().put(_outEventStatsCollector.getName(),
                                             _connOutEventStatsCollector);
        }
        if (null != _outHttpStatsCollector)
        {
          _latestDbusRequest.getParams().put(_outHttpStatsCollector.getName(),
                                             _connOutHttpStatsCollector);
        }
      }
      else if (shouldMerge(e))
      {
        //First or Last message in a call
        mergePerConnStats();
      }
    }

    super.messageReceived(ctx, e);
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (null != _connOutEventStatsCollector || null != _connOutHttpStatsCollector)
    {
      mergePerConnStats();
      if (null != _connOutEventStatsCollector) _connOutEventStatsCollector.unregisterMBeans();
      if (null != _connOutHttpStatsCollector) _connOutHttpStatsCollector.unregisterMBeans();
      _connOutEventStatsCollector = null;
      _connOutHttpStatsCollector = null;
    }
    _latestDbusRequest = null;
    super.channelClosed(ctx, e);
  }

}
