package com.linkedin.databus.client.netty;
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
import java.util.concurrent.ExecutorService;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.Timer;

import com.linkedin.databus.client.DatabusBootstrapConnection;
import com.linkedin.databus.client.DatabusBootstrapConnectionFactory;
import com.linkedin.databus.client.DatabusRelayConnection;
import com.linkedin.databus.client.DatabusRelayConnectionFactory;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;

public class NettyHttpConnectionFactory
       implements DatabusRelayConnectionFactory,
                  DatabusBootstrapConnectionFactory
{

  private final ExecutorService _bossThreadPool;
  private final ExecutorService _ioThreadPool;
  private final ContainerStatisticsCollector _containerStatsCollector;
  private final Timer _timeoutTimer;
  private final ChannelFactory _channelFactory;
  private final long _writeTimeoutMs;
  private final long _readTimeoutMs;
  private final long _bstReadTimeoutMs;
  private final int _protocolVersion;
  private final ChannelGroup _channelGroup; //provides automatic channel closure on shutdown
  private final int _maxEventVersion;

  public NettyHttpConnectionFactory(ExecutorService bossThreadPool,
                                    ExecutorService ioThreadPool,
                                    ContainerStatisticsCollector containerStatsCollector,
                                    Timer timeoutTimer,
                                    long writeTimeoutMs,
                                    long readTimeoutMs,
                                    long bstReadTimeoutMs,
                                    int protocolVersion,
                                    int maxEventVersion,
                                    ChannelGroup channelGroup)
  {
    super();
    _bossThreadPool = bossThreadPool;
    _ioThreadPool = ioThreadPool;
    _containerStatsCollector = containerStatsCollector;
    _timeoutTimer = timeoutTimer;
    _writeTimeoutMs = writeTimeoutMs;
    _readTimeoutMs = readTimeoutMs;
    _bstReadTimeoutMs = bstReadTimeoutMs;
    _protocolVersion = protocolVersion;
    _channelFactory = new NioClientSocketChannelFactory(_bossThreadPool, _ioThreadPool);
    _channelGroup = channelGroup;
    _maxEventVersion = maxEventVersion;
  }

  @Override
  public DatabusRelayConnection createRelayConnection(ServerInfo relay,
                                                      ActorMessageQueue callback,
                                                      RemoteExceptionHandler remoteExceptionHandler)
  throws IOException
  {
    return new NettyHttpDatabusRelayConnection(relay,
                                               callback,
                                               _channelFactory,
                                               _containerStatsCollector,
                                               remoteExceptionHandler,
                                               _timeoutTimer,
                                               _writeTimeoutMs,
                                               _readTimeoutMs,
                                               _protocolVersion,
                                               _maxEventVersion,
                                               _channelGroup);
  }

  @Override
  public DatabusBootstrapConnection createConnection(ServerInfo relay,
                                                     ActorMessageQueue callback,
                                                     RemoteExceptionHandler remoteExceptionHandler)
  throws IOException
  {
    return new NettyHttpDatabusBootstrapConnection(relay,
                                                   callback,
                                                   _channelFactory,
                                                   _containerStatsCollector,
                                                   remoteExceptionHandler,
                                                   _timeoutTimer,
                                                   _writeTimeoutMs,
                                                   _bstReadTimeoutMs,
                                                   _protocolVersion,  // not used?
                                                   _channelGroup);
  }

  ChannelFactory getChannelFactory()
  {
    return _channelFactory;
  }

}
