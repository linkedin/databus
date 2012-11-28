package com.linkedin.databus.client.netty;

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
  private final int _version;
  private final ChannelGroup _channelGroup; //provides automatic channel closure on shutdown

  public NettyHttpConnectionFactory(ExecutorService bossThreadPool,
                                    ExecutorService ioThreadPool,
                                    ContainerStatisticsCollector containerStatsCollector,
                                    Timer timeoutTimer,
                                    long writeTimeoutMs,
                                    long readTimeoutMs,
                                    int version,
                                    ChannelGroup channelGroup)
  {
    super();
    _bossThreadPool = bossThreadPool;
    _ioThreadPool = ioThreadPool;
    _containerStatsCollector = containerStatsCollector;
    _timeoutTimer = timeoutTimer;
    _writeTimeoutMs = writeTimeoutMs;
    _readTimeoutMs = readTimeoutMs;
    _version = version;
    _channelFactory = new NioClientSocketChannelFactory(_bossThreadPool, _ioThreadPool);
    _channelGroup = channelGroup;
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
                                               _version,
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
                                                   _readTimeoutMs,
                                                   _version,
                                                   _channelGroup);
  }

  ChannelFactory getChannelFactory()
  {
    return _channelFactory;
  }

}
