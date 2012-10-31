package com.linkedin.databus2.tools.console;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.request.codec.SimpleBinaryDatabusRequestEncoder;

public class EspressoTcpClientConnection
{
  public static final int CONNECT_TIMEOUT_MS = 30000;
  public static final int RESPONSE_TIMEOUT_MS = 120000;

  private static final Timer TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
  private final ChannelGroup _channelGroup;
  private final ClientBootstrap _clientBootstrap;
  private String _serverHost = "N/A";
  private Integer _serverPort;
  private InetSocketAddress _serverAddress = new InetSocketAddress(8080);
  private Channel _clientChannel;

  class EspressoTcpClientChannelFactory implements ChannelPipelineFactory
  {

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
      ChannelPipeline pipeline = Channels.pipeline();
      ExtendedReadTimeoutHandler readTimeoutHandler =
          new ExtendedReadTimeoutHandler(_serverHost + ":" + _serverPort, TIMER, RESPONSE_TIMEOUT_MS,
                                         true);
      pipeline.addLast("response timeout handler", readTimeoutHandler);
      pipeline.addLast("request encoder", new SimpleBinaryDatabusRequestEncoder(readTimeoutHandler));

      return pipeline;
    }

  }

  public EspressoTcpClientConnection(ChannelGroup channelGroup)
  {
    _channelGroup = channelGroup;
    _clientBootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                          Executors.newCachedThreadPool()));
    _clientBootstrap.setOption("bufferFactory",
                               DirectChannelBufferFactory.getInstance(DbusEvent.byteOrder));
    _clientBootstrap.setOption("connectTimeoutMillis", CONNECT_TIMEOUT_MS);
    _clientBootstrap.setPipelineFactory(new EspressoTcpClientChannelFactory());
  }

  public boolean connect(String serverHost)
  {

    int colonIdx = serverHost.indexOf(':');
    _serverPort = (colonIdx >= 0) ? Integer.parseInt(serverHost.substring(colonIdx + 1)) : 9001;
    _serverHost = (colonIdx >= 0) ? serverHost.substring(0, colonIdx) : serverHost;
    _serverAddress = new InetSocketAddress(_serverHost, _serverPort);

    ChannelFuture connectFuture = _clientBootstrap.connect(_serverAddress);
    boolean success = connectFuture.awaitUninterruptibly(CONNECT_TIMEOUT_MS);
    _clientChannel = connectFuture.getChannel();
    if (null != _channelGroup & _clientChannel.isConnected()) _channelGroup.add(_clientChannel);

    return success;
  }

  public boolean disconnect()
  {
    if (null == _clientChannel) return true;
    boolean success = true;

    if (_clientChannel.isOpen())
    {
      ChannelFuture disconnFuture = _clientChannel.close();
      success = disconnFuture.awaitUninterruptibly(CONNECT_TIMEOUT_MS);
    }
    if (success && null != _channelGroup) _channelGroup.remove(_clientChannel);
    if (success) _clientChannel = null;

    return success;
  }

  public void cleanup()
  {
    disconnect();
    _clientBootstrap.releaseExternalResources();
  }

  public String getServerHost() { return _serverHost; }

  public Integer getServerPort() { return _serverPort; }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(100);
    sb.append((null != _clientChannel && _clientChannel.isOpen()) ? "connected" : "disconnected");
    sb.append(':');
    sb.append(_serverHost);
    sb.append(':');
    sb.append(_serverPort);
    sb.append('(');
    sb.append(_serverAddress.getAddress().getHostAddress());
    sb.append(')');

    return sb.toString();
  }
}
