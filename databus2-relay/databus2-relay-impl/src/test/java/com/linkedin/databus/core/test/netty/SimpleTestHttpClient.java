package com.linkedin.databus.core.test.netty;

import static org.jboss.netty.channel.Channels.pipeline;
import static org.testng.AssertJUnit.assertTrue;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;

public class SimpleTestHttpClient
{
  public static enum TimeoutPolicy
  {
    NO_TIMEOUTS,
    CONNECT_TIMEOUT,
    SEND_TIMEOUT,
    ALL_TIMEOUTS
  }

  private SimpleHttpResponseHandler _responseHandler;
  private ClientBootstrap _clientBootstrap;
  private final TimeoutPolicy _timeoutPolicy;

  public SimpleTestHttpClient(ChannelFactory channelFactory, TimeoutPolicy timeoutPolicy) throws Exception
  {
    _clientBootstrap = new ClientBootstrap(channelFactory);
    ChannelPipeline pipeline = createPipeline();
    _clientBootstrap.setPipeline(pipeline);
    _timeoutPolicy = timeoutPolicy;
  }

  public static SimpleTestHttpClient createLocal(TimeoutPolicy timeoutPolicy) throws Exception
  {
    return new SimpleTestHttpClient(new DefaultLocalClientChannelFactory(), timeoutPolicy);
  }

  public SimpleHttpResponseHandler sendRequest(SocketAddress serverAddress, HttpRequest request)
                                               throws Exception
  {
    ChannelFuture connectFuture = _clientBootstrap.connect(serverAddress);
    if (_timeoutPolicy == TimeoutPolicy.CONNECT_TIMEOUT ||
        _timeoutPolicy == TimeoutPolicy.ALL_TIMEOUTS)
    {
      connectFuture.awaitUninterruptibly(1000, TimeUnit.SECONDS);
    }
    else
    {
      connectFuture.awaitUninterruptibly();
    }
    assertTrue("connect succeeded", connectFuture.isSuccess());

    Channel requestChannel = connectFuture.getChannel();
    ChannelFuture writeFuture = requestChannel.write(request);

    if (_timeoutPolicy == TimeoutPolicy.SEND_TIMEOUT ||
        _timeoutPolicy == TimeoutPolicy.ALL_TIMEOUTS)
    {
      writeFuture.awaitUninterruptibly(1000, TimeUnit.SECONDS);
    }
    else
    {
      writeFuture.awaitUninterruptibly();
    }
    assertTrue("send succeeded", writeFuture.isSuccess());

    return _responseHandler;
  }

  private ChannelPipeline createPipeline() throws Exception
  {
    ChannelPipeline clientPipeline = pipeline();

    clientPipeline.addLast("client logger 1", new LoggingHandler("client logger 1", InternalLogLevel.DEBUG, true));
    clientPipeline.addLast("codec", new HttpClientCodec());
    clientPipeline.addLast("aggregator", new FooterAwareHttpChunkAggregator(1000000));
    _responseHandler = new SimpleHttpResponseHandler();
    clientPipeline.addLast("handler", _responseHandler);
    clientPipeline.addLast("client logger 5", new LoggingHandler("client logger 5", InternalLogLevel.DEBUG, true));
    return clientPipeline;
  }

}
