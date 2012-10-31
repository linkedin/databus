package com.linkedin.databus3.espresso.client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult.ResultCode;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult.SummaryCode;
import com.linkedin.databus.core.BufferInfoResponse;
import com.linkedin.databus2.core.BackoffTimer;

public class RelayMaxSCNFinder implements ChannelFutureListener
{
  public static final String MODULE = RelayMaxSCNFinder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  final String _dbName;
  int _partitionId;

  DatabusServerCoordinates[] _serverCoordinates;
  CountDownLatch _latch;
  Map<Channel, DatabusServerCoordinates> _futureCoordinateMap;
  Map<DatabusServerCoordinates, BufferInfoResponse> _resultBufferInfoResponseMap;
  Map<DatabusServerCoordinates, ResultCode> _requestResultMap;

  ClientBootstrap _bootstrap;

  public RelayMaxSCNFinder(DatabusServerCoordinates[] serverCoordinates, String dbName, int partitionId)
  {
    _serverCoordinates = serverCoordinates;
    _dbName = dbName;
    _partitionId = partitionId;
  }

  public synchronized RelayFindMaxScnResultImpl getMaxSCNFromRelays(FetchMaxSCNRequest request) throws InterruptedException
  {
    RelayFindMaxScnResultImpl result = getMaxSCNFromRelaysInternal(request.getErrorRetry());
    while((result.getResultSummary() != SummaryCode.SUCCESS) &&
    		(request.getErrorRetry().getRemainingRetriesNum() > 0))
    {
      result = getMaxSCNFromRelaysInternal(request.getErrorRetry());
      if (result.getResultSummary() != SummaryCode.SUCCESS)
         request.getErrorRetry().backoffAndSleep();
    }
    return result;
  }

  RelayFindMaxScnResultImpl getMaxSCNFromRelaysInternal(BackoffTimer timer) throws InterruptedException
  {
    _futureCoordinateMap = new ConcurrentHashMap<Channel, DatabusServerCoordinates>();
    _latch = new CountDownLatch(_serverCoordinates.length);
    _resultBufferInfoResponseMap = new ConcurrentHashMap<DatabusServerCoordinates, BufferInfoResponse>();
    _requestResultMap = new ConcurrentHashMap<DatabusServerCoordinates, ResultCode>();
    _bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
    _bootstrap.setPipelineFactory(new HttpClientPipelineFactory());

    // Set default result
    for(int i = 0;i<_serverCoordinates.length; i++)
    {
      BufferInfoResponse invalidResponse = new BufferInfoResponse();
      _resultBufferInfoResponseMap.put(_serverCoordinates[i], invalidResponse);
      _requestResultMap.put(_serverCoordinates[i], ResultCode.TIMEOUT);
    }

    Set<ChannelFuture> futureSet = new HashSet<ChannelFuture>();

    // Start the async netty request
    for(int i = 0;i<_serverCoordinates.length; i++)
    {
      futureSet.add(startSendScnRequest(_serverCoordinates[i]));
    }

    // Wait for latch to become 0
    boolean timedOut = !_latch.await(timer.getCurrentSleepMs(), TimeUnit.MILLISECONDS);
    if(timedOut)
    {
      LOG.warn("getMaxSCNFromRelaysInternal timed out in " + timer.getCurrentSleepMs() +" MS");
    }

    for (ChannelFuture future : futureSet)
    {
      if(!future.isDone())
      {
        Channel channel = future.getChannel();
        if (null != channel && channel.isOpen())
        {
          LOG.warn("closing open channel FindMaxScn channel; maybe a timeout?: " + channel.getRemoteAddress());
          channel.close();
        }
        future.cancel();
      }
    }

    // This will call ChannelFactory.releaseExternalResources() which will close all
    // channels
    _bootstrap.releaseExternalResources();

    // Generate and return the result
    RelayFindMaxScnResultImpl result = new RelayFindMaxScnResultImpl();
    result.setResult(_requestResultMap, _resultBufferInfoResponseMap);
    return result;
  }

  ChannelFuture startSendScnRequest(DatabusServerCoordinates databusServerCoordinates)
  {
    InetSocketAddress relayAddress = databusServerCoordinates.getAddress();

    // Start the connection attempt.
    ChannelFuture future = _bootstrap.connect(relayAddress);
    _futureCoordinateMap.put(future.getChannel(), databusServerCoordinates);
    future.addListener(this);

    return future;
  }

  @Override
  public void operationComplete(ChannelFuture future)
  {
    if(future.isSuccess())
    {
      // Make up the request url for max SCN
      String url =  "/bufferInfo/inbound/" + _dbName + "/"+ _partitionId;

      try
      {
        LOG.info("Connected to " + future.getChannel().getRemoteAddress());

        // Send the request to the max SCN url
        HttpRequest request = new DefaultHttpRequest(
              HttpVersion.HTTP_1_1, HttpMethod.GET, new URI(url).toASCIIString());

        future.getChannel().write(request);
      }
      catch(Exception e)
      {
        LOG.error("Got exception while trying to send the request (" + url +").", e);
        _latch.countDown();
      }
    }
    else
    {
      LOG.error("Failed to connect to " + _futureCoordinateMap.get(future.getChannel()).getAddress().toString());
      DatabusServerCoordinates coordinate = _futureCoordinateMap.get(future.getChannel());
      _requestResultMap.put(coordinate, ResultCode.CANNOT_CONNECT);
      _latch.countDown();
    }
  }

  class HttpClientPipelineFactory implements ChannelPipelineFactory
  {
    public HttpClientPipelineFactory()
    {}

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
      ChannelPipeline pipeline = new DefaultChannelPipeline();
      pipeline.addLast("codec", new HttpClientCodec());
      pipeline.addLast("aggregator", new HttpChunkAggregator( 1024 * 1024));
      pipeline.addLast("responseHandler", new HttpResponseHandler());
      return pipeline;
    }
  }

  class HttpResponseHandler extends SimpleChannelUpstreamHandler
  {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event)
    throws Exception
    {
    	LOG.error("Exception caught during finding max scn" + event.getCause());
    	super.exceptionCaught(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
    {
      DatabusServerCoordinates coordinate = _futureCoordinateMap.get(ctx.getChannel());
      try
      {
        // Default to HTTP_ERROR
        _requestResultMap.put(coordinate, ResultCode.HTTP_ERROR);
        HttpResponse response = (HttpResponse) event.getMessage();
        if(response.getStatus().equals(HttpResponseStatus.OK))
        {
          ChannelBuffer content = response.getContent();
          if(content.readable())
          {
            String contentString = content.toString(CharsetUtil.UTF_8);
            InputStream is = new ByteArrayInputStream( contentString.getBytes() );
            ObjectMapper mapper = new ObjectMapper();

            BufferInfoResponse value =
                  mapper.readValue(is,BufferInfoResponse.class);
            // Parse the max SCN here, also set the result

            _resultBufferInfoResponseMap.put(coordinate, value);
            _requestResultMap.put(coordinate, ResultCode.SUCCESS);
            return;
          }
          else
          {
            LOG.error("content is not readable to" + ctx.getChannel().getRemoteAddress());
          }
        }
        else
        {
          LOG.error("Got Http status (" + response.getStatus() + ") from remote address :" + ctx.getChannel().getRemoteAddress());
        }
      }
      catch(Exception ex)
      {
        LOG.error("Got exception while handling Relay MaxSCN response :", ex);
      }
      finally
      {
        ctx.getChannel().close();
        _latch.countDown();
      }
    }
  }
}
