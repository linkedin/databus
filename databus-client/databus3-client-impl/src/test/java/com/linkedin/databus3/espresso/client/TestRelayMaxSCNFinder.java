package com.linkedin.databus3.espresso.client;

import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult.ResultCode;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult.SummaryCode;
import com.linkedin.databus.core.BufferInfoResponse;
import com.linkedin.databus.core.util.Utils;


@Test(singleThreaded=true)
public class TestRelayMaxSCNFinder
{
  public static class FakeRelay extends Thread
  {
    int _port;
    long _scn;
    boolean _returnSCN;
    ServerBootstrap _bootstrap;
    int _timeout;

    public FakeRelay(int port, long scn, boolean returnSCN, int timeout)
    {
      _port = port;
      _scn = scn;
      _returnSCN = returnSCN;
      _timeout = timeout;
    }

    public void run()
    {
      _bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

      // Set up the event pipeline factory.
      _bootstrap.setPipelineFactory(new HttpServerPipelineFactory());

      // Bind and start to accept incoming connections.
      _bootstrap.bind(new InetSocketAddress(_port));
    }

    void shutdown()
    {
      if(_bootstrap != null)
      {
        _bootstrap.releaseExternalResources();
      }
    }

    class HttpServerPipelineFactory implements ChannelPipelineFactory
    {
      public ChannelPipeline getPipeline() throws Exception
      {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = new DefaultChannelPipeline();
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new HttpRequestHandler());
        return pipeline;
      }
    }
    class HttpRequestHandler extends SimpleChannelUpstreamHandler
    {
      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception 
      {
        BufferInfoResponse bfResponse = new BufferInfoResponse();
        
        bfResponse.setMaxScn(_scn);
        bfResponse.setMinScn(_scn - 5000);
        bfResponse.setTimestampFirstEvent(System.currentTimeMillis() - 1000);
        bfResponse.setTimestampLatestEvent(System.currentTimeMillis());
        
        if(_timeout > 0)
        {
          Thread.currentThread().sleep(_timeout);
        }
        if(_returnSCN)
        {
          ObjectMapper mapper = new ObjectMapper();
          StringWriter sw = new StringWriter();
          mapper.writeValue(sw, bfResponse);
          
          HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          response.setContent(ChannelBuffers.copiedBuffer(sw.toString(), CharsetUtil.UTF_8));
          response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
          e.getChannel().write(response);
          e.getChannel().close();
        }
        
        else
        {
          HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
          response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
          e.getChannel().write(response);
          e.getChannel().close();
        }
      }
    }
  }

  @Test
  public void testNormalSCNFind() throws InterruptedException
  {
    int nFakeRelays = 5;
    DatabusServerCoordinates[] coords = new DatabusServerCoordinates[nFakeRelays];
    List<FakeRelay> relayList = new ArrayList<FakeRelay>();
    int prevPort = 27960;
    for(int i = 0; i < nFakeRelays; i++)
    {
      int port = Utils.getAvailablePort(prevPort + 1);
      FakeRelay relay = new FakeRelay(port, i, true, 0);
      relayList.add(relay);
      relay.start();
      coords[i] = new DatabusServerCoordinates(new InetSocketAddress(port), "some state");
      prevPort = port;
    }
    // wait the server to be online
    Thread.currentThread().sleep(500);
    RelayMaxSCNFinder finder = new RelayMaxSCNFinder(coords , "db", 1);
    
    RelayFindMaxScnResultImpl result = (RelayFindMaxScnResultImpl)finder.getMaxSCNFromRelays(new FetchMaxSCNRequest(5000,1));
    
    Assert.assertEquals(result._maxSCN.getSequence(), nFakeRelays - 1);
    Assert.assertEquals(result._minSCN.getSequence(), result._maxSCN.getSequence() - 5000);
    Assert.assertEquals(result._maxSCNRelaySet.size(), 1);
    Assert.assertTrue(result.getMaxScnRelays().contains(coords[nFakeRelays - 1]));
    Assert.assertEquals(result.getResultSummary(), SummaryCode.SUCCESS);
    Assert.assertEquals(result._resultBufferInfoResponseMap.size(), nFakeRelays);
    for(int i = 0; i < nFakeRelays; i++)
    {
      Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), i);
      Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), i-5000);
      Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.SUCCESS); 
    }
    for(FakeRelay relay : relayList)
    {
      relay.shutdown();
    }
  }
  
  @Test
  public void testPartitialSuccess() throws InterruptedException
  {
    int nFakeRelays = 5;
    DatabusServerCoordinates[] coords = new DatabusServerCoordinates[nFakeRelays];
    List<FakeRelay> relayList = new ArrayList<FakeRelay>();
    int prevPort = 27870;
    for(int i = 0; i < nFakeRelays; i++)
    {
      int port = Utils.getAvailablePort(prevPort + 1);;
      FakeRelay relay = new FakeRelay(port, i, (i % 2 == 1) , 0);
      relayList.add(relay);
      relay.start();
      coords[i] = new DatabusServerCoordinates(new InetSocketAddress(port), "some state");
      prevPort = port;
    }

    // wait the server to be online
    Thread.currentThread().sleep(500);
    RelayMaxSCNFinder finder = new RelayMaxSCNFinder(coords , "db", 1);
    RelayFindMaxScnResultImpl result = (RelayFindMaxScnResultImpl)finder.getMaxSCNFromRelays(new FetchMaxSCNRequest(5000,  1));
    
    Assert.assertEquals(result._maxSCN.getSequence(), nFakeRelays - 2);
    Assert.assertEquals(result._minSCN.getSequence(), result._maxSCN.getSequence() - 5000);
    Assert.assertEquals(result._maxSCNRelaySet.size(), 1);
    Assert.assertTrue(result.getMaxScnRelays().contains(coords[nFakeRelays - 2]));
    Assert.assertEquals(result.getResultSummary(), SummaryCode.PARTIAL_SUCCESS);
    Assert.assertEquals(result._resultBufferInfoResponseMap.size(), nFakeRelays);
    for(int i = 0; i < nFakeRelays; i++)
    {
      if(i % 2 ==1)
      {
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), i);
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), i-5000);
        Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.SUCCESS); 
      }
      else
      {
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), -1);
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), -1);
        Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.HTTP_ERROR); 
      }
    }
    for(FakeRelay relay : relayList)
    {
      relay.shutdown();
    }
  }
  
  @Test
  public void testPartitialTimeout() throws InterruptedException
  {
    int nFakeRelays = 5;
    DatabusServerCoordinates[] coords = new DatabusServerCoordinates[nFakeRelays];
    List<FakeRelay> relayList = new ArrayList<FakeRelay>();
    int prevPort = 27780;
    for(int i = 0; i < nFakeRelays; i++)
    {
      int port = Utils.getAvailablePort(prevPort + 1);
      FakeRelay relay = new FakeRelay(port, i, true , 1000 * ((i+1) % 2));
      relayList.add(relay);
      relay.start();
      coords[i] = new DatabusServerCoordinates(new InetSocketAddress(port), "some state");
      prevPort = port;
    }

    // wait the server to be online
    Thread.currentThread().sleep(500);
    RelayMaxSCNFinder finder = new RelayMaxSCNFinder(coords , "db", 1);
    RelayFindMaxScnResultImpl result
      = (RelayFindMaxScnResultImpl)finder.getMaxSCNFromRelays(new FetchMaxSCNRequest(500,1));
    
    Assert.assertEquals(result._maxSCN.getSequence(), nFakeRelays - 2);
    Assert.assertEquals(result._minSCN.getSequence(), result._maxSCN.getSequence() - 5000);
    Assert.assertEquals(result._maxSCNRelaySet.size(), 1);
    Assert.assertTrue(result.getMaxScnRelays().contains(coords[nFakeRelays - 2]));
    Assert.assertEquals(result.getResultSummary(), SummaryCode.PARTIAL_SUCCESS);
    Assert.assertEquals(result._resultBufferInfoResponseMap.size(), nFakeRelays);
    for(int i = 0; i < nFakeRelays; i++)
    {
      if(i % 2 ==1)
      {
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), i);
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), i-5000);
        Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.SUCCESS); 
      }
      else
      {
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), -1);
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), -1);
        Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.TIMEOUT); 
      }
    }
    for(FakeRelay relay : relayList)
    {
      relay.shutdown();
    }
  }
  

  @Test
  public void testPartitialCannotConnect() throws InterruptedException
  {
    int nFakeRelays = 5;
    DatabusServerCoordinates[] coords = new DatabusServerCoordinates[nFakeRelays];
    List<FakeRelay> relayList = new ArrayList<FakeRelay>();
    int prevPort = 27690;
    for(int i = 0; i < nFakeRelays; i++)
    {
      int port = Utils.getAvailablePort(prevPort+1);
      FakeRelay relay = new FakeRelay(port, i, true , 0);
      relayList.add(relay);
      if(i % 2 ==1)
      {
        relay.start();
      }
      coords[i] = new DatabusServerCoordinates(new InetSocketAddress(port), "some state");
      prevPort = port;
    }

    // wait the server to be online
    Thread.currentThread().sleep(500);
    RelayMaxSCNFinder finder = new RelayMaxSCNFinder(coords , "db", 1);
    RelayFindMaxScnResultImpl result
      = (RelayFindMaxScnResultImpl)finder.getMaxSCNFromRelays(new FetchMaxSCNRequest(500, 1));
    
    Assert.assertEquals(result._maxSCN.getSequence(), nFakeRelays - 2);
    Assert.assertEquals(result._maxSCNRelaySet.size(), 1);
    Assert.assertTrue(result.getMaxScnRelays().contains(coords[nFakeRelays - 2]));
    Assert.assertEquals(result.getResultSummary(), SummaryCode.PARTIAL_SUCCESS);
    Assert.assertEquals(result._resultBufferInfoResponseMap.size(), nFakeRelays);
    for(int i = 0; i < nFakeRelays; i++)
    {
      if(i % 2 ==1)
      {
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), i);
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), i-5000);
        Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.SUCCESS); 
      }
      else
      {
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMaxScn(), -1);
        Assert.assertEquals(result._resultBufferInfoResponseMap.get(coords[i]).getMinScn(), -1);
        Assert.assertEquals(result._requestResultMap.get(coords[i]), ResultCode.CANNOT_CONNECT); 
      }
    }
    for(FakeRelay relay : relayList)
    {
      relay.shutdown();
    }
  }
}
