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


import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.BufferInfoResponse;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.test.TestUtil;

public class TestClientChannelClose
{
	static
	{
	  TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestClientChannelClose_", ".log", Level.ERROR);
	}

	  public static enum MsgState
	  {
		  OPERATION_COMPLETE,
		  CHANNEL_EXCEPTION_DECORATOR,
		  CHANNEL_EXCEPTION_NO_DECORATOR,
		  MSG_ENQUEUED,
          REQUEST_FAILURE,
		  RESPONSE_FAILURE,
	  }

	  @Test
	  public void testTimeoutCase() throws Exception
	  {
	    int port = Utils.getAvailablePort(27781);
	    FakeRelay relay = new FakeRelay(port, 10, true , 100);
	    TestClientConnectionFactory factory = null;
	    try
	    {
	      relay.start();
	      List<MsgState> msgList = new ArrayList<MsgState>();
	      DatabusHttpClientImpl.Config conf = new DatabusHttpClientImpl.Config();
	      conf.getContainer().setReadTimeoutMs(54);
	      factory = new TestClientConnectionFactory(conf.build());
	      ServerInfo relayInfo = new ServerInfo("dummy", "dummy", new InetSocketAddress(InetAddress.getLocalHost(), port), "dummy");
	      for (int i = 0 ; i < 10 ; i++)
	      {
	          TestClientConnection conn = factory.createConnection(relayInfo, msgList);
	    	  conn.requestCall();
	    	  Thread.sleep(1000);
	    	  System.out.println("MsgList is :" + msgList);
	    	  //Assert.assertEquals(msgList.size(), 4, "Expected Size Check");
	    	  int numMsgEnqueued = 0;
	    	  for (MsgState s : msgList)
	    	  {
	    		  if (s == MsgState.MSG_ENQUEUED)
	    			  numMsgEnqueued++;
	    	  }
	    	  Assert.assertEquals(numMsgEnqueued, 1, "Expected NumEnqueued Msgs");
	    	  msgList.clear();
	      }
	    } finally {
	    	if ( null != relay )
	    		relay.shutdown();
	    	if (null != factory)
	    		factory.stop();
	    }
	  }


	  public static class TestClientConnectionFactory
	  {
		  private final ExecutorService _bossThreadPool;
		  private final ExecutorService _ioThreadPool;
		  private final Timer _timeoutTimer;
		  private final DatabusHttpClientImpl.StaticConfig _clientConfig;
		  private final ChannelFactory _channelFactory;
		  private final ChannelGroup _channelGroup; //provides automatic channel closure on shutdown

		  public TestClientConnectionFactory(
				  DatabusHttpClientImpl.StaticConfig clientConfig)
		  {
			  _bossThreadPool = Executors.newCachedThreadPool(new NamedThreadFactory("boss"));;
			  _ioThreadPool = Executors.newCachedThreadPool(new NamedThreadFactory("io"));;
			  _timeoutTimer = new HashedWheelTimer(5, TimeUnit.MILLISECONDS);
			  _clientConfig = clientConfig;
			  _channelFactory = new NioClientSocketChannelFactory(_bossThreadPool, _ioThreadPool);
			  _channelGroup = new DefaultChannelGroup();;
		  }

		  public TestClientConnection createConnection(ServerInfo relay, List<MsgState> msgList)
		  {
			  return new TestClientConnection(relay, new ClientBootstrap(_channelFactory), null, _timeoutTimer,
			                                  _clientConfig, _channelGroup, msgList,
			                                  15000, Logger.getLogger(TestClientConnectionFactory.class));
		  }

		  public void stop()
		  	throws InterruptedException
		  {
			  _channelGroup.close().await();
			  _timeoutTimer.stop();
		  }
	  }

	  public static class TestHttpResponseProcessor
	  	extends AbstractHttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
	  {
		private List<MsgState> msgList;
		private final ExtendedReadTimeoutHandler _readTimeOutHandler;

		public TestHttpResponseProcessor(List<MsgState> msgList, ExtendedReadTimeoutHandler readTimeOutHandler)
		{
			super(new ChunkedBodyReadableByteChannel());
			this.msgList = msgList;
			this._readTimeOutHandler = readTimeOutHandler;
		}

		  @Override
		  public void finishResponse() throws Exception
		  {
		    super.finishResponse();
		    System.out.println("finished response for /test");
		    if (null != _readTimeOutHandler) _readTimeOutHandler.stop();
		  }

		  @Override
		  public  void startResponse(HttpResponse response) throws Exception
		  {
			  LOG.info("Start Response !!");
			  try
			  {
				  Thread.sleep(200);
			  } catch (InterruptedException ie) {}
    	      super.startResponse(response);
    	      msgList.add(MsgState.MSG_ENQUEUED);
		  }

		@Override
		public void handleChannelException(Throwable cause)
		{

			if ((_responseStatus != ResponseStatus.CHUNKS_SEEN) &&
					(_responseStatus != ResponseStatus.CHUNKS_FINISHED))
			{
				LOG.info("CHannel Exception : Message Enqueued !!");
				msgList.add(MsgState.MSG_ENQUEUED);
			}


			if (null != _decorated)
			{
				System.out.println("Response Status is :" + _responseStatus);
				msgList.add(MsgState.CHANNEL_EXCEPTION_DECORATOR);
				_decorated.channelException(cause);
			}
			else
			{
				msgList.add(MsgState.CHANNEL_EXCEPTION_NO_DECORATOR);
				LOG.error("channel exception but no decorated object:" + cause.getClass() + ":" +
						cause.getMessage());
				if (LOG.isDebugEnabled()) LOG.error(cause);
			}
		}
	  }

	  public static class TestClientConnection extends AbstractNettyHttpConnection
	  {
		  private ExtendedReadTimeoutHandler _readTimeOutHandler;
		  private final List<MsgState> _msgList;

		  public TestClientConnection(ServerInfo relay,
                  ClientBootstrap bootstrap,
                  ContainerStatisticsCollector containerStatsCollector,
                  Timer timeoutTimer,
                  DatabusHttpClientImpl.StaticConfig clientConfig,
                  ChannelGroup channelGroup,
                  List<MsgState> msgList,
                  long timeoutMs,
                  Logger log)
		  {
			  super(relay, bootstrap, null, timeoutTimer, timeoutMs, timeoutMs, channelGroup, 1, log);
			  _msgList = msgList;
		  }

		  public void requestCall()
		  {
		    final TestClientConnection me = this;
		    if (null == _channel || ! _channel.isConnected())
		    {
		      connectWithListener(new ConnectResultListener()
              {
                @Override
                public void onConnectSuccess(Channel channel)
                {
                  me.onConnected();
                }

                @Override
                public void onConnectFailure(Throwable cause)
                {
                  _msgList.add(MsgState.REQUEST_FAILURE);
                }
              });
		    }
		    else
		    {
		      onConnected();
		    }
		  }

      void onConnected()
		  {
		    ChannelPipeline channelPipeline = _channel.getPipeline();
		    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
		    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

		    TestHttpResponseProcessor testResponseProcessor =
		        new TestHttpResponseProcessor(_msgList,_readTimeOutHandler);

            String uriString = "/test";
		    HttpRequest request = createEmptyRequest(uriString);

		    sendRequest(request, new AbstractNettyHttpConnection.SendRequestResultListener(){
            @Override
            public void onSendRequestSuccess(HttpRequest req)
            {
              _msgList.add(MsgState.REQUEST_FAILURE);
            }
            @Override
            public void onSendRequestFailure(HttpRequest req, Throwable cause)
            {
              onResponseFailure(cause);
            }
		    }, testResponseProcessor);
		  }

		  private void onResponseFailure(Throwable cause)
		  {
		  }
	  }

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

		@Override
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
			@Override
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
				System.out.println("Server : Request received");
				BufferInfoResponse bfResponse = new BufferInfoResponse();

				bfResponse.setMaxScn(_scn);
				bfResponse.setMinScn(_scn - 5000);
				bfResponse.setTimestampFirstEvent(System.currentTimeMillis() - 1000);
				bfResponse.setTimestampLatestEvent(System.currentTimeMillis());

				if(_timeout > 0)
				{
					//Thread.currentThread().sleep(_timeout);
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
		  		    try
		  		    {
		  		    	Thread.sleep(_timeout);
		  		    } catch (InterruptedException ie) {

		  		    }
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

}
