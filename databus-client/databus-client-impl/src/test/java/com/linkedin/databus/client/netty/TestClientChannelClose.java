package com.linkedin.databus.client.netty;

import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
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
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
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
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.netty.HttpResponseProcessorDecorator.ResponseStatus;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.BufferInfoResponse;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;

public class TestClientChannelClose 
{
    static Logger myLogger = Logger.getRootLogger();  
    static Appender myAppender;  
    
	static
	{
		myLogger.setLevel(Level.OFF);              
		myAppender = new ConsoleAppender(new SimpleLayout());
		myLogger.addAppender(myAppender);  
	}
	
	  public static enum MsgState
	  {
		  OPERATION_COMPLETE,
		  CHANNEL_EXCEPTION_DECORATOR,
		  CHANNEL_EXCEPTION_NO_DECORATOR,
		  MSG_ENQUEUED,
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
	      TestClientConnection conn = factory.createConnection(relayInfo, msgList);
	      for (int i = 0 ; i < 10 ; i++)
	      {
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
			  return new TestClientConnection(relay, new ClientBootstrap(_channelFactory), null, _timeoutTimer, _clientConfig, _channelGroup, msgList);
		  }
		  		  
		  public void stop()
		  	throws InterruptedException
		  {
			  _channelGroup.close().await();
			  _timeoutTimer.stop();
		  }
	  }
	  	  
	  public static class TestHttpResponseProcessor
	  	extends HttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>	  
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
	  
	  public static class TestClientConnection
	  	implements ChannelFutureListener
	  {
		  private final ServerInfo _relay;
		  private final ClientBootstrap _bootstrap;
		  private final GenericHttpClientPipelineFactory _pipelineFactory;
		  private Channel _channel;
		  private ExtendedReadTimeoutHandler _readTimeOutHandler;
		  private final ChannelGroup _channelGroup; //provides automatic channels closure on shutdown
		  private final List<MsgState> _msgList;
		  		  
		  public static enum State
		  {
			  INITIAL,
			  CONNECTED,
			  REQUEST_SENT,
			  OTHER
		  };
		  
		  private State _state = State.INITIAL;
		  
		  public TestClientConnection(ServerInfo relay,
                  ClientBootstrap bootstrap,
                  ContainerStatisticsCollector containerStatsCollector,
                  Timer timeoutTimer,
                  DatabusHttpClientImpl.StaticConfig clientConfig,
                  ChannelGroup channelGroup,
                  List<MsgState> msgList)
		  {
			  super();
			  _relay = relay;
			  _bootstrap = bootstrap;
			  _channelGroup = channelGroup;			 
			  _bootstrap.setOption("connectTimeoutMillis", DatabusSourcesConnection.CONNECT_TIMEOUT_MS);

			  _pipelineFactory = new GenericHttpClientPipelineFactory(
					  null,
					  GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE,
					  containerStatsCollector,
					  timeoutTimer,
					  clientConfig.getContainer().getWriteTimeoutMs(),
					  clientConfig.getContainer().getReadTimeoutMs(),
					  channelGroup);
			  _bootstrap.setPipelineFactory(_pipelineFactory);
			  _channel = null;
			  _msgList = msgList;
		  }
	
		  public void requestCall()
		  {
		    if (null == _channel || ! _channel.isConnected())
		    {
		      connectRetry();
		    }
		    else
		    {
		      onConnectSuccess();
		    }
		  }
		  
		  void onConnectSuccess()
		  {
		    ChannelPipeline channelPipeline = _channel.getPipeline();
		    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
		    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

		    TestHttpResponseProcessor testResponseProcessor =
		        new TestHttpResponseProcessor(_msgList,_readTimeOutHandler);
		    
		    channelPipeline.replace(
		        "handler", "handler",
		        new GenericHttpResponseHandler(testResponseProcessor,
		                                       GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE));

		    String uriString = "/test";
		    _state = State.REQUEST_SENT;
		    System.out.println("Sending " + uriString.toString());

		    // Prepare the HTTP request.
		    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uriString);
		    request.setHeader(HttpHeaders.Names.HOST, _relay.getAddress().toString());
		    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

		    ChannelFuture future = _channel.write(request);
		    if (_channel.isConnected())
		    {
		      future.addListener(this);
		      System.out.println("Wrote /sources request");
		    }
		    else
		    {
		      onResponseFailure(new ClosedChannelException());
		      System.err.println("disconnect on /sources request");
		    }
		  }
		  
		  private void connectRetry()
		  {
		    System.out.println("connecting : " + _relay.toString());
		    _state = State.CONNECTED;
		    ChannelFuture future = _bootstrap.connect(_relay.getAddress());
		    future.addListener(this);
		  }
		  
		  @Override
		  public void operationComplete(ChannelFuture future) throws Exception 
		  {	
			  if (_state == State.CONNECTED)
			  {
				  _msgList.add(MsgState.OPERATION_COMPLETE);
				  _channel = future.getChannel();
				  onConnectSuccess();
			  }
		  }
		  
		  private void onResponseFailure(Throwable cause)
		  {
			  _msgList.add(MsgState.RESPONSE_FAILURE);
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
