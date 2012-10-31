package com.linkedin.databus.client.netty;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.util.Formatter;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.Timer;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusBootstrapConnection;
import com.linkedin.databus.client.DatabusBootstrapConnectionStateMessage;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.filter.DbusKeyFilter;

public class NettyHttpDatabusBootstrapConnection
             implements DatabusBootstrapConnection, ChannelFutureListener
{
  public static final String MODULE = NettyHttpDatabusBootstrapConnection.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static enum State
  {
    TARGET_SCN_REQUEST_CONNECT,
    TARGET_SCN_REQUEST_WRITE,
    START_SCN_REQUEST_CONNECT,
    START_SCN_REQUEST_WRITE,
    STREAM_REQUEST_CONNECT,
    STREAM_REQUEST_WRITE
  }

  private final ServerInfo _server;
  private final ActorMessageQueue _callback;
  private final ClientBootstrap _bootstrap;
  private final GenericHttpClientPipelineFactory _pipelineFactory;
  private DatabusBootstrapConnectionStateMessage _callbackStateReuse;
  private Channel _channel;
  private ExtendedReadTimeoutHandler _readTimeOutHandler;
  private State _curState;
  private int _connectRetriesLeft;
  private Checkpoint _checkpoint;
  private String _sourcesIdList;
  private String _sourcesNameList;
  private int _freeBufferSpace;
  private final RemoteExceptionHandler _remoteExceptionHandler;
  private DbusKeyFilter _filter;
  private int _version;
  private final ChannelGroup _channelGroup; //provides automatic channels closure on shutdown
  private GenericHttpResponseHandler _handler;

  @Override
  public void close()
  {
	 if ( (null != _channel) && _channel.isConnected() )
	 {
		 ChannelFuture res = _channel.close();
		 res.awaitUninterruptibly();
	 }
  }

  public NettyHttpDatabusBootstrapConnection(ServerInfo server,
                                         ActorMessageQueue callback,
                                         ClientBootstrap bootstrap,
                                         ContainerStatisticsCollector containerStatsCollector,
                                         RemoteExceptionHandler remoteExceptionHandler,
                                         Timer timeoutTimer,
                                         long writeTimeoutMs,
                                         long readTimeoutMs,
                                         int version,
                                         ChannelGroup channelGroup)
  {
    super();
    _server = server;
    _callback = callback;
    _bootstrap = bootstrap;
    _channelGroup = channelGroup;
    //FIXME Use a config for the retries and timeout (DDSDBUS-95)
    _bootstrap.setOption("connectTimeoutMillis", DatabusSourcesConnection.CONNECT_TIMEOUT_MS);

    _pipelineFactory = new GenericHttpClientPipelineFactory(
        null,
        GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE,
        containerStatsCollector,
        timeoutTimer,
        writeTimeoutMs,
        readTimeoutMs,
        channelGroup);
    _bootstrap.setPipelineFactory(_pipelineFactory);
    _remoteExceptionHandler = remoteExceptionHandler;
    _channel = null;
    _version = version;
  }


  public NettyHttpDatabusBootstrapConnection(ServerInfo relay,
                                             ActorMessageQueue callback,
                                             ChannelFactory channelFactory,
                                             ContainerStatisticsCollector containerStatsCollector,
                                             RemoteExceptionHandler remoteExceptionHandler,
                                             Timer timeoutTimer,
                                             long writeTimeoutMs,
                                             long readTimeoutMs,
                                             int version,
                                             ChannelGroup channelGroup)
  {
    this(relay,
         callback,
         new ClientBootstrap(channelFactory), containerStatsCollector, remoteExceptionHandler,
                             timeoutTimer,writeTimeoutMs,readTimeoutMs,version,channelGroup);
  }


  @Override
  public void requestTargetScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse)
  {
    _checkpoint = checkpoint;
    _callbackStateReuse = stateReuse;
    _handler = null;
    
    if (null == _channel || ! _channel.isConnected())
    {
      //we've lost our connection to the bootstrap server; it's best to signal an error to the
      //puller so that it can figure out how to switch to a different one
      onRequestFailure();
    }
    else
    {
      onTargetScnConnectSuccess();
    }
  }

  void onTargetScnConnectSuccess()
  {
    _curState = State.TARGET_SCN_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    BootstrapTargetScnHttpResponseProcessor sourcesResponseProcessor =
        new BootstrapTargetScnHttpResponseProcessor(_callback, _callbackStateReuse, _checkpoint,
                                                    _remoteExceptionHandler, _readTimeOutHandler);
    _handler = new GenericHttpResponseHandler(sourcesResponseProcessor,
            						GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE);
    
    channelPipeline.replace(
            "handler", "handler",_handler);

    StringBuilder uriString = new StringBuilder(1024);
    Formatter uriFmt = new Formatter(uriString);
    uriFmt.format("/targetSCN?source=%s", _checkpoint.getSnapshotSource());

    LOG.info("Sending " + uriString.toString());

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, uriString.toString());
    request.setHeader(HttpHeaders.Names.HOST, _server.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    if (_channel.isConnected())
    {
      ChannelFuture future = _channel.write(request);	
      future.addListener(this);
      LOG.debug("Wrote /targetScn request");
    }
    else
    {
      onResponseFailure(new ClosedChannelException());
      LOG.error("disconnect on /targetSCN request");
    }

  }

  @Override
  public void requestStartScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse, String sourceNamesList)
  {
    _checkpoint = checkpoint;
    _callbackStateReuse = stateReuse;
    _sourcesNameList = sourceNamesList;
    _handler = null;
    
    if (null == _channel || ! _channel.isConnected())
    {
      connect(State.START_SCN_REQUEST_CONNECT);
    }
    else
    {
      onStartScnConnectSuccess();
    }
  }

  void onStartScnConnectSuccess()
  {
    _curState = State.START_SCN_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    BootstrapStartScnHttpResponseProcessor sourcesResponseProcessor =
        new BootstrapStartScnHttpResponseProcessor(_callback, _callbackStateReuse, _checkpoint,
                                                   _remoteExceptionHandler,
                                                   _readTimeOutHandler);
    _handler = new GenericHttpResponseHandler(sourcesResponseProcessor,
            				GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE);
    
    channelPipeline.replace(
        "handler", "handler",_handler);
        

    StringBuilder uriString = new StringBuilder(1024);
    Formatter uriFmt = new Formatter(uriString);
    uriFmt.format("/startSCN?sources=%s&checkPoint=%s", _sourcesNameList, _checkpoint.toString());

    LOG.info("Sending " + uriString.toString());

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, uriString.toString());
    request.setHeader(HttpHeaders.Names.HOST, _server.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    if (_channel.isConnected())
    {
      ChannelFuture future = _channel.write(request);	
      future.addListener(this);
      LOG.debug("Wrote /startScn request");
    }
    else
    {
      onResponseFailure(new ClosedChannelException());
      LOG.error("disconnect on /startSCN request");
    }

  }

  @Override
  public void requestStream(String sourcesIdList, DbusKeyFilter filter,
		  					int freeBufferSpace, Checkpoint cp,
                            DatabusBootstrapConnectionStateMessage stateReuse)
  {
    _checkpoint = cp;
    _callbackStateReuse = stateReuse;
    _sourcesIdList = sourcesIdList;
    _freeBufferSpace = freeBufferSpace;
    _filter = filter;
    _handler = null;
    
    if (null == _channel || ! _channel.isConnected())
    {
      connect(State.STREAM_REQUEST_CONNECT);
    }
    else
    {
      onStreamConnectSuccess();
    }
  }

  void onStreamConnectSuccess()
  {
    _curState = State.STREAM_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    StreamHttpResponseProcessor streamResponseProcessor =
        new StreamHttpResponseProcessor(_callback, _callbackStateReuse, _readTimeOutHandler);
    _handler = new GenericHttpResponseHandler(streamResponseProcessor,
            								GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE);
    
    channelPipeline.replace(
        "handler", "handler",_handler);
        

    ObjectMapper objMapper = new ObjectMapper();
    String filterStr = null;

    if ( null != _filter)
    {
    	try
    	{
    		filterStr = objMapper.writeValueAsString(_filter);
    	} catch( Exception ex) {
    		LOG.error("Got exception while serializing filter. Filter was : " + _filter, ex);
    		onRequestFailure();
    	}
    }

    StringBuilder uriString = new StringBuilder(10240);
    Formatter uriFmt = new Formatter(uriString);

    if ( null != filterStr)
    {
    	uriFmt.format("/bootstrap?sources=%s&checkPoint=%s&output=binary&batchSize=%d&filter=%s",
                  _sourcesIdList, _checkpoint.toString(), _freeBufferSpace, filterStr);
    } else {
    	uriFmt.format("/bootstrap?sources=%s&checkPoint=%s&output=binary&batchSize=%d",
                _sourcesIdList, _checkpoint.toString(), _freeBufferSpace);
    }

    if (LOG.isDebugEnabled())
    {
      LOG.debug("Sending " + uriString.toString());
    }

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, uriString.toString());
    request.setHeader(HttpHeaders.Names.HOST, _server.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    // Send the HTTP request.   
    if (_channel.isConnected())
    {
      ChannelFuture future = _channel.write(request);	
      future.addListener(this);
      LOG.debug("Wrote /bootstrap request");
    }
    else
    {
      onResponseFailure(new ClosedChannelException());
      LOG.error("disconnect on /bootstrap request");
    }
  }


  private void connect(State connectState)
  {
    _curState = connectState;
    //FIXME Use a config for the retries and timeout (DDSDBUS-96)
    _connectRetriesLeft = DatabusSourcesConnection.MAX_CONNECT_RETRY_NUM;
    connectRetry();
  }

  private void connectRetry()
  {
    LOG.info("connecting : " + _server.toString());
    ChannelFuture future = _bootstrap.connect(_server.getAddress());
    future.addListener(this);
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception
  {
    switch (_curState)
    {
      case START_SCN_REQUEST_CONNECT:
      case TARGET_SCN_REQUEST_CONNECT:
      case STREAM_REQUEST_CONNECT:
      {
        onConnectComplete(future);
        break;
      }
      case START_SCN_REQUEST_WRITE:
      case TARGET_SCN_REQUEST_WRITE:
      case STREAM_REQUEST_WRITE:
      {
        onWriteComplete(future);
        break;
      }
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }
  }

  private void onConnectComplete(ChannelFuture future) throws Exception
  {
    if (future.isCancelled())
    {
      LOG.error("Connect cancelled");
      onRequestFailure();
    }
    else if (future.isSuccess())
    {
      onConnectSuccess(future.getChannel());
    }
    else if (_connectRetriesLeft > 0)
    {
      -- _connectRetriesLeft;
      LOG.warn("connect failed: retries left " + _connectRetriesLeft + ": " + _server.toString(),
               future.getCause());
      connectRetry();
    }
    else
    {
      LOG.error("connect failed; giving up: " + _server.toString() , future.getCause());
      onRequestFailure();
    }
  }

  private void onConnectSuccess(Channel channel) throws Exception
  {
    LOG.info("connected: " + _server.toString());
    _channel = channel;
    switch (_curState)
    {
      case START_SCN_REQUEST_CONNECT: onStartScnConnectSuccess(); break;
      case TARGET_SCN_REQUEST_CONNECT: onTargetScnConnectSuccess(); break;
      case STREAM_REQUEST_CONNECT: onStreamConnectSuccess(); break;
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }
  }

  private void onWriteComplete(ChannelFuture future) throws Exception
  {
    if (future.isCancelled())
    {
      LOG.error("Write cancelled");
      onRequestFailure();
    }
    else if (! future.isSuccess())
    {  
      LOG.error("write failed", future.getCause());  
      onRequestFailure();
    }
  }

  private void onRequestFailure()
  {
    switch (_curState)
    {
      case START_SCN_REQUEST_CONNECT:
      case START_SCN_REQUEST_WRITE:
        _callbackStateReuse.switchToStartScnRequestError(); break;
      case TARGET_SCN_REQUEST_CONNECT:
      case TARGET_SCN_REQUEST_WRITE:
        _callbackStateReuse.switchToTargetScnRequestError(); break;
      case STREAM_REQUEST_CONNECT:
      case STREAM_REQUEST_WRITE:
    	  _callbackStateReuse.switchToStreamRequestError(); break;
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }

    _callback.enqueueMessage(_callbackStateReuse);
  }

  private void onResponseFailure(Throwable cause)
  {
    switch (_curState)
    {
      case START_SCN_REQUEST_WRITE: _callbackStateReuse.switchToStartScnResponseError(); break;
      case TARGET_SCN_REQUEST_WRITE: _callbackStateReuse.switchToTargetScnResponseError(); break;
      case STREAM_REQUEST_WRITE: _callbackStateReuse.switchToStreamResponseError(); break;
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }

    _callback.enqueueMessage(_callbackStateReuse);
  }

  @Override
  public int getVersion() {
	return _version;
  }

}

class BootstrapTargetScnHttpResponseProcessor
extends HttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
{
  public static final String MODULE = BootstrapTargetScnHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusBootstrapConnectionStateMessage _stateReuse;
  private final Checkpoint _checkpoint;
  private final RemoteExceptionHandler _remoteExceptionHandler;
  private final ExtendedReadTimeoutHandler _readTimeOutHandler;

  public BootstrapTargetScnHttpResponseProcessor(ActorMessageQueue bootstrapPullThread,
                                                 DatabusBootstrapConnectionStateMessage readStartScnState,
                                                 Checkpoint checkpoint,
                                                 RemoteExceptionHandler remoteExceptionHandler,
                                                 ExtendedReadTimeoutHandler readTimeoutHandler)
  {
    super(null);
    _callback = bootstrapPullThread;
    _stateReuse = readStartScnState;
    _checkpoint = checkpoint;
    _remoteExceptionHandler = remoteExceptionHandler;
    _readTimeOutHandler = readTimeoutHandler;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();

	if (_errorHandled)
	{
	    if (null != _readTimeOutHandler) _readTimeOutHandler.stop();
		return;
	}

    try
    {
      String exceptionName = RemoteExceptionHandler.getExceptionName(_decorated);
      Throwable remoteException = _remoteExceptionHandler.getException(_decorated);
      if (null != remoteException &&
          remoteException instanceof BootstrapDatabaseTooOldException)
      {
        _remoteExceptionHandler.handleException(remoteException);
      }
      else if (null != exceptionName)
      {
        LOG.error("/targetScn response error: " + RemoteExceptionHandler.getExceptionMessage(_decorated));
        _stateReuse.switchToTargetScnResponseError();
      }
      else
      {
        InputStream bodyStream = Channels.newInputStream(_decorated);
        ObjectMapper mapper = new ObjectMapper();
        String scnString = mapper.readValue(bodyStream, String.class);
        LOG.info("targetScn:" + scnString);

        long targetScn = Long.parseLong(scnString);

        _stateReuse.switchToTargetScnSuccess();

        // make sure we are in the expected mode
        Checkpoint ckpt = _checkpoint;
        if (ckpt.getConsumptionMode() != DbusClientMode.BOOTSTRAP_CATCHUP)
        {
          throw new RuntimeException("TargetScnResponseProcessor:"
                                     + " expecting in client mode: " + DbusClientMode.BOOTSTRAP_CATCHUP
                                     + " while in the incorrect mode: " + ckpt.getConsumptionMode());
        }

        LOG.info("Target SCN "
                 + targetScn
                 + " received for bootstrap catchup source "
                 + ckpt.getCatchupSource()
                 + " after completion of snapshot source "
                 + ckpt.getSnapshotSource());
        ckpt.setBootstrapTargetScn(targetScn);
      }
    }
    catch (Exception ex)
    {
      LOG.error("/targetScn response error:" + ex.getMessage(), ex);
      _stateReuse.switchToTargetScnResponseError();
    }
    finally
    {
      if (null != _readTimeOutHandler) _readTimeOutHandler.stop();
    }

    _callback.enqueueMessage(_stateReuse);
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
    _decorated = new ChunkedBodyReadableByteChannel();
    super.startResponse(response);
  }

  @Override
  public void handleChannelException(Throwable cause)
  {
    LOG.error("channel exception: " + cause.getMessage(), cause);
    if (_responseStatus != ResponseStatus.CHUNKS_FINISHED)
    {
    	LOG.info("Enqueueing TargetSCN Response Error State to Puller Queue");
    	_stateReuse.switchToTargetScnResponseError();
    	_callback.enqueueMessage(_stateReuse);
    } else {
    	LOG.info("Skipping Enqueueing TargetSCN Response Error State to Puller Queue");

    }
    super.handleChannelException(cause);
  }
}

class BootstrapStartScnHttpResponseProcessor
      extends HttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
{
  public static final String MODULE = BootstrapStartScnHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusBootstrapConnectionStateMessage _stateReuse;
  private final Checkpoint _checkpoint;
  private final RemoteExceptionHandler _remoteExceptionHandler;
  private final ExtendedReadTimeoutHandler _readTimeOutHandler;

  public BootstrapStartScnHttpResponseProcessor(ActorMessageQueue bootstrapPullThread,
                                                DatabusBootstrapConnectionStateMessage readStartScnState,
                                                Checkpoint checkpoint,
                                                RemoteExceptionHandler remoteExceptionHandler,
                                                ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(null);
    _callback = bootstrapPullThread;
    _stateReuse = readStartScnState;
    _checkpoint = checkpoint;
    _remoteExceptionHandler = remoteExceptionHandler;
    _readTimeOutHandler = readTimeOutHandler;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();

	if (_errorHandled)
	{
	    if (null != _readTimeOutHandler) _readTimeOutHandler.stop();
		return;
	}

    try
    {
      String exceptionName = RemoteExceptionHandler.getExceptionName(_decorated);
      Throwable remoteException = _remoteExceptionHandler.getException(_decorated);
      if (null != remoteException &&
          remoteException instanceof BootstrapDatabaseTooOldException)
      {
        _remoteExceptionHandler.handleException(remoteException);
      }
      else if (null != exceptionName)
      {
        LOG.error("/startScn response error: " + RemoteExceptionHandler.getExceptionMessage(_decorated));
        _stateReuse.switchToStartScnResponseError();
      }
      else
      {
        InputStream bodyStream = Channels.newInputStream(_decorated);

        ObjectMapper mapper = new ObjectMapper();
        String scnString = mapper.readValue(bodyStream, String.class);

        ServerInfo serverInfo  = null;
        String serverHostPort = _decorated.getMetadata(DbusConstants.SERVER_INFO_HOSTPORT_HEADER_PARAM);
        try
        {
          serverInfo = ServerInfo.buildServerInfoFromHostPort(serverHostPort, DbusConstants.HOSTPORT_DELIMITER);
        } catch(Exception ex) {
        	LOG.error("Unable to extract Boostrap Server info from StartSCN response. ServerInfo was :" + serverHostPort, ex);
        }

        LOG.info("Response startScn:" + scnString + ", from bootstrap Server :" + serverHostPort);
        long startScn = Long.parseLong(scnString);

        /*
         * No need to create a seperate BootstrapConnection as we are guaranteed to have a bootstrap Connection
         * at this point.
         */
        _stateReuse.switchToStartScnSuccess(_checkpoint, null, serverInfo);

        //Checkpoint ckpt = new Checkpoint();
        //ckpt.setInit(true);
        // TODO: add the following when the method is added in checkpoint (DDSDBUS-94)
        // ckpt.setAllBootstrapSources(_readStartScnState.getSourcesNames());
        //ckpt.setBootstrapStartScn(startScn);
        //ckpt.setBootstrapPhase(BootstrapPhase.BOOTSTRAP_PHASE_SNAPSHOT);
        //ckpt.setSnapshotSource(_readStartScnState.getSourcesNames().get(0));
        //ckpt.startSnapShotSource();

        // make sure we are in the expected mode
        Checkpoint ckpt = _checkpoint;
        if (ckpt.getConsumptionMode() != DbusClientMode.BOOTSTRAP_SNAPSHOT)
        {
          throw new RuntimeException("StartScnResponseProcessor:"
                                     + " expecting in client mode: "
                                     + DbusClientMode.BOOTSTRAP_SNAPSHOT
                                     + " while in the incorrect mode: "
                                     + ckpt.getConsumptionMode());
        }

        LOG.info("Start SCN "
                  + startScn
                  + " received for bootstrap snapshot source "
                  + ckpt.getSnapshotSource());
        ckpt.setBootstrapStartScn(startScn);
        ckpt.setBootstrapServerInfo(serverHostPort);
      }
    }
    catch (Exception ex)
    {
      LOG.error("Failed to process /startscn response", ex);
      _stateReuse.switchToStartScnResponseError();
    }
    finally
    {
      if (null != _readTimeOutHandler) _readTimeOutHandler.stop();
    }

    _callback.enqueueMessage(_stateReuse);
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
    _decorated = new ChunkedBodyReadableByteChannel();
    super.startResponse(response);
  }

  @Override
  public void handleChannelException(Throwable cause)
  {
    LOG.error("channel exception: " + cause.getMessage(), cause);
    if (_responseStatus != ResponseStatus.CHUNKS_FINISHED)
    {
    	LOG.info("Enqueueing StartSCN Response Error State to Puller Queue");
    	_stateReuse.switchToStartScnResponseError();
    	_callback.enqueueMessage(_stateReuse);
    } else {
    	LOG.info("Skipping Enqueueing StartSCN Response Error State to Puller Queue");
    }
    super.handleChannelException(cause);
  }
}
