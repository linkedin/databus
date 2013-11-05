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


import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Formatter;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.Timer;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusBootstrapConnection;
import com.linkedin.databus.client.DatabusBootstrapConnectionStateMessage;
import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.BaseHttpResponseProcessor;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.InvalidCheckpointException;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.filter.DbusKeyFilter;

public class NettyHttpDatabusBootstrapConnection
             extends AbstractNettyHttpConnection
             implements DatabusBootstrapConnection
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

  private final ActorMessageQueue _callback;
  private DatabusBootstrapConnectionStateMessage _callbackStateReuse;
  private ExtendedReadTimeoutHandler _readTimeOutHandler;
  private State _curState;
  private Checkpoint _checkpoint;
  private String _sourcesIdList;
  private String _sourcesNameList;
  private int _freeBufferSpace;
  private final RemoteExceptionHandler _remoteExceptionHandler;
  private DbusKeyFilter _filter;
  private GenericHttpResponseHandler _handler;
  //private MyConnectListener _connectListener;

  public NettyHttpDatabusBootstrapConnection(ServerInfo server,
                                         ActorMessageQueue callback,
                                         ClientBootstrap bootstrap,
                                         ContainerStatisticsCollector containerStatsCollector,
                                         RemoteExceptionHandler remoteExceptionHandler,
                                         Timer timeoutTimer,
                                         long writeTimeoutMs,
                                         long readTimeoutMs,
                                         int protocolVersion,
                                         ChannelGroup channelGroup)
  {
    super(server, bootstrap, containerStatsCollector, timeoutTimer, writeTimeoutMs, readTimeoutMs,
          channelGroup, protocolVersion, LOG);
    _callback = callback;
    _remoteExceptionHandler = remoteExceptionHandler;

    //_connectListener = new MyConnectListener();
    //setConnectListener(_connectListener);
  }


  public NettyHttpDatabusBootstrapConnection(ServerInfo relay,
                                             ActorMessageQueue callback,
                                             ChannelFactory channelFactory,
                                             ContainerStatisticsCollector containerStatsCollector,
                                             RemoteExceptionHandler remoteExceptionHandler,
                                             Timer timeoutTimer,
                                             long writeTimeoutMs,
                                             long readTimeoutMs,
                                             int protocolVersion,
                                             ChannelGroup channelGroup)
  {
    this(relay,
         callback,
         new ClientBootstrap(channelFactory),
         containerStatsCollector,
         remoteExceptionHandler,
         timeoutTimer,
         writeTimeoutMs,
         readTimeoutMs,
         protocolVersion,
         channelGroup);
  }


  @Override
  public void requestTargetScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse)
  {
    _checkpoint = checkpoint;
    _callbackStateReuse = stateReuse;
    _handler = null;

    if (null == _channel || ! _channel.isConnected())
    {
      connect(State.TARGET_SCN_REQUEST_CONNECT);
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

    BootstrapTargetScnHttpResponseProcessor targetResponseProcessor =
        new BootstrapTargetScnHttpResponseProcessor(this, _callback, _callbackStateReuse,
                                                    _checkpoint,
                                                    _remoteExceptionHandler, _readTimeOutHandler);

    final String url = createTargetScnRequestUrl();
    LOG.info("Sending " + url);

    // Prepare the HTTP request.
    HttpRequest request = createEmptyRequest(url);
    sendRequest(request, new TargetScnRequestResultListener(), targetResponseProcessor);
  }

  private String createTargetScnRequestUrl()
  {
    // Adding checkpoint to the targetSCN request for supporting V3 bootstrap. It is unused in case of V2 bootstrap
    return String.format("/targetSCN?source=%s&checkPoint=%s", _checkpoint.getSnapshotSource(), _checkpoint.toString());
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
        new BootstrapStartScnHttpResponseProcessor(this, _callback, _callbackStateReuse,
                                                   _checkpoint,
                                                   _remoteExceptionHandler,
                                                   _readTimeOutHandler);

    final String url = createStartScnRequestUrl();
    LOG.info("Sending " + url);

    // Prepare the HTTP request.
    HttpRequest request = createEmptyRequest(url);
    sendRequest(request, new StartScnRequestResultListener(), sourcesResponseProcessor);
  }

  private String createStartScnRequestUrl()
  {
    return String.format("/startSCN?sources=%s&checkPoint=%s", _sourcesNameList,
                         _checkpoint.toString());
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
        new StreamHttpResponseProcessor(this, _callback, _callbackStateReuse, _readTimeOutHandler);

    StringBuilder uriString = new StringBuilder(10240);
    boolean error = populateBootstrapRequestUrl(uriString);

    if (error)
    {
      return;
    }

    final String url = uriString.toString();
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Sending " + url);
    }

    // Prepare the HTTP request.
    HttpRequest request = createEmptyRequest(url);
    sendRequest(request, new BootstrapRequestResultListener(), streamResponseProcessor);
  }

  private boolean populateBootstrapRequestUrl(StringBuilder uriString)
  {
    boolean error = false;
    ObjectMapper objMapper = new ObjectMapper();
    String filterStr = null;

    if ( null != _filter)
    {
    	try
    	{
    		filterStr = objMapper.writeValueAsString(_filter);
    	} catch( Exception ex) {
    		LOG.error("Got exception while serializing filter. Filter was : " + _filter, ex);
    		error = true;
    		onRequestFailure(uriString.toString(), ex);
    	}
    }

    Formatter uriFmt = new Formatter(uriString);
    if ( null != filterStr)
    {
    	uriFmt.format("/bootstrap?sources=%s&checkPoint=%s&output=binary&batchSize=%d&filter=%s",
                  _sourcesIdList, _checkpoint.toString(), _freeBufferSpace, filterStr);
    } else {
    	uriFmt.format("/bootstrap?sources=%s&checkPoint=%s&output=binary&batchSize=%d",
                _sourcesIdList, _checkpoint.toString(), _freeBufferSpace);
    }
    uriFmt.close(); //make the compiler shut up

    return error;
  }


  private void connect(State connectState)
  {
    _curState = connectState;
    connectWithListener(new MyConnectListener());
  }

  private void onConnectSuccess(Channel channel)
  {
    switch (_curState)
    {
      case START_SCN_REQUEST_CONNECT: onStartScnConnectSuccess(); break;
      case TARGET_SCN_REQUEST_CONNECT: onTargetScnConnectSuccess(); break;
      case STREAM_REQUEST_CONNECT: onStreamConnectSuccess(); break;
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }
  }

  /**
   * TODO : This method is overridden to get the _handler local to this class and not the
   * parent class. Once cleanup is performed to use _handler from parent class, this method
   * must be removed
   */
  @Override
  protected GenericHttpResponseHandler getHandler()
  {
    return _handler;
  }

  private void onRequestFailure(HttpRequest req, Throwable cause)
  {
    onRequestFailure(null == req ? (String)null : req.getUri(), cause);
  }

  private void onRequestFailure(String req, Throwable cause)
  {
    LOG.info("request failure: req=" + req + " cause=" + cause);

    if(shouldIgnoreWriteTimeoutException(cause)) {
      LOG.error("got RequestFailure because of WriteTimeoutException");
      return;
    }
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

  private class MyConnectListener implements ConnectResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener#onConnectSuccess(org.jboss.netty.channel.Channel)
     */
    @Override
    public void onConnectSuccess(Channel channel)
    {
      NettyHttpDatabusBootstrapConnection.this.onConnectSuccess(channel);
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener#onConnectFailure(java.lang.Throwable)
     */
    @Override
    public void onConnectFailure(Throwable cause)
    {
      NettyHttpDatabusBootstrapConnection.this.onRequestFailure((String)null, cause);
    }
  }

  /** Callback for /startSCN request result */
  private class StartScnRequestResultListener implements SendRequestResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestSuccess(org.jboss.netty.handler.codec.http.HttpRequest)
     */
    @Override
    public void onSendRequestSuccess(HttpRequest req)
    {
      //Do nothing
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestFailure(org.jboss.netty.handler.codec.http.HttpRequest, java.lang.Throwable)
     */
    @Override
    public void onSendRequestFailure(HttpRequest req, Throwable cause)
    {
      //TODO eventually the onRequestFailure can be expanded here
      onRequestFailure(req, cause);
    }
  }

  /** Callback for /targetSCN request result */
  private class TargetScnRequestResultListener implements SendRequestResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestSuccess(org.jboss.netty.handler.codec.http.HttpRequest)
     */
    @Override
    public void onSendRequestSuccess(HttpRequest req)
    {
      //Do nothing
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestFailure(org.jboss.netty.handler.codec.http.HttpRequest, java.lang.Throwable)
     */
    @Override
    public void onSendRequestFailure(HttpRequest req, Throwable cause)
    {
      //TODO eventually the onRequestFailure can be expanded here
      onRequestFailure(req, cause);
    }
  }

  /** Callback for /bootstrap request result */
  private class BootstrapRequestResultListener implements SendRequestResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestSuccess(org.jboss.netty.handler.codec.http.HttpRequest)
     */
    @Override
    public void onSendRequestSuccess(HttpRequest req)
    {
      //Do nothing
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestFailure(org.jboss.netty.handler.codec.http.HttpRequest, java.lang.Throwable)
     */
    @Override
    public void onSendRequestFailure(HttpRequest req, Throwable cause)
    {
      //TODO eventually the onRequestFailure can be expanded here
      onRequestFailure(req, cause);
    }
  }
}

class BootstrapTargetScnHttpResponseProcessor extends BaseHttpResponseProcessor
{
  public static final String MODULE = BootstrapTargetScnHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusBootstrapConnectionStateMessage _stateReuse;
  private final Checkpoint _checkpoint;
  private final RemoteExceptionHandler _remoteExceptionHandler;

  /**
   * Constructor
   * @param parent                the AbstractNettyHttpConnection object that instantiated this
   *                              response processor
   * @param bootstrapPullThread   callback to send the processed response or errors to
   * @param readStartScnState     a message object to reuse for the callback
   *                              (TODO remove that: premature GC optimization)
   * @param readTimeOutHandler    the ReadTimeoutHandler for the connection handled by this
   *                              response handler.
   */
  public BootstrapTargetScnHttpResponseProcessor(AbstractNettyHttpConnection parent,
                                                 ActorMessageQueue bootstrapPullThread,
                                                 DatabusBootstrapConnectionStateMessage readStartScnState,
                                                 Checkpoint checkpoint,
                                                 RemoteExceptionHandler remoteExceptionHandler,
                                                 ExtendedReadTimeoutHandler readTimeoutHandler)
  {
    super(parent, readTimeoutHandler);
    _callback = bootstrapPullThread;
    _stateReuse = readStartScnState;
    _checkpoint = checkpoint;
    _remoteExceptionHandler = remoteExceptionHandler;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();

	if (_errorHandled)
	{
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

        // make sure we are in the expected mode -- sanity checks
        Checkpoint ckpt = _checkpoint;
        if (ckpt.getConsumptionMode() != DbusClientMode.BOOTSTRAP_SNAPSHOT)
        {
          throw new InvalidCheckpointException("TargetScnResponseProcessor:"
                                     + " expecting in client mode: " + DbusClientMode.BOOTSTRAP_SNAPSHOT,
                                     ckpt);
        }
        else if (! ckpt.isSnapShotSourceCompleted())
        {
          throw new InvalidCheckpointException("TargetScnResponseProcessor: current snapshot source not completed",
                                               ckpt);
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
    LOG.error("exception during /targetSCN response: " + cause, cause);
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

class BootstrapStartScnHttpResponseProcessor extends BaseHttpResponseProcessor
{
  public static final String MODULE = BootstrapStartScnHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusBootstrapConnectionStateMessage _stateReuse;
  private final Checkpoint _checkpoint;
  private final RemoteExceptionHandler _remoteExceptionHandler;

  /**
   * Constructor
   * @param parent                the AbstractNettyHttpConnection object that instantiated this
   *                              response processor
   * @param bootstrapPullThread   callback to send the processed response or errors to
   * @param readStartScnState     a message object to reuse for the callback
   *                              (TODO remove that: premature GC optimization)
   * @param readTimeOutHandler    the ReadTimeoutHandler for the connection handled by this
   *                              response handler.
   */
  public BootstrapStartScnHttpResponseProcessor(AbstractNettyHttpConnection parent,
                                                ActorMessageQueue bootstrapPullThread,
                                                DatabusBootstrapConnectionStateMessage readStartScnState,
                                                Checkpoint checkpoint,
                                                RemoteExceptionHandler remoteExceptionHandler,
                                                ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(parent, readTimeOutHandler);
    _callback = bootstrapPullThread;
    _stateReuse = readStartScnState;
    _checkpoint = checkpoint;
    _remoteExceptionHandler = remoteExceptionHandler;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();

	if (_errorHandled)
	{
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
        String hostHdr = DbusConstants.UNKNOWN_HOST;
        String svcHdr = DbusConstants.UNKNOWN_SERVICE_ID;

        if (null != getParent())
        {
          hostHdr = getParent().getRemoteHost();
          svcHdr = getParent().getRemoteService();
          LOG.info("initiated bootstrap sesssion to host " + hostHdr + " service " + svcHdr);
        }

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
        Checkpoint ckpt = _checkpoint;

        if (startScn < 0)
        {
          LOG.error("unexpected value for startSCN: " + startScn);
          _stateReuse.switchToStartScnResponseError();
        }
        else if (ckpt.getConsumptionMode() != DbusClientMode.BOOTSTRAP_SNAPSHOT)
        {
          LOG.error("StartScnResponseProcessor:" + " expecting in client mode: " + DbusClientMode.BOOTSTRAP_SNAPSHOT
                    + " while in the incorrect mode: " + ckpt.getConsumptionMode());
        }
        else
        {
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

          LOG.info("Start SCN "
                    + startScn
                    + " received for bootstrap snapshot source "
                    + ckpt.getSnapshotSource());
          ckpt.setBootstrapStartScn(startScn);
          ckpt.setBootstrapServerInfo(serverHostPort);
        }
      }
    }
    catch (Exception ex)
    {
      LOG.error("Failed to process /startscn response", ex);
      _stateReuse.switchToStartScnResponseError();
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
    LOG.error("exception during /startSCN response: " + cause, cause);
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
