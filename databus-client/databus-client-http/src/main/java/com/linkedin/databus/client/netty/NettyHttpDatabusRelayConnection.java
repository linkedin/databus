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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
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
import com.linkedin.databus.client.DatabusRelayConnection;
import com.linkedin.databus.client.DatabusRelayConnectionStateMessage;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.DatabusStreamConnectionStateMessage;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.Range;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;

public class NettyHttpDatabusRelayConnection
             implements DatabusRelayConnection, ChannelFutureListener
{
  public static final String MODULE = NettyHttpDatabusRelayConnection.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static enum State
  {
    SOURCES_REQUEST_CONNECT,
    SOURCES_REQUEST_WRITE,
    REGISTER_REQUEST_CONNECT,
    REGISTER_REQUEST_WRITE,
    STREAM_REQUEST_CONNECT,
    STREAM_REQUEST_WRITE
  }

  private final ServerInfo _relay;
  private final ActorMessageQueue _callback;
  private final ClientBootstrap _bootstrap;
  private final GenericHttpClientPipelineFactory _pipelineFactory;
  private DatabusRelayConnectionStateMessage _callbackStateReuse;
  Channel _channel;
  private ExtendedReadTimeoutHandler _readTimeOutHandler;
  private CheckpointMult _checkpoint;
  private State _curState;
  private int _connectRetriesLeft;
  private String _sourcesSubsList;
  private int _freeBufferSpace;
  private DbusKeyCompositeFilter _filter;
  private boolean _enableReadFromLatestSCN = false;
  private final int _version;
  private GenericHttpResponseHandler _handler;

  public NettyHttpDatabusRelayConnection(ServerInfo relay,
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
    _relay = relay;
    _callback = callback;
    _bootstrap = bootstrap;
    //FIXME Use a config for the retries and timeout (DDSDBUS-97)
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
    _channel = null;
    _version = version;
  }

  @Override
  public int getVersion() {
    return _version;
  }

  public NettyHttpDatabusRelayConnection(ServerInfo relay,
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
                             timeoutTimer, writeTimeoutMs, readTimeoutMs, version,
                             channelGroup);
  }


  @Override
  public void close()
  {
	  if ( (null != _channel) && _channel.isConnected() )
	  {
		  ChannelFuture res = _channel.close();
		  res.awaitUninterruptibly();
	  }
  }

  public boolean isEnableReadFromLatestSCN() {
	return _enableReadFromLatestSCN;
  }

@Override
  public void requestSources(DatabusRelayConnectionStateMessage stateReuse)
  {
    _callbackStateReuse = stateReuse;
    _handler = null;
    if (null == _channel || ! _channel.isConnected())
    {
      connect(State.SOURCES_REQUEST_CONNECT);
    }
    else
    {
      onSourcesConnectSuccess();
    }
  }

  void onSourcesConnectSuccess()
  {
    _curState = State.SOURCES_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage> sourcesResponseProcessor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(_callback,
            _callbackStateReuse, _readTimeOutHandler);
    _handler = new GenericHttpResponseHandler(sourcesResponseProcessor,
            GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE);
    channelPipeline.replace(
        "handler", "handler", _handler);

    String uriString = "/sources";

    LOG.info("Sending " + uriString);

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uriString);
    request.setHeader(HttpHeaders.Names.HOST, _relay.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    if (_channel.isConnected())
    {
      ChannelFuture future = _channel.write(request);
      future.addListener(this);
      LOG.debug("Wrote /sources request");
    }
    else
    {
      onRequestFailure(new ClosedChannelException());
      LOG.error("disconnect on /sources request");
    }
  }

  @Override
  public void requestRegister(String sourcesIdList, DatabusRelayConnectionStateMessage stateReuse)
  {
    _sourcesSubsList = sourcesIdList;
    _callbackStateReuse = stateReuse;
    _handler = null;

    if (null == _channel || ! _channel.isConnected())
    {
      _curState = State.REGISTER_REQUEST_CONNECT;
      //we've lost our connection to the relay; it's best to signal an error to the puller so that
      //the client can go through the whole registration process again
      onRequestFailure(new ClosedChannelException());
    }
    else
    {
      onRegisterConnectSuccess();
    }
  }

  void onRegisterConnectSuccess()
  {
    _curState = State.REGISTER_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    RegisterHttpResponseProcessor registerResponseProcessor =
        new RegisterHttpResponseProcessor(_callback, _callbackStateReuse, _readTimeOutHandler);
    _handler =  new GenericHttpResponseHandler(registerResponseProcessor,
    									GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE);

    channelPipeline.replace(
        "handler", "handler",_handler);

    StringBuilder uriString = new StringBuilder(1024);
    if(3 != getVersion()) {
      uriString.append("/register?sources=");
      uriString.append(_sourcesSubsList);
    } else {
      uriString.append("/register");
    }

    LOG.info("Sending " + uriString.toString());

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, uriString.toString());
    request.setHeader(HttpHeaders.Names.HOST, _relay.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);


    if (_channel.isConnected())
    {
      ChannelFuture future = _channel.write(request);
      //not enough info to pass the list of source names
      future.addListener(this);
    }
    else
    {
      onRequestFailure(new ClosedChannelException());
      LOG.error("disconnect on /register request");
    }
  }

  @Override
  public void requestStream(String sourcesSubsList, DbusKeyCompositeFilter filter,
		  					int freeBufferSpace, CheckpointMult cp, Range keyRange,
                            DatabusRelayConnectionStateMessage stateReuse)
  {
    _checkpoint = cp;
    _callbackStateReuse = stateReuse;
    _sourcesSubsList = sourcesSubsList;
    _freeBufferSpace = freeBufferSpace;
    _handler = null;

    _filter = filter;

    if (null == _channel || ! _channel.isConnected())
    {
      _curState = State.STREAM_REQUEST_CONNECT;
      //we've lost our connection to the relay; it's best to signal an error to the puller so that
      //the client can go through the whole registration process again
      onRequestFailure(new ClosedChannelException());
    }
    else
    {
      onStreamConnectSuccess();
    }
  }

  void formRequest(Formatter formatter, String filtersStr) {
    String checkpointParam = (_version >= 3)? "checkPointMult" : "checkPoint";
    String fmtString =
      "/stream?" + ((_version >= 3)? "subs" : "sources") +
      "=%s&streamFromLatestScn=%s&" + checkpointParam + "=%s&output=binary&size=%d";
    if(filtersStr != null)
      fmtString =  fmtString + "&filters=" + filtersStr;

    formatter.format(fmtString, _sourcesSubsList, Boolean.toString(_enableReadFromLatestSCN),
                     (_version >= 3) ?
                    	   _checkpoint.toString() :
                    		 _checkpoint.getCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION),
                     _freeBufferSpace);

    if(LOG.isDebugEnabled())
      LOG.debug("request string for stream (v=" + _version + "):" + formatter.toString());
  }

  void onStreamConnectSuccess()
  {
    boolean debugEnabled = LOG.isDebugEnabled();

    _curState = State.STREAM_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    StreamHttpResponseProcessor streamResponseProcessor =
        new StreamHttpResponseProcessor(_callback, _callbackStateReuse, _readTimeOutHandler);

    _handler = new GenericHttpResponseHandler(streamResponseProcessor,
    										GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE);

    channelPipeline.replace(
        "handler", "handler", _handler);

    StringBuilder uriString = new StringBuilder(1024);
    Formatter uriFmt = new Formatter(uriString);

    ObjectMapper objMapper = new ObjectMapper();
    String filtersStr = null;
    if ( null != _filter)
    {
    	try
    	{
    		Map<Long, DbusKeyFilter> fMap = _filter.getFilterMap();
    		if ( null != fMap && fMap.size()>0)
    			filtersStr = objMapper.writeValueAsString(fMap);
    	} catch( IOException ex) {
    		LOG.error("Got exception while serializing Filters. Filter Map was : " + _filter, ex);
    		onRequestFailure(ex);
    	}
    	catch( RuntimeException ex) {
    		LOG.error("Got exception while serializing Filters. Filter Map was : " + _filter, ex);
    		onRequestFailure(ex);
    	}
    }

    formRequest(uriFmt, filtersStr);

    if (debugEnabled) LOG.debug("Sending " + uriString.toString());

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, uriString.toString());
    request.setHeader(HttpHeaders.Names.HOST, _relay.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

    // Send the HTTP request.
    if (_channel.isConnected())
    {
      ChannelFuture future = _channel.write(request);
      future.addListener(this);
    }
    else
    {
      onRequestFailure(new ClosedChannelException());
      LOG.error("disconnect on /stream request");
    }
  }


  private void connect(State connectState)
  {
    _curState = connectState;
    //FIXME Use a config for the retries and timeout (DDSDBUS-98)
    _connectRetriesLeft = DatabusSourcesConnection.MAX_CONNECT_RETRY_NUM;
    connectRetry();
  }

  private void connectRetry()
  {
    LOG.info("connecting : " + _relay.toString());
    ChannelFuture future = _bootstrap.connect(_relay.getAddress());
    future.addListener(this);
  }

  @Override
  public synchronized void operationComplete(ChannelFuture future) throws Exception
  {
    switch (_curState)
    {
      case SOURCES_REQUEST_CONNECT:
      case REGISTER_REQUEST_CONNECT:
      case STREAM_REQUEST_CONNECT:
      {
        onConnectComplete(future);
        break;
      }
      case SOURCES_REQUEST_WRITE:
      case REGISTER_REQUEST_WRITE:
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
      onRequestFailure(future.getCause());
    }
    else if (future.isSuccess())
    {
      onConnectSuccess(future.getChannel());
    }
    else if (_connectRetriesLeft > 0)
    {
      -- _connectRetriesLeft;
      LOG.warn("connect failed: retries left " + _connectRetriesLeft + ": " + _relay.toString(),
               future.getCause());
      connectRetry();
    }
    else
    {
      LOG.error("connect failed; giving up:" + _relay.toString(), future.getCause());
      onRequestFailure(future.getCause());
    }
  }

  private void onConnectSuccess(Channel channel) throws Exception
  {
    LOG.info("connected: " + _relay.toString());
    _channel = channel;

    switch (_curState)
    {
      case SOURCES_REQUEST_CONNECT: onSourcesConnectSuccess(); break;
      case REGISTER_REQUEST_CONNECT: onRegisterConnectSuccess(); break;
      case STREAM_REQUEST_CONNECT: onStreamConnectSuccess(); break;
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }
  }

  private void onWriteComplete(ChannelFuture future) throws Exception
  {
    if (future.isCancelled())
    {
      LOG.error("Write cancelled !!");
      onRequestFailure(future.getCause());
    }
    else if (! future.isSuccess())
    {
      LOG.error("Write failed", future.getCause());
      onRequestFailure(future.getCause());
    } else {
    	//reset readFromLatestSCN
    	_enableReadFromLatestSCN = false;
    }
  }

  private void onRequestFailure(Throwable cause)
  {
    switch (_curState)
    {
      case SOURCES_REQUEST_CONNECT:
      case SOURCES_REQUEST_WRITE:
      {
        _callbackStateReuse.switchToSourcesRequestError(); break;
      }
      case REGISTER_REQUEST_CONNECT:
      case REGISTER_REQUEST_WRITE:
      {
        _callbackStateReuse.switchToRegisterRequestError(); break;
      }
      case STREAM_REQUEST_CONNECT:
      case STREAM_REQUEST_WRITE:
      {
        _callbackStateReuse.switchToStreamRequestError(); break;
      }
      default: throw new RuntimeException("don't know what to do in state:" + _curState);
    }

    _callback.enqueueMessage(_callbackStateReuse);
  }

  @Override
  public void enableReadFromLatestScn(boolean enable) {
	  _enableReadFromLatestSCN  = enable;
  }

}

//FIXME HIGH add handling of server disconnects before response is ready (DDSDBUS-99)
class SourcesHttpResponseProcessor<M extends DatabusRelayConnectionStateMessage>
    extends HttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
{
  public static final String MODULE = SourcesHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusRelayConnectionStateMessage _stateReuse;
  private final ExtendedReadTimeoutHandler _readTimeOutHandler;

  public SourcesHttpResponseProcessor(ActorMessageQueue callback, M stateReuse,
                                      ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(null);
    _callback = callback;
    _stateReuse = stateReuse;
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
      if (null != exceptionName)
      {
        LOG.error("/sources response error: " + RemoteExceptionHandler.getExceptionMessage(_decorated));
        _stateReuse.switchToSourcesResponseError();
      }
      else
      {
      InputStream bodyStream = Channels.newInputStream(_decorated);
      ObjectMapper mapper = new ObjectMapper();

        List<IdNamePair> sources =
            mapper.readValue(bodyStream,
                             new TypeReference<List<IdNamePair>>()
                                 {});
        _stateReuse.switchToSourcesSuccess(sources);
      }
    }
    catch (IOException ex)
    {
      LOG.error("/sources response error: ", ex);
      _stateReuse.switchToSourcesResponseError();
    }
    catch (RuntimeException ex)
    {
      LOG.error("/sources response error: ", ex);
      _stateReuse.switchToSourcesResponseError();
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
    	LOG.info("Enqueueing Register Response Error State to Puller Queue");
    	_stateReuse.switchToSourcesResponseError();
    	_callback.enqueueMessage(_stateReuse);
    } else {
    	LOG.info("Skipping Enqueueing Sources Response Error State to Puller Queue");
    }
    super.handleChannelException(cause);
  }
}

class RegisterHttpResponseProcessor
    extends HttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
{
  public static final String MODULE = RegisterHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusRelayConnectionStateMessage _stateReuse;
  private final ExtendedReadTimeoutHandler _readTimeOutHandler;

  public RegisterHttpResponseProcessor(ActorMessageQueue relayThread,
                                       DatabusRelayConnectionStateMessage stateReuse,
                                       ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(null);
    _callback = relayThread;
    _stateReuse = stateReuse;
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
      if (null != exceptionName)
      {
        LOG.error("/register response error: " + RemoteExceptionHandler.getExceptionMessage(_decorated));
        _stateReuse.switchToRegisterResponseError();
      }
      else
      {
        InputStream bodyStream = Channels.newInputStream(_decorated);
        ObjectMapper mapper = new ObjectMapper();

        List<RegisterResponseEntry> schemas =
            mapper.readValue(bodyStream,
                             new TypeReference<List<RegisterResponseEntry>>()
                                 {});

        HashMap<Long, List<RegisterResponseEntry>> sourcesSchemas =
            new HashMap<Long, List<RegisterResponseEntry>>(schemas.size() * 2);
        for (RegisterResponseEntry entry: schemas)
        {
          List<RegisterResponseEntry> val = sourcesSchemas.get(entry.getId());
          if ( null == val)
          {
        	  val = new ArrayList<RegisterResponseEntry>();
        	  val.add(entry);
        	  sourcesSchemas.put(entry.getId(), val);
          } else {
        	  val.add(entry);
          }
        }

        _stateReuse.switchToRegisterSuccess(sourcesSchemas);
      }
    }
    catch (IOException ex)
    {
      LOG.error("/register response error:", ex);
      _stateReuse.switchToRegisterResponseError();
    }
    catch (RuntimeException ex)
    {
      LOG.error("/register response error:", ex);
      _stateReuse.switchToRegisterResponseError();
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
	LOG.error("channel exception(" + cause.getClass().getSimpleName() + ") during register response. Cause: " + cause.getMessage(),
	              cause);
    if (_responseStatus != ResponseStatus.CHUNKS_FINISHED)
    {
    	LOG.info("Enqueueing Register Response Error State to Puller Queue");
    	_stateReuse.switchToRegisterResponseError();
    	_callback.enqueueMessage(_stateReuse);
    } else {
    	LOG.info("Skipping Enqueueing Register Response Error State to Puller Queue");
    }
    super.handleChannelException(cause);
  }
}


class StreamHttpResponseProcessor
      extends HttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
{
  public static final String MODULE = StreamHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final DatabusStreamConnectionStateMessage _stateReuse;
  private final ActorMessageQueue _callback;
  private final ExtendedReadTimeoutHandler _readTimeOutHandler;

  public StreamHttpResponseProcessor(ActorMessageQueue callback,
                                     DatabusStreamConnectionStateMessage stateReuse,
                                     ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(null);
    _stateReuse = stateReuse;
    _callback = callback;
    _readTimeOutHandler = readTimeOutHandler;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();
    if (LOG.isTraceEnabled()) LOG.trace("finished response for /stream");
    if (null != _readTimeOutHandler) _readTimeOutHandler.stop();
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
	  try
	  {
		  if (LOG.isTraceEnabled()) LOG.trace("started response for /stream");
		  _decorated = new ChunkedBodyReadableByteChannel();
		  super.startResponse(response);
		  if ( !_errorHandled)
		  {
			  _stateReuse.switchToStreamSuccess(_decorated);
			  _callback.enqueueMessage(_stateReuse);
		  }
	  }
	  catch (Exception e)
	  {
		  LOG.error("Error reading events from server", e);
		  if ( ! _errorHandled)
		  {
			  _stateReuse.switchToStreamResponseError();
			  _callback.enqueueMessage(_stateReuse);
		  }
	  }
  }

  @Override
  public void handleChannelException(Throwable cause)
  {
    LOG.error("channel exception(" + cause.getClass().getSimpleName() +
              ") during stream response. Cause: " + cause.getClass() + ":" + cause.getMessage());
    if ((_responseStatus != ResponseStatus.CHUNKS_SEEN) &&
    		(_responseStatus != ResponseStatus.CHUNKS_FINISHED))
    {
    	LOG.info("Enqueueing Stream Response Error State to Puller Queue");
    	_stateReuse.switchToStreamResponseError();
    	_callback.enqueueMessage(_stateReuse);
    } else {
    	LOG.info("Skipping Enqueueing Stream Response Error State to Puller Queue");
    }
    super.handleChannelException(cause);
  }
}
