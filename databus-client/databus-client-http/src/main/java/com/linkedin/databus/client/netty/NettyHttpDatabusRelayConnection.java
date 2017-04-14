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
import java.nio.charset.Charset;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.Timer;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusRelayConnection;
import com.linkedin.databus.client.DatabusRelayConnectionStateMessage;
import com.linkedin.databus.client.DatabusStreamConnectionStateMessage;
import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.BaseHttpResponseProcessor;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusPrettyLogUtils;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.CompressUtil;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.Range;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;

public class NettyHttpDatabusRelayConnection
             extends AbstractNettyHttpConnection
             implements DatabusRelayConnection
{
  public static final String MODULE = NettyHttpDatabusRelayConnection.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final boolean needCompress = true;

  private static enum State
  {
    SOURCES_REQUEST_CONNECT,
    SOURCES_REQUEST_WRITE,
    REGISTER_REQUEST_CONNECT,
    REGISTER_REQUEST_WRITE,
    STREAM_REQUEST_CONNECT,
    STREAM_REQUEST_WRITE
  }

  private final ActorMessageQueue _callback;
  private DatabusRelayConnectionStateMessage _callbackStateReuse;
  private ExtendedReadTimeoutHandler _readTimeOutHandler;
  private CheckpointMult _checkpoint;
  private State _curState;
  private String _sourcesSubsList;  // comma-separated list of source IDs (integers)
  private int _freeBufferSpace;
  private DbusKeyCompositeFilter _filter;
  private boolean _enableReadFromLatestSCN = false;

  //private MyConnectListener _connectListener;

  final int _maxEventVersion; // max version of DbusEvent this client can understand

  public NettyHttpDatabusRelayConnection(ServerInfo relay,
                                         ActorMessageQueue callback,
                                         ClientBootstrap bootstrap,
                                         ContainerStatisticsCollector containerStatsCollector,
                                         RemoteExceptionHandler remoteExceptionHandler,
                                         Timer timeoutTimer,
                                         long writeTimeoutMs,
                                         long readTimeoutMs,
                                         int protocolVersion,
                                         int maxEventVersion,
                                         ChannelGroup channelGroup)
  {

    super(relay, bootstrap, containerStatsCollector, timeoutTimer, writeTimeoutMs, readTimeoutMs,
          channelGroup, protocolVersion, LOG);
    _callback = callback;
    _maxEventVersion = maxEventVersion;

    //_connectListener = new MyConnectListener();
    //setConnectListener(_connectListener);
  }

  public NettyHttpDatabusRelayConnection(ServerInfo relay,
                                         ActorMessageQueue callback,
                                         ChannelFactory channelFactory,
                                         ContainerStatisticsCollector containerStatsCollector,
                                         RemoteExceptionHandler remoteExceptionHandler,
                                         Timer timeoutTimer,
                                         long writeTimeoutMs,
                                         long readTimeoutMs,
                                         int protocolVersion,
                                         int maxEventVersion,
                                         ChannelGroup channelGroup)
  {
    this(relay,
         callback,
         new ClientBootstrap(channelFactory), containerStatsCollector, remoteExceptionHandler,
                             timeoutTimer, writeTimeoutMs, readTimeoutMs, protocolVersion,
                             maxEventVersion, channelGroup);
  }

  public boolean isEnableReadFromLatestSCN()
  {
    return _enableReadFromLatestSCN;
  }

  @Override
  public void requestSources(DatabusRelayConnectionStateMessage stateReuse)
  {
    _callbackStateReuse = stateReuse;

    if (!hasConnection())
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
    _readTimeOutHandler =
        (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage> sourcesResponseProcessor =
        new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(this, _callback,
            _callbackStateReuse, _readTimeOutHandler);

    String uriString = "/sources?" + DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=" + getProtocolVersion();

    LOG.info("Sending " + uriString);

    // Prepare the HTTP request.
    HttpRequest request = createEmptyRequest(uriString);
    sendRequest(request, new SourcesRequestResultListener(), sourcesResponseProcessor);
  }

  @Override
  public void requestRegister(String sourcesIdList, DatabusRelayConnectionStateMessage stateReuse)
  {
    _sourcesSubsList = sourcesIdList;
    _callbackStateReuse = stateReuse;

    if (!hasConnection())
    {
      _curState = State.REGISTER_REQUEST_CONNECT;
      //we've lost our connection to the relay; it's best to signal an error to the puller so that
      //the client can go through the whole registration process again
      onRequestFailure((String)null, new ClosedChannelException());
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
    _readTimeOutHandler =
        (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    RegisterHttpResponseProcessor registerResponseProcessor =
        new RegisterHttpResponseProcessor(this, _callback, _callbackStateReuse, _readTimeOutHandler);

    final String url = createRegisterUrl();

    LOG.info("Sending " + url);

    // Prepare the HTTP request.
    HttpRequest request = createEmptyRequest(url);
    sendRequest(request, new RegisterRequestResultListener(), registerResponseProcessor);
  }

  private String createRegisterUrl()
  {
    StringBuilder uriString = new StringBuilder(1024);

    // Technically, "/register" should be "/getschemas" or similar; there's no actual registration
    // going on, just retrieval of the schemas we (the client) think we're going to need in order
    // to decode our events.  The relay doesn't store any state based on this call.
    //
    // DDSDBUS-2009, DDSDBUS-2175:  Always specify the protocol version, and if it's >= 4, its a
    // request for an extended response (metadata and/or key schemas in addition to source/payload/
    // document schemas) for the /register request.  Wiki reference:  Databus+v2.0++Protocol
    uriString.append("/register?")
             .append(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM)
             .append("=")
             .append(getProtocolVersion());
    if (getProtocolVersion() < 3) // protocol v2
    {
      // Older clients send the list of source IDs because they know them in advance; newer
      // clients don't necessarily.  If the parameter is omitted, all schemas are returned.
      // (Always omitting this should be fine, but there's no real need to make such a change
      // right now.)
      uriString.append("&sources=")
               .append(_sourcesSubsList);
    }
    uriString.append("&").append(DatabusHttpHeaders.PROTOCOL_COMPRESS_PARAM).append("=").append(needCompress);
    final String url = uriString.toString();
    return url;
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

    _filter = filter;

    if (!hasConnection())
    {
      _curState = State.STREAM_REQUEST_CONNECT;
      //we've lost our connection to the relay; it's best to signal an error to the puller so that
      //the client can go through the whole registration process again
      onRequestFailure("/stream", new ClosedChannelException());
    }
    else
    {
      onStreamConnectSuccess();
    }
  }

  void formRequest(Formatter formatter, String filtersStr)
  {
    StringBuilder fmtString = new StringBuilder(1024);

    fmtString.append("/stream?")
             .append(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM)
             .append("=")
             .append(getProtocolVersion())
             .append((getProtocolVersion() >= 3)? "&subs" : "&sources")
             .append("=%s&streamFromLatestScn=%s&")
             .append((getProtocolVersion() >= 3)? "checkPointMult" : "checkPoint")
             .append("=%s&output=binary&size=%d");
    if (filtersStr != null)
    {
      fmtString.append("&filters=")
               .append(filtersStr);
    }
    if (_maxEventVersion > 0)
    {
      fmtString.append("&")
               .append(DatabusHttpHeaders.MAX_EVENT_VERSION)
               .append("=")
               .append(_maxEventVersion);
    }

    formatter.format(fmtString.toString(), _sourcesSubsList, Boolean.toString(_enableReadFromLatestSCN),
                     (getProtocolVersion() >= 3) ?
                         _checkpoint.toString() :
                         _checkpoint.getCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION),
                     _freeBufferSpace);

    if (LOG.isDebugEnabled())
    {
      LOG.debug("request string for stream (protocolVersion=" + getProtocolVersion() + "):" + formatter.toString());
    }
  }

  void onStreamConnectSuccess()
  {
    boolean debugEnabled = LOG.isDebugEnabled();

    _curState = State.STREAM_REQUEST_WRITE;

    ChannelPipeline channelPipeline = _channel.getPipeline();
    _readTimeOutHandler = (ExtendedReadTimeoutHandler)channelPipeline.get(GenericHttpClientPipelineFactory.READ_TIMEOUT_HANDLER_NAME);
    _readTimeOutHandler.start(channelPipeline.getContext(_readTimeOutHandler));

    StreamHttpResponseProcessor streamResponseProcessor =
        new StreamHttpResponseProcessor(this, _callback, _callbackStateReuse, _readTimeOutHandler);

    StringBuilder uriString = new StringBuilder(1024);

    boolean error = populateStreamRequestUrl(uriString);

    if (error)
    {
      return;
    }
    final String url = uriString.toString();

    if (debugEnabled) LOG.debug("Sending " + url);

    // Prepare the HTTP request.
    HttpRequest request = createEmptyRequest(url);
    sendRequest(request, new StreamRequestResultListener(), streamResponseProcessor);
  }

  private boolean populateStreamRequestUrl(StringBuilder uriString)
  {
    ObjectMapper objMapper = new ObjectMapper();
    Formatter uriFmt = new Formatter(uriString);
    String filtersStr = null;
    boolean error = false;
    if (null != _filter)
    {
      try
      {
        Map<Long, DbusKeyFilter> fMap = _filter.getFilterMap();
        if (null != fMap && fMap.size() > 0)
          filtersStr = objMapper.writeValueAsString(fMap);
      }
      catch (IOException ex)
      {
        LOG.error("Got exception while serializing Filters. Filter Map was : " + _filter, ex);
        error = true;
        onRequestFailure(uriString.toString(), ex);
      }
      catch (RuntimeException ex)
      {
        LOG.error("Got exception while serializing Filters. Filter Map was : " + _filter, ex);
        error = true;
        onRequestFailure(uriString.toString(), ex);
      }
    }

    formRequest(uriFmt, filtersStr);
    return error;
  }


  private void connect(State connectState)
  {
    _curState = connectState;
    connectWithListener(new MyConnectListener());
  }

  @Override
  public void enableReadFromLatestScn(boolean enable)
  {
    _enableReadFromLatestSCN  = enable;
  }

  private void onConnectSuccess(Channel channel)
  {

    switch (_curState)
    {
      case SOURCES_REQUEST_CONNECT:
        onSourcesConnectSuccess();
        break;
      case REGISTER_REQUEST_CONNECT:
        onRegisterConnectSuccess();
        break;
      case STREAM_REQUEST_CONNECT:
        onStreamConnectSuccess();
        break;
      default:
        throw new RuntimeException("don't know what to do in state:" + _curState);
    }
  }

  private void onRequestFailure(HttpRequest req, Throwable cause)
  {
    onRequestFailure(null == req ? (String)null : req.getUri(), cause);
  }

  private void onRequestFailure(String req, Throwable cause)
  {
    LOG.info("request failure: req=" + req + "  cause=" + cause);

    // special case - DDSDBUS-1497
    // in case of WriteTimeoutException we will get close channel exception.
    // Since the timeout exception comes from a different thread (timer) we
    // may end up informing PullerThread twice - and causing it to create two new connections
    // Instead we just drop this exception and just react to close channel
    if (shouldIgnoreWriteTimeoutException(cause))
    {
      LOG.error("got RequestFailure because of WriteTimeoutException");
      return;
    }
    switch (_curState)
    {
      case SOURCES_REQUEST_CONNECT:
      case SOURCES_REQUEST_WRITE:
      {
        _callbackStateReuse.switchToSourcesRequestError();
        break;
      }
      case REGISTER_REQUEST_CONNECT:
      case REGISTER_REQUEST_WRITE:
      {
        _callbackStateReuse.switchToRegisterRequestError();
        break;
      }
      case STREAM_REQUEST_CONNECT:
      case STREAM_REQUEST_WRITE:
      {
        _callbackStateReuse.switchToStreamRequestError();
        break;
      }
      default:
        throw new RuntimeException("don't know what to do in state:" + _curState);
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
      NettyHttpDatabusRelayConnection.this.onConnectSuccess(channel);
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener#onConnectFailure(java.lang.Throwable)
     */
    @Override
    public void onConnectFailure(Throwable cause)
    {
      NettyHttpDatabusRelayConnection.this.onRequestFailure((String)null, cause);
    }
  }

  /** Callback for /sources request result */
  private class SourcesRequestResultListener implements SendRequestResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestSuccess(org.jboss.netty.handler.codec.http.HttpRequest)
     */
    @Override
    public void onSendRequestSuccess(HttpRequest req)
    {
      _enableReadFromLatestSCN = false;
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

  /** Callback for /register request result */
  private class RegisterRequestResultListener implements SendRequestResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestSuccess(org.jboss.netty.handler.codec.http.HttpRequest)
     */
    @Override
    public void onSendRequestSuccess(HttpRequest req)
    {
      _enableReadFromLatestSCN = false;
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

  /** Callback for /sources request result */
  private class StreamRequestResultListener implements SendRequestResultListener
  {
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener#onSendRequestSuccess(org.jboss.netty.handler.codec.http.HttpRequest)
     */
    @Override
    public void onSendRequestSuccess(HttpRequest req)
    {
      _enableReadFromLatestSCN = false;
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

  @Override
  public int getMaxEventVersion()
  {
    return _maxEventVersion;
  }

}

class SourcesHttpResponseProcessor<M extends DatabusRelayConnectionStateMessage>
    extends BaseHttpResponseProcessor
{
  public static final String MODULE = SourcesHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusRelayConnectionStateMessage _stateReuse;

  /**
   * Constructor
   * @param parent                the AbstractNettyHttpConnection object that instantiated this
   *                              response processor
   * @param callback              callback to send the processed response or errors to
   * @param stateReuse            a message object to reuse for the callback
   *                              (TODO remove that: premature GC optimization)
   * @param readTimeOutHandler    the ReadTimeoutHandler for the connection handled by this
   *                              response handler.
   */
  public SourcesHttpResponseProcessor(AbstractNettyHttpConnection parent,
                                      ActorMessageQueue callback, M stateReuse,
                                      ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(parent, readTimeOutHandler);
    _callback = callback;
    _stateReuse = stateReuse;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();
    if (_errorHandled)
    {
        return;
    }

    final String sourcesResponseError = "/sources response error: ";
    try
    {
      String exceptionName = RemoteExceptionHandler.getExceptionName(_decorated);
      if (null != exceptionName)
      {
        LOG.error(sourcesResponseError + RemoteExceptionHandler.getExceptionMessage(_decorated));
        _stateReuse.switchToSourcesResponseError();
      }
      else
      {
        String hostHdr = DbusConstants.UNKNOWN_HOST;
        String svcHdr = DbusConstants.UNKNOWN_SERVICE_ID;
        if (null != getParent())
        {
          hostHdr = getParent().getRemoteHost();
          svcHdr = getParent().getRemoteService();
          LOG.info("initiated sesssion to host " + hostHdr + " service " + svcHdr);
        }

        InputStream bodyStream = Channels.newInputStream(_decorated);
        ObjectMapper mapper = new ObjectMapper();

        List<IdNamePair> sources =
            mapper.readValue(bodyStream,
                             new TypeReference<List<IdNamePair>>()
                                 {});
       _stateReuse.switchToSourcesSuccess(sources, hostHdr, svcHdr);
      }
    }
    catch (IOException ex)
    {
      LOG.error(sourcesResponseError, ex);
      _stateReuse.switchToSourcesResponseError();
    }
    catch (RuntimeException ex)
    {
      LOG.error(sourcesResponseError, ex);
      _stateReuse.switchToSourcesResponseError();
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
    DbusPrettyLogUtils.logExceptionAtError("Exception during /sources response: ", cause, LOG);
    if (_responseStatus != ResponseStatus.CHUNKS_FINISHED)
    {
      LOG.info("Enqueueing /sources response error state to puller queue");
      _stateReuse.switchToSourcesResponseError();
      _callback.enqueueMessage(_stateReuse);
    }
    else
    {
      LOG.info("Skipping enqueueing /sources response error state to puller queue");
    }
    super.handleChannelException(cause);
  }
} // end class SourcesHttpResponseProcessor


class RegisterHttpResponseProcessor extends BaseHttpResponseProcessor
{
  public static final String MODULE = RegisterHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ActorMessageQueue _callback;
  private final DatabusRelayConnectionStateMessage _stateReuse;

  private String _registerResponseVersionHdr;  // version of relay-client protocol used by relay for this response

  /**
   * Constructor
   * @param parent                the AbstractNettyHttpConnection object that instantiated this
   *                              response processor
   * @param relayThread           callback to send the processed response or errors to
   * @param stateReuse            a message object to reuse for the callback
   *                              (TODO remove that: premature GC optimization)
   * @param readTimeOutHandler    the ReadTimeoutHandler for the connection handled by this
   *                              response handler.
   */
  public RegisterHttpResponseProcessor(AbstractNettyHttpConnection parent,
                                       ActorMessageQueue relayThread,
                                       DatabusRelayConnectionStateMessage stateReuse,
                                       ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(parent, readTimeOutHandler);
    _callback = relayThread;
    _stateReuse = stateReuse;
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
    _decorated = new ChunkedBodyReadableByteChannel();
    _registerResponseVersionHdr = response.getHeader(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR);
    super.startResponse(response);
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();
    if (_errorHandled)
    {
        return;
    }

    final String registerResponseError = "/register response error: ";
    try
    {
      String exceptionName = RemoteExceptionHandler.getExceptionName(_decorated);
      if (null != exceptionName)
      {
        LOG.error(registerResponseError + RemoteExceptionHandler.getExceptionMessage(_decorated));
        _stateReuse.switchToRegisterResponseError();
      }
      else
      {
        InputStream bodyStream = Channels.newInputStream(_decorated);
        String bodyStr = IOUtils.toString(bodyStream,Charset.defaultCharset().name());
        IOUtils.closeQuietly(bodyStream);
        if (NettyHttpDatabusRelayConnection.needCompress)
        {
          try
          {
            bodyStr = CompressUtil.uncompress(bodyStr);
          }
          catch (Exception e)//failed because the steam may be not compressed
          {
          }
        }

        ObjectMapper mapper = new ObjectMapper();
        int registerResponseVersion = 3;  // either 2 or 3 would suffice here; we care only about 4

        if (_registerResponseVersionHdr != null)
        {
          try
          {
            registerResponseVersion = Integer.parseInt(_registerResponseVersionHdr);
          }
          catch (NumberFormatException e)
          {
            throw new RuntimeException("Could not parse /register response protocol version: " +
                                       _registerResponseVersionHdr);
          }
          if (registerResponseVersion < 2 || registerResponseVersion > 4)
          {
            throw new RuntimeException("Out-of-range /register response protocol version: " +
                                       _registerResponseVersionHdr);
          }
        }

        if (registerResponseVersion == 4)  // DDSDBUS-2009
        {
          HashMap<String, List<Object>> responseMap =
              mapper.readValue(bodyStr, new TypeReference<HashMap<String, List<Object>>>() {});

          // Look for mandatory SOURCE_SCHEMAS_KEY.
          Map<Long, List<RegisterResponseEntry>> sourcesSchemasMap = RegisterResponseEntry.createFromResponse(responseMap,
                                                                                                              RegisterResponseEntry.SOURCE_SCHEMAS_KEY,
                                                                                                              false);
          // Look for optional KEY_SCHEMAS_KEY
          // Key schemas, if they exist, should correspond to source schemas, but it's not
          // a one-to-one mapping.  The same version of a key schema may be used for several
          // versions of a source schema, or vice versa.  (The IDs must correspond.)
          //
          // TODO (DDSDBUS-xxx):  support key schemas on the relay side, too
          Map<Long, List<RegisterResponseEntry>> keysSchemasMap = RegisterResponseEntry.createFromResponse(responseMap,
                                                                                                           RegisterResponseEntry.KEY_SCHEMAS_KEY,
                                                                                                           true);

          // Look for optional METADATA_SCHEMAS_KEY
          List<RegisterResponseMetadataEntry> metadataSchemasList = RegisterResponseMetadataEntry.createFromResponse(responseMap,
                                                                                                                     RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY,
                                                                                                                     true);

          _stateReuse.switchToRegisterSuccess(sourcesSchemasMap, keysSchemasMap, metadataSchemasList);
        }
        else // version 2 or 3
        {
          List<RegisterResponseEntry> schemasList =
              mapper.readValue(bodyStr, new TypeReference<List<RegisterResponseEntry>>() {});

          Map<Long, List<RegisterResponseEntry>> sourcesSchemasMap = RegisterResponseEntry.convertSchemaListToMap(schemasList);

          _stateReuse.switchToRegisterSuccess(sourcesSchemasMap, null, null);
        }
      }
    }
    catch (IOException ex)
    {
      LOG.error(registerResponseError, ex);
      _stateReuse.switchToRegisterResponseError();
    }
    catch (RuntimeException ex)
    {
      LOG.error(registerResponseError, ex);
      _stateReuse.switchToRegisterResponseError();
    }

    _callback.enqueueMessage(_stateReuse);
  }

  @Override
  public void handleChannelException(Throwable cause)
  {
    DbusPrettyLogUtils.logExceptionAtError("Exception during /register response: ", cause, LOG);
    if (_responseStatus != ResponseStatus.CHUNKS_FINISHED)
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Enqueueing /register response error state to puller queue");
      }
      _stateReuse.switchToRegisterResponseError();
      _callback.enqueueMessage(_stateReuse);
    }
    else
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Skipping enqueueing /register response error state to puller queue");
      }
    }
    super.handleChannelException(cause);
  }
} // end class RegisterHttpResponseProcessor


class StreamHttpResponseProcessor extends BaseHttpResponseProcessor
{
  public static final String MODULE = StreamHttpResponseProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final DatabusStreamConnectionStateMessage _stateReuse;
  private final ActorMessageQueue _callback;

  /**
   * Constructor
   * @param parent                the AbstractNettyHttpConnection object that instantiated this
   *                              response processor
   * @param callback              callback to send the processed response or errors to
   * @param stateReuse            a message object to reuse for the callback
   *                              (TODO remove that: premature GC optimization)
   * @param readTimeOutHandler    the ReadTimeoutHandler for the connection handled by this
   *                              response handler.
   */
  public StreamHttpResponseProcessor(AbstractNettyHttpConnection parent,
                                     ActorMessageQueue callback,
                                     DatabusStreamConnectionStateMessage stateReuse,
                                     ExtendedReadTimeoutHandler readTimeOutHandler)
  {
    super(parent, readTimeOutHandler);
    _stateReuse = stateReuse;
    _callback = callback;
  }

  @Override
  public void finishResponse() throws Exception
  {
    super.finishResponse();
    if (LOG.isTraceEnabled()) LOG.trace("finished response for /stream");
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
    try
    {
      if (LOG.isTraceEnabled()) LOG.trace("started response for /stream");
      _decorated = new ChunkedBodyReadableByteChannel();
      super.startResponse(response);
      if (!_errorHandled)
      {
        _stateReuse.switchToStreamSuccess(_decorated);
        _callback.enqueueMessage(_stateReuse);
      }
    }
    catch (Exception e)
    {
      LOG.error("Error reading events from server", e);
      if (!_errorHandled)
      {
        _stateReuse.switchToStreamResponseError();
        _callback.enqueueMessage(_stateReuse);
      }
    }
  }

  @Override
  public void handleChannelException(Throwable cause)
  {
    DbusPrettyLogUtils.logExceptionAtError("Exception during /stream response: ", cause, LOG);
    if ((_responseStatus != ResponseStatus.CHUNKS_SEEN) &&
        (_responseStatus != ResponseStatus.CHUNKS_FINISHED))
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Enqueueing /stream response error state to puller queue");
      }
      _stateReuse.switchToStreamResponseError();
      _callback.enqueueMessage(_stateReuse);
    }
    else
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Skipping enqueueing /stream response error state to puller queue");
      }
    }
    super.handleChannelException(cause);
  }
} // end class StreamHttpResponseProcessor
