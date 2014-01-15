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
package com.linkedin.databus.client.netty;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.timeout.WriteTimeoutException;
import org.jboss.netty.util.Timer;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusServerConnection;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusPrettyLogUtils;
import com.linkedin.databus.core.util.DbusHttpUtils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;

/**
 * Common functionality between {@link NettyHttpDatabusRelayConnection} and
 * {@link NettyHttpDatabusBootstrapConnection}.
 *
 */
public class AbstractNettyHttpConnection implements DatabusServerConnection
{
  /** Connection state */
  static enum State
  {
    INIT,
    CONNECTING,
    CONNECTED,
    CLOSING,
    CLOSED,
    ERROR
  }

  //State transitions
  // (INIT, connect()) --> CONNECTING
  // (INIT, close()) --> CLOSING
  // (CONNECTING, connect_error) --> INIT
  // (CONNECTING, connect_success) --> CONNECTED
  // (CONNECTING, close()) --> CLOSING
  // (CONNECTED, close()) --> CLOSING
  // (CLOSING, close()) --> CLOSING
  // (CLOSING, close_complete) --> CLOSED
  // (CLOSED, close()) --> CLOSED
  // (CLOSED, close_complete) --> CLOSED
  // (CLOSED, connect()) --> CONNECTING
  // Attempt for any other transition will result in going to state ERROR

  private final Lock _mutex = new ReentrantLock();
  private final Condition _connectionClosed = _mutex.newCondition();
  private final GenericHttpClientPipelineFactory _pipelineFactory;
  protected final ClientBootstrap _bootstrap;
  protected final ServerInfo _server;
  private final Logger _log;
  final private int _protocolVersion;
  protected volatile Channel _channel;
  private int _connectRetriesLeft;
  private State _state;
  private final GenericHttpResponseHandler _handler;
  String _hostHdr;
  String _svcHdr;

  public AbstractNettyHttpConnection(ServerInfo server,
                                     ClientBootstrap bootstrap,
                                     ContainerStatisticsCollector containerStatsCollector,
                                     Timer timeoutTimer,
                                     long writeTimeoutMs,
                                     long readTimeoutMs,
                                     ChannelGroup channelGroup,
                                     int protocolVersion,
                                     Logger log)
  {
    super();
    _log = null != log ? log : Logger.getLogger(AbstractNettyHttpConnection.class);

    _server = server;
    _bootstrap = bootstrap;
    _protocolVersion = protocolVersion;
    _bootstrap.setOption("connectTimeoutMillis", DatabusSourcesConnection.CONNECT_TIMEOUT_MS);

    _handler = new GenericHttpResponseHandler(GenericHttpResponseHandler.KeepAliveType.KEEP_ALIVE, null);
    _pipelineFactory = new GenericHttpClientPipelineFactory(
        _handler,
        containerStatsCollector,
        timeoutTimer,
        writeTimeoutMs,
        readTimeoutMs,
        channelGroup);
    _bootstrap.setPipelineFactory(_pipelineFactory);
    _channel = null;
    _state = State.INIT;
  }

  @Override
  public int getProtocolVersion()
  {
    return _protocolVersion;
  }


  /** Closes the connection. Note: this method will block until the connection is actually closed */
  @Override
  public void close()
  {
    _log.info("closing connection to: " + _server.getAddress());
    final State newState = switchToClosing();
    if (State.CLOSING != newState && State.CLOSED != newState)
    {
      return;
    }

    if (null == _channel || !_channel.isConnected())
    {
      switchToClosed();
    }
    else
    {
      _channel.close();
      awaitForCloseUninterruptibly();
    }
  }

  @Override
  public String getRemoteHost()
  {
    String host = getHostHdr();
    return null != host ? host : DbusConstants.UNKNOWN_HOST;
  }

  @Override
  public String getRemoteService()
  {
    String service = getSvcHdr();
    return null != service ? service : DbusConstants.UNKNOWN_SERVICE_ID;
  }


  private void awaitForCloseUninterruptibly()
  {
    _mutex.lock();
    try
    {
      while (! isClosed())
      {
        _connectionClosed.awaitUninterruptibly();
      }
    }
    finally
    {
      _mutex.unlock();
    }
  }

  /**
   * @return the logger used by this instance
   */
  public Logger getLog()
  {
    return _log;
  }

  public boolean isInit()
  {
    _mutex.lock();
    try
    {
      return State.INIT == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  public boolean isClosingOrClosed()
  {
    _mutex.lock();
    try
    {
      return State.CLOSING == _state || State.CLOSED == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  public boolean isClosed()
  {
    _mutex.lock();
    try
    {
      return State.CLOSED == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  public boolean isClosing()
  {
    _mutex.lock();
    try
    {
      return State.CLOSING == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  public boolean isConnectingOrConnected()
  {
    _mutex.lock();
    try
    {
      return State.CONNECTING == _state || State.CONNECTED == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  public boolean isConnecting()
  {
    _mutex.lock();
    try
    {
      return State.CONNECTING == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  public boolean isConnected()
  {
    _mutex.lock();
    try
    {
      return State.CONNECTED == _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  private void unexpectedTransition(State fromState, State toState)
  {
    _log.error("unexpected netty connection transition from " + fromState + " to " + toState);
    _state = State.ERROR;
  }

  /**
   * Attempt to switch to CLOSING state
   * @return the new state
   */
  private State switchToClosing()
  {
    _mutex.lock();
    try
    {
      switch (_state)
      {
      case CLOSING:
      case CLOSED: break; //NOOP
      default: _state = State.CLOSING;
      }
      return _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  /**
   * Switch to CLOSED state
   * @return the new state
   */
  private State switchToClosed()
  {
    _mutex.lock();
    try
    {
      switch (_state)
      {
      case CLOSING:
      case CLOSED:
        _state = State.CLOSED;
        break;
      default: unexpectedTransition(_state, State.CLOSED); break;
      }
      return _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  /**
   * Switch to CONNECTING state
   * @return the new state
   */
  private State switchToConnecting()
  {
    _mutex.lock();
    try
    {
      switch (_state)
      {
      case CLOSED:
      case INIT: _state = State.CONNECTING; break;
      default: unexpectedTransition(_state, State.CONNECTING); break;
      }
      return _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  /**
   * Switch to CONNECTED state
   * @return the new state
   */
  private State switchToConnected()
  {
    _mutex.lock();
    try
    {
      switch (_state)
      {
      case CONNECTING: _state = State.CONNECTED; break;
      case CLOSING:
      case CLOSED: break; //NOOP
      default: unexpectedTransition(_state, State.CONNECTED); break;
      }
      return _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  /**
   * Switch to INIT state
   * @return the new state
   */
  private State switchToInit()
  {
    _mutex.lock();
    try
    {
      switch (_state)
      {
      case CONNECTING: _state = State.INIT; break;
      default: unexpectedTransition(_state, State.INIT); break;
      }
      return _state;
    }
    finally
    {
      _mutex.unlock();
    }
  }

  /**
   * Checks if there is network connection is available. Note that if the connection is in a
   * transitional state (CONNECTING, CLOSING), the method will still return true to avoid race
   * conditions where the caller will try to re-establish the connection while these transitions are
   * in progress. If the caller tries an operation that is not allowed on the channel, e.g. send data
   * over a closed channel, netty should be able to detect that and throw a ClosedChannelException.   *
   */
  protected boolean hasConnection()
  {
    return null != _channel && State.INIT != _state && State.CLOSED != _state;
    //return null != _channel && _channel.isConnected();
  }

  /** For testing */
  State getNetworkState()
  {
    return _state;
  }

  protected void connectWithListener(ConnectResultListener listener)
  {

    if (State.CONNECTING != switchToConnecting())
    {
      listener.onConnectFailure(new RuntimeException("unable to connect"));
    }
    else
    {
      _connectRetriesLeft = DatabusSourcesConnection.MAX_CONNECT_RETRY_NUM;
      connectRetry(listener);
    }
  }

  private void connectRetry(ConnectResultListener listener)
  {
    _log.info("connecting: " + _server.toSimpleString());
    if (isClosingOrClosed())
    {
      listener.onConnectFailure(new ClosedChannelException());
      return;
    }
    else if (! isConnecting())
    {
      listener.onConnectFailure(new RuntimeException("unable to connect"));
    }
    else
    {
      //ChannelFuture future = _bootstrap.connect(_server.getAddress());
      //future.addListener(new MyConnectListener(listener));
      _handler.reset();// reset state to make sure new connection starts with a blank Handler state
      _handler.setConnectionListener(new AbstractNettyConnectListener(listener));
      _bootstrap.connect(_server.getAddress());
    }
  }

  protected void sendRequest(HttpRequest request,
                             SendRequestResultListener listener,
                             HttpResponseProcessor responseProcessor)
  {
    if (isClosingOrClosed())
    {
      listener.onSendRequestFailure(request, new ClosedChannelException());
    }
    else if (! isConnected())
    {
      listener.onSendRequestFailure(request, new RuntimeException("unable to send request"));
    }
    else
    {
      try {
        setResponseProcessor(responseProcessor, listener);
      } catch (DatabusException e) {
        listener.onSendRequestFailure(request, e.getCause());
        _channel.close();
        return;
      }

      // Send the HTTP request.
      if (_channel.isConnected())
      {
        //ChannelFuture future = _channel.write(request);
        //future.addListener(new MySendRequestListener(request, listener));
        _channel.write(request);
      }
      else
      {
        _log.error("disconnect on request: " + request.getUri());
        listener.onSendRequestFailure(request, new ClosedChannelException());
      }
    }
  }

  /**
   * Creates an empty HttpRequest object with pre-filled standard headers
   * @param uriString  the request URL
   * @return the request object
   */
  protected HttpRequest createEmptyRequest(String uriString)
  {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uriString);
    request.setHeader(HttpHeaders.Names.HOST, _server.getAddress().toString());
    request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
    String hostHdr = DbusHttpUtils.getLocalHostName();
    String svcHdr = DbusConstants.getServiceIdentifier();
    if (! hostHdr.isEmpty())
    {
        request.setHeader(DatabusHttpHeaders.DBUS_CLIENT_HOST_HDR, hostHdr);
    }
    if (! svcHdr.isEmpty())
    {
        request.setHeader(DatabusHttpHeaders.DBUS_CLIENT_SERVICE_HDR, svcHdr);
    }
    return request;
  }

  protected void setConnectListener(ConnectResultListener l) {
    _handler.setConnectionListener(l);
  }

  protected void setResponseProcessor(HttpResponseProcessor responseProcessor, SendRequestResultListener l)
      throws DatabusException
  {
    _handler.setResponseProcessor(responseProcessor);
    _handler.setRequestListener(l);

    /*
    if (null != _channel)
    {
      ChannelPipeline channelPipeline = _channel.getPipeline();
      channelPipeline.replace("handler", "handler", _handler);
    }
    */
    assert(_channel != null);
    assert(_channel.getPipeline().get("handler")!=null);
  }

  protected GenericHttpResponseHandler getHandler()
  {
    return _handler;
  }

  protected boolean shouldIgnoreWriteTimeoutException(Throwable cause)
  {
    // special case - DDSDBUS-1497
    // in case of WriteTimeoutException we will get close channel exception.
    // Since the timeout exception comes from a different thread (timer) we
    // may end up informing PullerThread twice - and causing it to create two new connections
    // Instead we just drop this exception and just react to close channel

    //If null == getHandler(), the error happened the connect attempt, i.e. the request was not
    //sent.
    boolean requestSent = null != getHandler() ? getHandler()._messageState.hasSentRequest()
                                               : false;
    if(requestSent && (cause instanceof WriteTimeoutException)) {
      _log.error("Got RequestFailure because of WriteTimeoutException. requestSent = " + requestSent);
      return true;
    }
    else
    {
      _log.error("The request has not been sent due to " + cause + " requestSent = " + requestSent);
      return false;
    }
  }

  protected void addConnectionTracking(HttpResponse response) throws Exception
  {
      boolean debugEnabled = _log.isDebugEnabled();
      _hostHdr = response.getHeader(DatabusHttpHeaders.DBUS_SERVER_HOST_HDR);
      _svcHdr  = response.getHeader(DatabusHttpHeaders.DBUS_SERVER_SERVICE_HDR);
      if (debugEnabled)
      {
          if (null != _hostHdr)
          {
            _log.debug("Received response for databus server host: " + _hostHdr);
          }
          if (null != _svcHdr)
          {
            _log.debug("Received response for databus server host: " + _svcHdr);
          }
      }
      return;
  }

  /** Internal listener to handle connect success and failures */
  private class AbstractNettyConnectListener implements ConnectResultListener
  {
    private final ConnectResultListener _listener;

    public AbstractNettyConnectListener(ConnectResultListener listener)
    {
      super();
      _listener = listener;
    }
    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener#onConnectSuccess(org.jboss.netty.channel.Channel)
     */
    @Override
    public void onConnectSuccess(Channel channel)
    {
      if (State.CONNECTED != switchToConnected())
      {
        _listener.onConnectFailure(new RuntimeException("unable to connect"));
      }
      else
      {
        _log.info("connected: " + _server.toSimpleString());
        //_channel = future.getChannel();
        //channel.getCloseFuture().addListener(new MyChannelCloseListener());
        _handler.setCloseListener(new NettyChannelCloseListener());
        _channel = channel;
        _listener.onConnectSuccess(channel);
      }
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener#onConnectFailure(java.lang.Throwable)
     */
    @Override
    public void onConnectFailure(Throwable cause)
    {
      DbusPrettyLogUtils.logExceptionAtError("Connect cancelled/failed", cause, _log);
      switchToInit();
      _listener.onConnectFailure(cause);
    }
  }


  /** Marks the connection as closed */
  private class NettyChannelCloseListener implements ChannelCloseListener
  {

    @Override
    public void onChannelClose()
    {
      _mutex.lock();
      try
      {
        switchToClosing();
        switchToClosed();
        _connectionClosed.signalAll();
        _log.info("connection closed: " + _server.getAddress());
      }
      finally
      {
        _mutex.unlock();
      }
    }
  }

  /** Notifies about channel close */
  public interface ChannelCloseListener
  {
    /** Notifies about channel close */
    public void onChannelClose();
  }

  /** Notifies for the result of connect attempts */
  public interface ConnectResultListener
  {
    /** Notifies about connect success with the given Netty channel */
    void onConnectSuccess(Channel channel);

    /** Notifies about connect failure with the given cause */
    void onConnectFailure(Throwable cause);
  }

  /** Notifies for the result of sending requests */
  public interface SendRequestResultListener
  {
    /** Notifies about success of sending the specified request */
    void onSendRequestSuccess(HttpRequest req);

    /** Notifies about failure of sending the specified request with the given cause */
    void onSendRequestFailure(HttpRequest req, Throwable cause);
  }

  static class BaseHttpResponseProcessor
        extends AbstractHttpResponseProcessorDecorator <ChunkedBodyReadableByteChannel>
  {
    private final ExtendedReadTimeoutHandler _readTimeOutHandler;
    private final AbstractNettyHttpConnection _parent;
    protected String _serverErrorClass;
    protected String _serverErrorMessage;

    /**
     * Constructor
     * @param parent                the AbstractNettyHttpConnection object that instantiated this
     *                              response processor
     * @param readTimeOutHandler    the ReadTimeoutHandler for the connection handled by this
     *                              response handler.
     */
    public BaseHttpResponseProcessor(AbstractNettyHttpConnection parent,
                                     ExtendedReadTimeoutHandler readTimeOutHandler)
    {
      super(null);
      _readTimeOutHandler = readTimeOutHandler;
      _parent = parent;
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractHttpResponseProcessorDecorator#finishResponse()
     */
    @Override
    public void finishResponse() throws Exception
    {
      try
      {
        super.finishResponse();
      }
      finally
      {
        stopReadTimeoutTimer();
      }
    }

    protected void stopReadTimeoutTimer()
    {
      if (null != _readTimeOutHandler)
      {
        _readTimeOutHandler.stop();
      }
    }

    /**
     * @see com.linkedin.databus.client.netty.AbstractHttpResponseProcessorDecorator#startResponse(org.jboss.netty.handler.codec.http.HttpResponse)
     */
    @Override
    public void startResponse(HttpResponse response) throws Exception
    {
      //check for errors in the response
      _serverErrorClass = response.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_CLASS_HEADER);
      _serverErrorMessage = response.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_MESSAGE_HEADER);
      if (null == _serverErrorClass)
      {
        _serverErrorClass = response.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER);
        _serverErrorMessage = response.getHeader(DatabusHttpHeaders.DATABUS_ERROR_MESSAGE_HEADER);
      }
      if (null != _serverErrorClass)
      {
        if (null != _parent)
        {
          _parent.getLog().error("server error detected class=" + _serverErrorClass +
                                 " message=" + _serverErrorMessage);
        }
      }

      super.startResponse(response);
      if (null != _parent) _parent.addConnectionTracking(response);
    }

    protected AbstractNettyHttpConnection getParent()
    {
      return _parent;
    }

    /* (non-Javadoc)
     * @see com.linkedin.databus.client.netty.HttpResponseProcessorDecorator#addTrailer(org.jboss.netty.handler.codec.http.HttpChunkTrailer)
     */
    @Override
    public void addTrailer(HttpChunkTrailer trailer) throws Exception
    {
      //check for errors in the middle of the response
      if (null == _serverErrorClass)
      {
        _serverErrorClass = trailer.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_CLASS_HEADER);
        _serverErrorMessage = trailer.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_MESSAGE_HEADER);
        if (null == _serverErrorClass)
        {
          _serverErrorClass = trailer.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER);
          _serverErrorMessage = trailer.getHeader(DatabusHttpHeaders.DATABUS_ERROR_MESSAGE_HEADER);
        }
        if (null != _serverErrorClass)
        {
          if (null != _parent)
          {
            _parent.getLog().error("server error detected class=" + _serverErrorClass +
                                   " message=" + _serverErrorMessage);
          }
        }
      }

      super.addTrailer(trailer);
    }

  }

  protected String getHostHdr()
  {
    return _hostHdr;
  }

  protected String getSvcHdr()
  {
    return _svcHdr;
  }

  @Override
  public int getMaxEventVersion()
  {
    // return default
    return DbusEventFactory.DBUS_EVENT_V1;
  }
}
