/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.databus.client.netty;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ChannelCloseListener;
import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.ConnectResultListener;
import com.linkedin.databus.client.netty.AbstractNettyHttpConnection.SendRequestResultListener;
import com.linkedin.databus.core.DbusPrettyLogUtils;
import com.linkedin.databus2.core.DatabusException;

public class GenericHttpResponseHandler extends SimpleChannelHandler {
  public static final String MODULE = GenericHttpResponseHandler.class.getName();

  public static enum KeepAliveType {
    KEEP_ALIVE, NO_KEEP_ALIVE
  }

  public static enum MessageState {
    INIT,
    CONNECTED,// on successful connection
    CONNECT_FAIL,
    REQUEST_WAIT, // waiting for new requests
    REQUEST_START, REQUEST_SENT, REQUEST_FAILURE,
    RESPONSE_START, RESPONSE_FINISH, RESPONSE_FAILURE,
    WAIT_FOR_CHUNK,
    CLOSED;

    public boolean hasSentRequest() {
      if (this.equals(INIT) ||
          this.equals(CONNECTED) ||
          this.equals(REQUEST_WAIT) ||
          this.equals(REQUEST_START) ||
          this.equals(REQUEST_FAILURE)
          )
        return false;

      return true;
    }
    public boolean waitForResponse() {
      if(this.equals(REQUEST_SENT) ||
          this.equals(RESPONSE_START) ||
          this.equals(WAIT_FOR_CHUNK))
        return true;

      return false;
    }

  };

  /** is used primary for status printing */
  public static enum ChannelState {
    CHANNEL_ACTIVE, CHANNEL_EXCEPTION, CHANNEL_CLOSED
  };

  private HttpResponseProcessor _responseProcessor;
  private final KeepAliveType _keepAlive;
  MessageState _messageState;
  private ChannelState _channelState;
  private HttpResponse _httpResponse;
  private HttpChunkTrailer _httpTrailer;
  private HttpRequest _httpRequest;

  private ConnectResultListener _connectListener = null;
  private SendRequestResultListener _requestListener = null;
  private ChannelCloseListener _closeListener = null;
  private Throwable _lastError;
  private final StateLogger _log;

  // TODO - remove this constructor (used in tests only)
  GenericHttpResponseHandler(HttpResponseProcessor responseProcessor,
                             KeepAliveType keepAlive) {
    this(keepAlive, null);
    _responseProcessor = responseProcessor;
  }

  public GenericHttpResponseHandler(KeepAliveType keepAlive) {
    this(keepAlive, null);
  }

  public GenericHttpResponseHandler(KeepAliveType keepAlive, Logger log) {
    super();
    _keepAlive = keepAlive;
    if(log == null) {
      log = Logger.getLogger(MODULE);
    }
    _log = new StateLogger(log); //wrapper for extra dynamic info in the log
    _messageState = MessageState.INIT;
    reset();
    _log.info("Created new Handler");
  }

  /** set listener for channelConnected event */
  public synchronized void setConnectionListener(ConnectResultListener listener) {
    _connectListener = listener;
  }
  /** set listener for request submitted event */
  public synchronized void setRequestListener(SendRequestResultListener listener) {
    _requestListener = listener;
  }
  /** set listerner for channelClosed event */
  public synchronized void setCloseListener(ChannelCloseListener listener) {
    _closeListener = listener;
  }

  public Throwable getLastError() {
    return _lastError;
  }

  /**
   * replace response processor for this Handler
   * @param responseProcessor
   * @throws DatabusException
   */
  public synchronized void setResponseProcessor(HttpResponseProcessor responseProcessor)
      throws DatabusException {

    if(responseProcessor == null) {
      throw new RuntimeException("GenericHttpResponseHandler cannot have null responseProcessor");
    }
    if(_messageState != MessageState.REQUEST_WAIT) {
      String msg = "replacing responseProcessor while in state=" + _messageState;
      _log.error(msg);
      _messageState = MessageState.CLOSED;
      // do we need to find the channel and close it?
      throw new DatabusException(new IllegalStateException(msg));
    } else {
      _log.debug("setting processor " + responseProcessor);
      _responseProcessor = responseProcessor;
    }
  }

  /**
   * validate current state and call listener with failures if it is not as expected
   * @param channel
   * @param expectedState
   * @return true if valid
   */
  private boolean validateCurrentState(Channel c, MessageState expectedState) {
    if(_messageState != expectedState) {
      String msg = "unexpected state: expectedState=" + expectedState +
          "; actual State" + _messageState;

      Throwable cause = new IllegalStateException(msg);
      _log.error(msg, cause);

      if(_messageState == MessageState.INIT)
        informConnectListener(c, cause);
      else if(_messageState == MessageState.REQUEST_START || _messageState == MessageState.REQUEST_WAIT) {
        informRequestListener(_httpRequest, cause);
      } else if(_messageState.waitForResponse()) {
        if(_responseProcessor != null) {
          _responseProcessor.channelException(cause);
        } else {
          _log.error("waiting for response but responseProcessor is null", cause);
        }
      }

      // since we failing the request we need to close the channel
      if(c != null) {
        _log.debug("closing the channel because state validate failed");
        c.close();
      }

      _messageState = MessageState.CLOSED;
      return false; // validation failed
    }
    return true;
  }

  /**
   * this is used mostly in test (except in the constructor)
   * state of the Handler shouldn't be changed from outside
   * if the handler failed - create a new connection (and a new handler)
   */
  synchronized void reset() {
    if(_messageState == MessageState.INIT || _messageState == MessageState.CLOSED) {
      _messageState = MessageState.INIT;
      _channelState = ChannelState.CHANNEL_ACTIVE;
      _requestListener = null;
      _connectListener = null;
      _closeListener = null;
    } else {
      String msg = "calling reset in wrong state " + _messageState;
      _log.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  private void informConnectListener(Channel channel, Throwable cause) {
    boolean success = (cause == null);
    _log.debug("informRequestListener: success=" + success + ";ch=" + channel, cause);

    // listener is nullified (under sync) to guarantee it is called only once
    ConnectResultListener tempListener = null;

    synchronized(this) {
      if(cause != null)
        _lastError = cause;

      if(_connectListener != null) {
        tempListener = _connectListener;
        _connectListener = null;
      }
    }

    if(tempListener != null) {
      _log.info("Notify about connection completed. success=" + success);
      if(success)
        tempListener.onConnectSuccess(channel);
      else
        tempListener.onConnectFailure(cause);

    } else {
      _log.warn("informConnectListener called with listener==null; ch=" + channel, cause);
    }
  }

  private void informRequestListener(HttpRequest req, Throwable cause) {
    boolean success = (cause == null);
    boolean debug = _log.isDebugEnabled();

    // listener is nullified (under sync) to guarantee it is called only once
    SendRequestResultListener tempListener = null;

    synchronized(this) {
      if(cause != null)
        _lastError = cause;
      if(_requestListener != null) {
        tempListener = _requestListener;
        _requestListener = null;
      }
    }

    if(debug)
      _log.debug("informRequestListener: success=" + success + ";req=" + req, cause);
    if(tempListener != null) {
      _log.debug("Notify about requestSent completed. success=" + success);
      if(success)
        tempListener.onSendRequestSuccess(req);
      else
        tempListener.onSendRequestFailure(req, cause);
    } else {
      _log.warn("informRequestListener called with listener==null; req=" + req, cause);
    }
  }

  private void informCloseListener() {
    _log.info("Calling channelCloseListener");

    // listener is nullified (under sync) to guarantee it is called only once
    ChannelCloseListener tempListener = null;

    synchronized (this) {
      if(_closeListener != null) {
        tempListener = _closeListener;
        _closeListener = null;
      }
    }
    if(tempListener != null)
      tempListener.onChannelClose();
  }


  @Override
  public void channelBound(ChannelHandlerContext ctx,
                                        ChannelStateEvent e) throws Exception {
    _log.info("channel to peer bound: " + e.getChannel().getRemoteAddress());
    super.channelBound(ctx, e);
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx,
                                            ChannelStateEvent e) throws Exception {

    _log.debug("channelConnected");
    super.channelConnected(ctx, e);

    synchronized(this) {
      if(! validateCurrentState(e.getChannel(), MessageState.INIT))
        return;

      _messageState = MessageState.CONNECTED;
      _log.info("channel to peer connected: " + e.getChannel().getRemoteAddress());
      _messageState = MessageState.REQUEST_WAIT;
    }

    informConnectListener(e.getChannel(), null); //success
  }

  MessageState getMessageState()
  {
    return _messageState;
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx,
                                          MessageEvent e) throws Exception {

    boolean debugEnabled = _log.isDebugEnabled();
    if(debugEnabled)
      _log.debug("WriteRequested: chConnected=" + e.getChannel().isConnected() + "; msg=" + e.getMessage());

    synchronized (this){

      if ( e.getMessage() instanceof HttpRequest)
      {
        _httpRequest = (HttpRequest)e.getMessage();

        if(! validateCurrentState(e.getChannel(), MessageState.REQUEST_WAIT))
          return;

        _messageState = MessageState.REQUEST_START;
        if (debugEnabled)
          _log.debug("Write Requested  :" + e);

      }
    }
    super.writeRequested(ctx, e);
  }

  @Override
  public void writeComplete(ChannelHandlerContext ctx,
                                         WriteCompletionEvent e) throws Exception {

    Throwable cause = null;

    synchronized(this) {
      _log.debug("WriteComplete");
      // Future should be done by this time
      ChannelFuture future = e.getFuture();
      
      boolean success = future.isSuccess();
      if(_httpRequest == null && success){
    	  super.writeComplete(ctx, e);
    	  return;
      }

      if(! validateCurrentState(e.getChannel(), MessageState.REQUEST_START)) {
        _httpRequest = null;
        return;
      }

      if (!success) {
        String msg = "Write request failed with cause :" + future.getCause();
        _log.error(msg);
        _messageState = MessageState.REQUEST_FAILURE;
        cause = new IllegalStateException(msg);
        _messageState = MessageState.CLOSED;
      } else {
        _messageState = MessageState.REQUEST_SENT;
        _log.debug("Write Completed successfully :" + e);
        _messageState = MessageState.RESPONSE_START;
      }
      _httpRequest = null;
    }

    informRequestListener(_httpRequest, cause);

    super.writeComplete(ctx, e);

    if(cause != null)
      e.getChannel().close();
  }


  @Override
  public void messageReceived(ChannelHandlerContext ctx,
                                           MessageEvent e) throws Exception {
    boolean debug = _log.isDebugEnabled();
    Object message = e.getMessage();
    if(! //not
        ( message instanceof HttpResponse ||
          message instanceof HttpChunkTrailer ||
          message instanceof HttpChunk)) {

      _log.debug("Uknown object type:"
          + message.getClass().getName());

      super.messageReceived(ctx, e);
      return;
    }

    if( null == _responseProcessor) {
      _log.error("No response processor set");
      _messageState = MessageState.CLOSED;
      e.getChannel().close();
      throw new RuntimeException("No response processor set in messageReceived.state="+_messageState + ";msg=" + message);
    }

    synchronized (this) {
      if (message instanceof HttpResponse) {

        if(! validateCurrentState(e.getChannel(), MessageState.RESPONSE_START)) {
          _log.error("MessageReceived(HttpResponse) failed for message: " + message);
          return;
        }

        if(debug)
          _log.debug("msgRecived. HttpResponse");
        _httpResponse = (HttpResponse) message;
        _responseProcessor.startResponse(_httpResponse);
        if (!_httpResponse.isChunked()) {
          finishResponse(e); // done with this request/response
        } else {
          _messageState = MessageState.WAIT_FOR_CHUNK;
        }
      } else if (message instanceof HttpChunkTrailer) {
        if(! validateCurrentState(e.getChannel(), MessageState.WAIT_FOR_CHUNK)) {
          _log.error("MessageReceived(HttpChunkTrailer) failed for message: " + message);
          return;
        }

        if(debug)
          _log.debug("msgRecived. HttpChunkTrailer");
        _httpTrailer = (HttpChunkTrailer) message;
        _responseProcessor.addTrailer(_httpTrailer);
        finishResponse(e); // done with this request/response
      } else if (message instanceof HttpChunk) {
        if(! validateCurrentState(e.getChannel(), MessageState.WAIT_FOR_CHUNK)) {
          _log.error("MessageReceived(HttpChunk) failed for message: " + message);
          return;
        }
        if(debug)
          _log.debug("msgRecived. HttpChunk");
        _messageState = MessageState.WAIT_FOR_CHUNK;
        _responseProcessor.addChunk((HttpChunk) message);
      }
    }
  }

  private void finishResponse(MessageEvent e) throws Exception {
    _messageState = MessageState.RESPONSE_FINISH;
    _log.debug("FINISH_RESPONSE");
    _responseProcessor.finishResponse();
    _responseProcessor = null;
    if (_keepAlive == KeepAliveType.NO_KEEP_ALIVE) {
      e.getChannel().close();
    }
    _messageState = MessageState.REQUEST_WAIT;
  }

  private void logExceptionMessage(Throwable cause) {

    if (!_messageState.hasSentRequest()) {
      if (!(cause instanceof ConnectException))
      {
        _log.info("Skipping exception message even before request has been sent. State=" + _messageState, cause);
      } else {
        _log.info("Got connection Exception", cause);
      }
    } else {
      if (cause instanceof RejectedExecutionException)
      {
        _log.info("shutdown in progress");
      }
      else if (cause instanceof IOException && null != cause.getMessage() &&
          cause.getMessage().contains("Connection reset by peer"))
      {
        _log.warn("connection reset by peer");
      }
      else if (!(cause instanceof ClosedChannelException)) {
        _log.error(
                   "http client exception("
                       + cause.getClass().getSimpleName() + "):"
                       + cause.getMessage(), cause);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx,
                                           ExceptionEvent e) throws Exception {
    boolean debug = _log.isDebugEnabled();
    Throwable cause = e.getCause();

    if(cause == null) { // may it happen? (may be in test)
      cause = new RuntimeException("exceptionCaught is invoked with empty exception");
    }

    logExceptionMessage(cause);

    if(debug) {
      _log.debug("exceptionCaught.rp=" + _responseProcessor, cause);
    }
    synchronized (this) {

      switch(_messageState) {
      case INIT:
        _messageState = MessageState.CONNECT_FAIL;
        informConnectListener(e.getChannel(), cause);
        break;
      case REQUEST_START:
        _messageState = MessageState.REQUEST_FAILURE;
        informRequestListener(_httpRequest, cause);
        break;
      case REQUEST_SENT:
      case RESPONSE_START:
      case WAIT_FOR_CHUNK:
        _messageState = MessageState.RESPONSE_FAILURE;
        if (null != _responseProcessor) {
          _responseProcessor.channelException(cause);
        }
        break;
      default:
        _log.warn("exceptionCaught is called", cause);
      }

      _messageState = MessageState.CLOSED;
      _channelState = ChannelState.CHANNEL_EXCEPTION;
    }

    super.exceptionCaught(ctx, e);

    e.getChannel().close();
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx,
                                         ChannelStateEvent e) throws Exception {
    Channel channel = e.getChannel();
    SocketAddress a = (null != channel) ? channel.getRemoteAddress() : null;
    _log.info("channel to peer closed: " + a);

    synchronized (this){
      _channelState = ChannelState.CHANNEL_CLOSED;

      switch(_messageState) {
      case INIT:
        _log.warn("got closed channel before connecting");
        _messageState = MessageState.CONNECT_FAIL;
        informConnectListener(e.getChannel(), new ClosedChannelException());
        break;
      case REQUEST_WAIT:
        _messageState = MessageState.CLOSED;//normal case
        break;
      case REQUEST_START:
        _log.warn("got closed channel before sending request");
        _messageState = MessageState.REQUEST_FAILURE;
        informRequestListener(_httpRequest, new ClosedChannelException());
        break;
      case REQUEST_SENT:
      case RESPONSE_START:
      case WAIT_FOR_CHUNK:
        _log.error("got closed channel while waiting for response");
        _messageState = MessageState.RESPONSE_FAILURE;
        if(_responseProcessor != null)
          _responseProcessor.channelException(new ClosedChannelException());
        break;
        default:
          _log.warn("closeChannel is called in unexpected state:" + _messageState);
      }

      _messageState = MessageState.CLOSED;
      _channelState = ChannelState.CHANNEL_EXCEPTION;
    }
    informCloseListener();
    super.channelClosed(ctx, e);
  }

  @Override
  public String toString() {
    return "GenericHttpResponseHandler ["
        + "_keepAlive=" + _keepAlive + ", _messageState="
        + _messageState + ", _channelState=" + _channelState + "]";
  }

  public synchronized String getHeader(String headerName)
  {
    String result = null;
    if (null != _httpResponse)
    {
      result = _httpResponse.getHeader(headerName);
      if (null == result && null != _httpTrailer)
      {
        result = _httpTrailer.getHeader(headerName);
      }
    }

    return result;
  }
  public StateLogger getLog() {
    return _log;
  }

  public class StateLogger {
    private final Logger _log;
    public StateLogger (Logger l) {
      _log = l;
    }
    protected StringBuilder setPrefix() {
      StringBuilder sb = new StringBuilder();
      sb.append("<").append(GenericHttpResponseHandler.this.hashCode());
      sb.append("_").append(_messageState).append(">");
      return sb;
    }
    public void info(String msg) {
      info(msg, null);
    }
    public void debug(String msg) {
      debug(msg, null);
    }
    public void warn(String msg) {
      warn(msg, null);
    }
    public void error(String msg) {
      error(msg, null);
    }
    public void info(String msg, Throwable e) {
      if(isDebugEnabled())
        msg = setPrefix().append(msg).toString();

      DbusPrettyLogUtils.logExceptionAtInfo(msg, e, _log);
    }
    public void debug(String msg, Throwable e) {
      msg = setPrefix().append(msg).toString();
      DbusPrettyLogUtils.logExceptionAtDebug(msg, e, _log);
    }
    public void warn(String msg, Throwable e) {
      msg = setPrefix().append(msg).toString();
      DbusPrettyLogUtils.logExceptionAtWarn(msg, e, _log);
    }
    public void error(String msg, Throwable e) {
      msg = setPrefix().append(msg).toString();
      DbusPrettyLogUtils.logExceptionAtError(msg, e, _log);
    }
    public boolean isDebugEnabled() {
      return _log.isDebugEnabled();
    }
    public void setLevel(org.apache.log4j.Level l) {
      _log.setLevel(l);
    }
  }
}
