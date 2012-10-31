package com.linkedin.databus2.core.container;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Formatter;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * Logs HTTP requests in Netty. The handler tracks both upstream and downstream traffic.
 *
 * The format of the messages is:
 *
 * direction ip method url rqst_body_size resp_code resp_start_time_ms resp_end_time_ms resp_body_size
 *
 * <dl>
 *   <dt>direction</dt>
 *   <dd>&gt; for inbound requests and &lt; for outbound requests</dd>
 *
 *   <dt>ip</dt>
 *   <dd>Peer IP address</dd>
 *
 *   <dt>method</dt>
 *   <dd>HTML verb: GET, PUT, HEAD, etc. </dd>
 *
 *   <dt>url</dt>
 *   <dd>The request URL</dd>
 *
 *   <dt>rqst_body_size</dt>
 *   <dd>The size of the request body</dd>
 *
 *   <dt>resp_code</dt>
 *   <dd>The HTTP response code</dd>
 *
 *   <dt>resp_start_time_ms</dt>
 *   <dd>The time to send the first byte of the response in milliseconds</dd>
 *
 *   <dt>resp_end_time_ms</dt>
 *   <dd>The time to send the last byte of the response in milliseconds</dd>
 *
 *   <dt>resp_body_size</dt>
 *   <dd>The size of the request body</dd>
 *
 * <dl>
 *
 * Apart from the standard verbs, the logger uses CONNECT for connection starts and ends. In the
 * former case *url* is /START and *resp_end_time_ms* contains the time to connect for outgoing
 * connections . In the latter case, the *url* is /END and *resp_end_time_ms* contains the
 * total time of the connection.
 *
 * The handler relies on the logging facility to add timestamps to log records.
 *
 * <p>NOTE: The implementation is not thread-safe and is meant to be instatiated for each channel
 * pipeline.
 *
 * @author cbotev
 */
public class HttpRequestLoggingHandler extends SimpleChannelHandler
{
  public static final String MODULE = HttpRequestLoggingHandler.class.getName();
  public static final String INBOUND_MODULE = MODULE + ".in";
  public static final String OUTBOUND_MODULE = MODULE + ".out";
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final Logger INBOUND_LOG = Logger.getLogger(INBOUND_MODULE);
  public static final Logger OUTBOUND_LOG = Logger.getLogger(OUTBOUND_MODULE);

  public static final String INBOUND_DIR = ">";
  public static final String OUTBOUND_DIR = "<";

  private static final String LOG_LINE_FORMAT = "%s %s %s %s %d %d %7.2f %7.2f %d";
  private static final String CONNECT_LINE_FORMAT = "%s %s %s %s %d %d %14.2f %14.2f %d";
  private static final int MAX_SKIPPED_LOG_LINES = 500;

  private enum State
  {
    WAIT,
    INBOUND_REQUEST,
    OUTBOUND_REQUEST,
    INBOUND_RESPONSE,
    OUTBOUND_RESPONSE,
    INBOUND_RESPONSE_END
  }

  private String _peerIp = "N/A";
  private long _connRequestNano = - 1;
  private long _connStartNano = -1;
  private long _reqStartNano = -1;
  private long _respStartNano = -1;
  private long _respFinishNano = -1;
  private long _reqBytes = 0;
  private long _respBytes = 0;
  private long _connBytes = 0;
  private State _state = State.WAIT;
  private HttpRequest _request;
  private HttpResponse _response;
  private String _lastLogLine = "";
  private int _lastLogLineRepeat = 0;

  public HttpRequestLoggingHandler()
  {
  }

  @Override
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    SocketAddress remoteAddress = ctx.getChannel().getRemoteAddress();
    if (remoteAddress instanceof InetSocketAddress)
    {
      InetSocketAddress inetAddress = (InetSocketAddress)remoteAddress;

      _peerIp = inetAddress.getAddress().getHostAddress();
    }
    else
    {
      _peerIp = remoteAddress.toString();
    }

    _connStartNano = System.nanoTime();
    _connBytes = 0;
    logConnectionStart();

    super.channelConnected(ctx, e);
  }

  @Override
  public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    logConnectionEnd();
    super.channelDisconnected(ctx, e);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    if (e.getMessage() instanceof HttpRequest)
    {
      HttpRequest req = (HttpRequest)e.getMessage();
      startHttpRequest(false, req);
    }
    else if (e.getMessage() instanceof HttpResponse)
    {
      HttpResponse resp = (HttpResponse)e.getMessage();
      startHttpResponse(true, resp);
    }
    else if (e.getMessage() instanceof HttpChunk)
    {
      HttpChunk chunk = (HttpChunk)e.getMessage();
      processHttpChunk(true, chunk);
    }
    super.messageReceived(ctx, e);
  }

  @Override
  public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception
  {
    if (State.INBOUND_RESPONSE_END == _state)
    {
      _respFinishNano = System.nanoTime();
      endHttpResponse(false);
    }
    super.writeComplete(ctx, e);
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    if (e.getMessage() instanceof HttpRequest)
    {
      HttpRequest req = (HttpRequest)e.getMessage();
      startHttpRequest(true, req);
    }
    else if (e.getMessage() instanceof HttpResponse)
    {
      HttpResponse resp = (HttpResponse)e.getMessage();
      startHttpResponse(false, resp);
    }
    else if (e.getMessage() instanceof HttpChunk)
    {
      HttpChunk chunk = (HttpChunk)e.getMessage();
      processHttpChunk(false, chunk);
    }
    super.writeRequested(ctx, e);
  }

  private void startHttpRequest(boolean outbound, HttpRequest req)
  {
    _reqStartNano = System.nanoTime();
    _respBytes = 0;
    _reqBytes = 0;
    _request = req;
    _state = outbound ? State.OUTBOUND_REQUEST : State.INBOUND_REQUEST;
  }

  private void startHttpResponse(boolean outbound, HttpResponse resp)
  {
    _respStartNano = System.nanoTime();
    if (! resp.isChunked())
    {
      _respBytes = resp.getContent().readableBytes();
      _connBytes += _respBytes;
    }
    _response = resp;
    if (outbound)
    {
      _state = resp.isChunked() ? State.OUTBOUND_RESPONSE : State.WAIT;
    }
    else
    {
      _state = resp.isChunked() ? State.INBOUND_RESPONSE : State.INBOUND_RESPONSE_END;
    }

    if (State.WAIT == _state)
    {
      endHttpResponse(outbound);
    }
  }

  private void processHttpChunk(boolean outbound, HttpChunk chunk)
  {
    switch (_state)
    {
      case INBOUND_REQUEST:
      case OUTBOUND_REQUEST:
      {
        _reqBytes += chunk.getContent().readableBytes();
        break;
      }
      case OUTBOUND_RESPONSE:
      case INBOUND_RESPONSE:
      {
        _respBytes += chunk.getContent().readableBytes();
        _connBytes += chunk.getContent().readableBytes();
        if (State.INBOUND_RESPONSE == _state && chunk.isLast()) _state = State.INBOUND_RESPONSE_END;
        break;
      }
    }
  }

  private void endHttpResponse(boolean outbound)
  {
    String method = (null != _request) ? _request.getMethod().getName() : "ERR";
    String uri = (null != _request) ? _request.getUri() : "ERR";
    int respCode = (null != _response) ? _response.getStatus().getCode() : -1;

    _respFinishNano = System.nanoTime();

    StringBuilder logLineBuilder = new StringBuilder(10000);
    java.util.Formatter logFormatter = new Formatter(logLineBuilder);
    logFormatter.format(LOG_LINE_FORMAT,
                        outbound ? OUTBOUND_DIR : INBOUND_DIR,
                        _peerIp,
                        method,
                        uri,
                        _reqBytes,
                        respCode,
                        (_respStartNano - _reqStartNano) * 1.0 / 1000000.0,
                        (_respFinishNano - _reqStartNano) * 1.0 / 1000000.0,
                        _respBytes);
    log(outbound, logFormatter);
    _state = State.WAIT;
  }

  private void logConnectionStart()
  {
    StringBuilder logLineBuilder = new StringBuilder(10000);

    boolean outbound = _connRequestNano > -1;

    java.util.Formatter logFormatter = new Formatter(logLineBuilder);
    logFormatter.format(CONNECT_LINE_FORMAT,
                        outbound ? OUTBOUND_DIR : INBOUND_DIR,
                        _peerIp,
                        "CONNECT",
                        "/START",
                        0,
                        200,
                        0.0,
                        outbound ? (System.nanoTime() - _connRequestNano) * 1.0 / 1000000.0 : 0.0,
                        0);
    log(outbound, logFormatter);
  }

  private void logConnectionEnd()
  {
    StringBuilder logLineBuilder = new StringBuilder(10000);

    boolean outbound = _connRequestNano > -1;

    Formatter logFormatter = new Formatter(logLineBuilder);
    logFormatter.format(CONNECT_LINE_FORMAT,
                        outbound ? OUTBOUND_DIR : INBOUND_DIR,
                        _peerIp,
                        "CONNECT",
                        "/END",
                        0,
                        200,
                        0.0,
                        outbound ? (System.nanoTime() - _connStartNano) * 1.0 / 1000000.0 : 0.0,
                        _connBytes);
    log(outbound, logFormatter);
  }

  @Override
  public void connectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    _connRequestNano = System.nanoTime();
    super.connectRequested(ctx, e);
  }

  private void log(boolean outbound, Formatter logLine)
  {
    logLine.flush();
    String newLogLine = logLine.toString();

    boolean skipLog = true;
    int saveLastLogLineRepeat = _lastLogLineRepeat;
    if (_lastLogLine.equals(newLogLine) && _lastLogLineRepeat < MAX_SKIPPED_LOG_LINES)
    {
      ++_lastLogLineRepeat;
    }
    else
    {
      skipLog = false;
      _lastLogLine = newLogLine;
      _lastLogLineRepeat = 0;
    }

    if (!skipLog)
    {
      if (outbound)
      {
        if (0 != saveLastLogLineRepeat) OUTBOUND_LOG.debug("last line repeated: " + saveLastLogLineRepeat);
        OUTBOUND_LOG.debug(newLogLine);
      }
      else
      {
        if (0 != saveLastLogLineRepeat) INBOUND_LOG.debug("last line repeated: " + saveLastLogLineRepeat);
        INBOUND_LOG.debug(newLogLine);
      }
    }
  }

}
