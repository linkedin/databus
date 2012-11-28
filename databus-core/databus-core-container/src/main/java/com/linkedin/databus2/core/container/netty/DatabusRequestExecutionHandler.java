package com.linkedin.databus2.core.container.netty;

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.linkedin.databus2.core.container.netty.ChunkedBodyWritableByteChannel;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;

/**
 * Expects DatabusRequest objects and runs them
 * @author cbotev
 *
 */
public class DatabusRequestExecutionHandler extends SimpleChannelUpstreamHandler
{
  public static final String MODULE = DatabusRequestExecutionHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final RequestProcessorRegistry _processorRegistry;
  private final ServerContainer _serverContainer;

  private DatabusRequest _dbusRequest;
  private HttpRequest _httpRequest;

  public DatabusRequestExecutionHandler(ServerContainer serverContainer)
  {
    _serverContainer = serverContainer;
    _processorRegistry = _serverContainer.getProcessorRegistry();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    if (e.getMessage() instanceof HttpRequest)
    {
      _httpRequest = (HttpRequest)e.getMessage();
      ctx.sendUpstream(e);
    }
    if (e.getMessage() instanceof DatabusRequest)
    {
      _dbusRequest = (DatabusRequest)e.getMessage();

      //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
      /*NettyStats nettyStats = _configManager.getNettyStats();
      boolean nettyStatsEnabled = nettyStats.isEnabled();
      CallCompletion callCompletion = nettyStatsEnabled ?
          nettyStats.getRequestHandler_writeResponse().startCall() :
          null;
      CallCompletion processRequestCompletion = null;*/
      try
      {
        if (LOG.isDebugEnabled())
        {
          LOG.debug("Creating response for command [" + _dbusRequest.getId() + "] " +
                    _dbusRequest.getName());
        }

        // Decide whether to close the connection or not.
        boolean keepAlive = isKeepAlive(_httpRequest);

        HttpResponse response = generateEmptyResponse();
        if (LOG.isDebugEnabled())
        {
          //We are debugging -- let's add some more info to the response
          response.addHeader(DatabusRequest.DATABUS_REQUEST_ID_HEADER,
                             Long.toString(_dbusRequest.getId()));
        }

        // Write the response.
        ChunkedBodyWritableByteChannel responseChannel = null;
        try
        {
          responseChannel = new ChunkedBodyWritableByteChannel(e.getChannel(), response);
          _dbusRequest.setResponseContent(responseChannel);

          if (LOG.isDebugEnabled())
          {
            LOG.debug("About to run command [" + _dbusRequest.getId() + "] " + _dbusRequest.getName());
          }

          //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
          /*if (nettyStatsEnabled)
          {
            processRequestCompletion = nettyStats.getRequestHandler_processRequest().startCall();
          }*/
          Future<DatabusRequest> responseFuture = _processorRegistry.run(_dbusRequest);

          ServerContainer.RuntimeConfig config = _dbusRequest.getConfig();
          int timeoutMs = config.getRequestProcessingBudgetMs();

          boolean done = responseFuture.isDone();
          while (!done)
          {
            try
            {
              responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
              done = true;
            }
            catch (InterruptedException ie)
            {
              done = responseFuture.isDone();
            }
            catch (Exception ex)
            {
              done = true;
              _dbusRequest.setError(ex);
              //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
              /*if (null != processRequestCompletion)
              {
                processRequestCompletion.endCallWithError(ex);
                processRequestCompletion = null;
              }*/
            }
          }
        }
        finally
        {
          if (null != responseChannel)
          {
            if (LOG.isDebugEnabled())
            {
              //Add some more debugging info
              long curTimeMs = System.currentTimeMillis();
              responseChannel.addMetadata(DatabusRequest.DATABUS_REQUEST_LATENCY_HEADER,
                                          Long.toString(curTimeMs - _dbusRequest.getCreateTimestampMs()));
            }
            responseChannel.close();
          }
          if (null != _dbusRequest.getResponseThrowable())
          {
            ContainerStatisticsCollector statsCollector = _serverContainer.getContainerStatsCollector();
            if (null != statsCollector)
            {
              statsCollector.registerContainerError(_dbusRequest.getResponseThrowable());
            }
          }
       }

        //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
        /*if (null != processRequestCompletion)
        {
          processRequestCompletion.endCall();
        }*/

        if (LOG.isDebugEnabled())
        {
          LOG.debug("Done runing command [" + _dbusRequest.getId() + "] " + _dbusRequest.getName());
        }

        // Close the non-keep-alive or hard-failed connection after the write operation is done.
        if (!keepAlive || null == responseChannel)
        {
          e.getChannel().close();
        }

        //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
        /*if (null != callCompletion)
        {
          callCompletion.endCall();
        }*/
      }
      catch (RuntimeException ex)
      {
        LOG.error("HttpRequestHandler.writeResponse error", ex);
        //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
        /*if (null != callCompletion)
        {
          callCompletion.endCallWithError(ex);
        }*/
        ContainerStatisticsCollector statsCollector = _serverContainer.getContainerStatsCollector();
        if (null != statsCollector) statsCollector.registerContainerError(ex);
      }

    }
    else
    {
      //Pass on everything else
      ctx.sendUpstream(e);
    }
  }

  private HttpResponse generateEmptyResponse()
  {
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

    //response.setContent(ChannelBuffers.wrappedBuffer(responseBody));
    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
    response.setHeader("Access-Control-Allow-Origin", "*");

    // Encode the cookie.
    String cookieString = _httpRequest.getHeader(COOKIE);
    if (cookieString != null)
    {
        CookieDecoder cookieDecoder = new CookieDecoder();
        Set<Cookie> cookies = cookieDecoder.decode(cookieString);
        if(!cookies.isEmpty()) {
            // Reset the cookies if necessary.
            CookieEncoder cookieEncoder = new CookieEncoder(true);
            for (Cookie cookie : cookies) {
                cookieEncoder.addCookie(cookie);
            }
            response.addHeader(SET_COOKIE, cookieEncoder.encode());
        }
    }

    return response;
  }

}
