package com.linkedin.databus2.core.container.netty;
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
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.request.DatabusRequest;

/**
 *
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

	public static final String MODULE = HttpRequestHandler.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

  private final ServerContainer _serverContainer;
	private final ExtendedReadTimeoutHandler _readTimeoutHandler;

    private HttpRequest request;
    private boolean readingChunks = false;

    private DatabusRequest dbusRequest;
    private ArrayList<ChannelBuffer> body = new ArrayList<ChannelBuffer>();

	public HttpRequestHandler(ServerContainer serverContainer,
	                          ExtendedReadTimeoutHandler readTimeoutHandler)
	{
		super();
		_serverContainer = serverContainer;
		_readTimeoutHandler = readTimeoutHandler;
	}

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
      /*NettyStats nettyStats = _configManager.getNettyStats();
      CallCompletion callCompletion = nettyStats.isEnabled() ?
          nettyStats.getRequestHandler_messageRecieved().startCall() :
          null;*/
      try
      {
        if (!readingChunks) {
            request = (HttpRequest) e.getMessage();
            ctx.sendUpstream(e);

            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
            String queryPath = queryStringDecoder.getPath();
            int slashPos = queryPath.indexOf('/', 1);
            if (slashPos < 0)
            {
            	slashPos = queryPath.length();
            }
            String cmdName = queryPath.substring(1, slashPos);
            ServerContainer.RuntimeConfig config = _serverContainer.getContainerRuntimeConfigMgr()
                .getReadOnlyConfig();

            if (LOG.isDebugEnabled())
            {
              LOG.debug("Got command: " + cmdName);
            }


            dbusRequest = new DatabusRequest(cmdName, request.getMethod(), e.getRemoteAddress(),
                                             config);
            if (LOG.isDebugEnabled())
            {
              LOG.debug("Starting processing command [" + dbusRequest.getId() + "] " +
                        dbusRequest.getName() );
            }

            Properties requestProps = dbusRequest.getParams();

            if (slashPos < queryPath.length())
            {
            	requestProps.put(DatabusRequest.PATH_PARAM_NAME, queryPath.substring(slashPos + 1));
            }

            for (Map.Entry<String, String> h: request.getHeaders()) {
            	handleHttpHeader(h);
            }

            Map<String, List<String>> params = queryStringDecoder.getParameters();
            if (!params.isEmpty())
            {
                for (Entry<String, List<String>> p: params.entrySet())
                {
                    String key = p.getKey();
                    List<String> vals = p.getValue();
                    if (vals.size() == 1)
                    {
                    	requestProps.put(key, vals.get(0));
                    }
                    else
                    {
                    	requestProps.put(key, vals);
                    }
                    for (String val : vals) {
                        LOG.trace("PARAM: " + key + " = " + val);
                    }
                }
            }

            if (request.isChunked())
            {
              if (null != _readTimeoutHandler)
              {
                readingChunks = true;
                _readTimeoutHandler.start(ctx.getPipeline().getContext(_readTimeoutHandler));
              }
            }
            else
            {
                ChannelBuffer content = request.getContent();
            	handleRequestContentChunk(content);
                writeResponse(ctx, e);
            }
        }
        else if (e.getMessage() instanceof HttpChunk)
        {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                readingChunks = false;
                LOG.trace("END OF CONTENT");

                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                for (Map.Entry<String, String> h: trailer.getHeaders())
                {
                	handleHttpHeader(h);
                }

                writeResponse(ctx, e);
            } else {
            	ChannelBuffer content = chunk.getContent();
            	handleRequestContentChunk(content);
            }
        }

        ctx.sendUpstream(e);

        //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
        /*if (null != callCompletion)
        {
          callCompletion.endCall();
        }*/
      }
      catch (Exception ex)
      {
        LOG.error("HttpRequestHandler.messageReceived error", ex);
        //FIXME   DDS-305: Rework the netty stats collector to use event-based stats aggregation
        /*if (null != callCompletion)
        {
          callCompletion.endCallWithError(ex);
        }*/
      }
    }

    private void writeResponse(ChannelHandlerContext ctx, MessageEvent e) {
      try
      {
    	if (HttpMethod.POST != dbusRequest.getRequestType())
    	{
    		dbusRequest.getParams().put(DatabusRequest.DATA_PARAM_NAME, body);
    	}
    	else
    	{
    		//FIXME -- parse the body
    	}

    	//done with processing the request -- turn off the read time out
    	if (null != _readTimeoutHandler && _readTimeoutHandler.isStarted())
    	{
    	  _readTimeoutHandler.stop();
    	}

    	ctx.sendUpstream(new UpstreamMessageEvent(e.getChannel(), dbusRequest,
    	                                          e.getRemoteAddress()));

      }
      catch (Exception ex)
      {
        LOG.error("HttpRequestHandler.writeResponse error", ex);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception
    {
      Throwable cause = e.getCause();
      if (cause instanceof OutOfMemoryError) 
      {
    	  LOG.error("Running out of memory. Initiating a shutdown on the server container");
    	  _serverContainer.shutdownAsynchronously();
      }

      boolean logError = true;
      if (cause instanceof ClosedChannelException)
      {
        //ignore
        logError = false;
      }
      else if (cause instanceof IOException)
      {
        logError = ! (cause.getMessage().contains("Connection reset by peer"));
      }

      if (logError)
      {
        LOG.error("exception detected: " + cause.getMessage(), cause);
      }

      if (e.getChannel().isOpen()) e.getChannel().close();
    }

    private void handleRequestContentChunk(ChannelBuffer chunk)
    {
    	if (chunk.readable())
    	{
    		body.add(chunk);
    		if (LOG.isTraceEnabled())
    		{
    		  LOG.trace("CHUNK: " + chunk.readableBytes());
    		}
    	}
    }

    private void handleHttpHeader(Map.Entry<String, String> h)
    {
        Properties requestProps = dbusRequest.getParams();

    	String headerKey = h.getKey().toLowerCase();
        LOG.trace("HEADER: " + h.getKey() + " = " + h.getValue());
        if (headerKey.startsWith(DatabusHttpHeaders.DATABUS_HTTP_HEADER_PREFIX))
        {
        	String headerParamName = headerKey.substring(DatabusHttpHeaders.DATABUS_HTTP_HEADER_PREFIX.length());
        	requestProps.put(headerParamName, h.getValue());
        }
    }
}
