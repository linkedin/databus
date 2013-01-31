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


import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpResponse;


public class HttpResponseProcessorDecorator<T extends HttpResponseProcessor> implements HttpResponseProcessor
{
  public static final String MODULE = HttpResponseProcessorDecorator.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  protected T _decorated;

  /*
   * Sometimes, Netty seems to call both ChannelException and ChannelClosed for the same issue resulting
   * in multiple messages to Relay/Bootstrap Pull Thread causing bad state in its state-machine
   */
  protected volatile boolean _errorHandled = false;

  public static enum ResponseStatus
  {
	  WAITING_FOR_FIRST_CHUNK,
	  CHUNKS_SEEN,
	  CHUNKS_FINISHED,
	  CHUNKS_EXCEPTION;

	  public boolean isActivelyProcessing()
	  {
		  return (this.equals(WAITING_FOR_FIRST_CHUNK) || this.equals(CHUNKS_SEEN));
	  }
  };

  protected volatile ResponseStatus _responseStatus;

  public HttpResponseProcessorDecorator(T decorated)
  {
    super();
    _decorated = decorated;
    _responseStatus = ResponseStatus.WAITING_FOR_FIRST_CHUNK;
  }

  @Override
  public void addChunk(HttpChunk chunk) throws Exception
  {
    if (null != _decorated)
    {
      _decorated.addChunk(chunk);
    }
    else
    {
      LOG.error("addChunk ignored; no decorated object");
    }
  }

  @Override
  public void addTrailer(HttpChunkTrailer trailer) throws Exception
  {
    if (null != _decorated)
    {
      _decorated.addTrailer(trailer);
    }
    else
    {
      LOG.error("addTrailer ignored; no decorated object");
    }
  }

  @Override
  public void finishResponse() throws Exception
  {
	_responseStatus = ResponseStatus.CHUNKS_FINISHED;

    if (null != _decorated)
    {
      _decorated.finishResponse();
    }
    else
    {
      LOG.error("finishResponse ignored; no decorated object");
    }
  }

  @Override
  public void startResponse(HttpResponse response) throws Exception
  {
	_responseStatus = ResponseStatus.CHUNKS_SEEN;

    if (null != _decorated)
    {
      _decorated.startResponse(response);
    }
    else
    {
      LOG.error("startResponse ignored; no decorated object");
    }
  }

  @Override
  public final void channelException(Throwable cause)
  {
	if ( _errorHandled )
	{
		LOG.error("Channel Exception received !! But skipping as it is already handled by the client !! " +
	              cause.getClass() + ":" + cause.getMessage());
        if (LOG.isDebugEnabled()) LOG.error(cause);
		return;
	}

	handleChannelException(cause);

	_errorHandled = true;
	_responseStatus = ResponseStatus.CHUNKS_EXCEPTION;
  }

  public void handleChannelException(Throwable cause)
  {
    if (null != _decorated)
    {
      _decorated.channelException(cause);
    }
    else
    {
      LOG.error("channel exception but no decorated object:" + cause.getClass() + ":" +
                cause.getMessage());
      if (LOG.isDebugEnabled()) LOG.error(cause);
    }
  }

  @Override
  public final void channelClosed()
  {
	if ( _responseStatus.isActivelyProcessing())
	{
		String msg = "Channel is closed even before finishing response.";
		Exception ex = new Exception(msg);
		channelException(ex);
	} else {
		if (null != _decorated)
		{
			LOG.info("channel closed and response is finished. decorated object available");
			_decorated.channelClosed();
		}
		else
		{
			LOG.error("Channel closed but no decorated object to delegate !!");
			throw new RuntimeException("channel closed but no decorated object");
		}
	}
  }
}
