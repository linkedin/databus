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

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;

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

public class GenericHttpResponseHandler extends SimpleChannelHandler {
	public static final String MODULE = GenericHttpResponseHandler.class
			.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static enum KeepAliveType {
		KEEP_ALIVE, NO_KEEP_ALIVE
	}

	public static enum MessageState {
		INIT, REQUEST_START, REQUEST_SENT, START_RESPONSE, FINISH_RESPONSE, WAIT_FOR_CHUNK, ADD_CHUNK, REQUEST_FAILURE;

		public boolean hasSentRequest() {
			if (this.equals(INIT) || (this.equals(REQUEST_START))
					|| this.equals(REQUEST_FAILURE))
				return false;

			return true;
		}

		public boolean hasResponseProcessed() {
			return this.equals(FINISH_RESPONSE);
		}
	};

	public static enum ChannelState {
		CHANNEL_ACTIVE, CHANNEL_EXCEPTION, CHANNEL_CLOSED
	};

	private final HttpResponseProcessor _responseProcessor;
	private final KeepAliveType _keepAlive;
	private MessageState _messageState;
	private ChannelState _channelState;

	public GenericHttpResponseHandler(HttpResponseProcessor responseProcessor,
			KeepAliveType keepAlive) {
		super();
		_responseProcessor = responseProcessor;
		_keepAlive = keepAlive;
		reset();
	}

	public void reset() {
		_messageState = MessageState.INIT;
		_channelState = ChannelState.CHANNEL_ACTIVE;
	}

	@Override
	public synchronized void channelBound(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		LOG.info("channel to peer bound: " + e.getChannel().getRemoteAddress());
		super.channelBound(ctx, e);
	}

	@Override
	public synchronized void writeComplete(ChannelHandlerContext ctx,
			WriteCompletionEvent e) throws Exception {
		// Future should be done by this time
		ChannelFuture future = e.getFuture();

		boolean success = future.isSuccess();
		if (!success) {
			LOG.error("Write request failed with cause :" + future.getCause());
			_messageState = MessageState.REQUEST_FAILURE;
		} else {
			LOG.debug("Write Completed successfully :" + e);
			_messageState = MessageState.REQUEST_SENT;
		}
		super.writeComplete(ctx, e);
	}

	@Override
	public synchronized void writeRequested(ChannelHandlerContext ctx,
			MessageEvent e) throws Exception 
	{
		if ( e.getMessage() instanceof HttpRequest)
		{
			_messageState = MessageState.REQUEST_START;
			if (LOG.isDebugEnabled())
				LOG.debug("Write Requested  :" + e);
		}
		super.writeRequested(ctx, e);
	}

	@Override
	public synchronized void messageReceived(ChannelHandlerContext ctx,
			MessageEvent e) throws Exception {
		if (null == _responseProcessor) {
			LOG.error("No response processor set");
			super.messageReceived(ctx, e);
		} else {
			if (e.getMessage() instanceof HttpResponse) {
				HttpResponse response = (HttpResponse) e.getMessage();
				_messageState = MessageState.START_RESPONSE;
				_responseProcessor.startResponse(response);
				if (!response.isChunked()) {
					_messageState = MessageState.FINISH_RESPONSE;
					_responseProcessor.finishResponse();
					if (_keepAlive == KeepAliveType.NO_KEEP_ALIVE) {
						e.getChannel().close();
					}
				} else {
					_messageState = MessageState.WAIT_FOR_CHUNK;
				}
			} else if (e.getMessage() instanceof HttpChunkTrailer) {
				_responseProcessor
						.addTrailer((HttpChunkTrailer) e.getMessage());
				_messageState = MessageState.FINISH_RESPONSE;
				_responseProcessor.finishResponse();
				if (_keepAlive == KeepAliveType.NO_KEEP_ALIVE) {
					e.getChannel().close();
				}
			} else if (e.getMessage() instanceof HttpChunk) {
				_messageState = MessageState.ADD_CHUNK;
				_responseProcessor.addChunk((HttpChunk) e.getMessage());
			} else {
				LOG.error("Uknown object type:"
						+ e.getMessage().getClass().getName());
			}
		}
	}



	@Override
	public synchronized void exceptionCaught(ChannelHandlerContext ctx,
			ExceptionEvent e) throws Exception 
	{

		if (!_messageState.hasSentRequest()) {
			LOG.info("Got Channel Close message even before request has been sent !! Skipping Channel Close message !!");
		} else {
			Throwable cause = e.getCause();

			if (cause instanceof java.net.ConnectException) {
				LOG.error("Unable to connect ");
			} else if (!(cause instanceof ClosedChannelException)) {
				LOG.error(
						"http client exception("
								+ cause.getClass().getSimpleName() + "):"
								+ cause.getMessage(), cause);
			}

			if (null != _responseProcessor) {
				// ClosedChannelException would always be accompanied with
				// channelClosed() callback
				if (!(cause instanceof ClosedChannelException)) {
					_responseProcessor.channelException(e.getCause());
					_channelState = ChannelState.CHANNEL_EXCEPTION;
				}
			}
		}
		super.exceptionCaught(ctx, e);
	}

	@Override
	public synchronized void channelClosed(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		Channel channel = e.getChannel();
		SocketAddress a = (null != channel) ? channel.getRemoteAddress() : null;
		LOG.info("channel to peer closed: " + a);

		_channelState = ChannelState.CHANNEL_CLOSED;

		 if (!_messageState.hasSentRequest()) {
			LOG.warn("Got Channel Close message even before request has been sent !! Skipping Channel Close message !!");
		} else {
			try {
				if ((!_messageState.hasResponseProcessed())
						&& (null != _responseProcessor)) {
					_responseProcessor.channelClosed();
				}
			} catch (RuntimeException re) {
				LOG.error("runtime exception while closing channel: "
						+ re.getClass() + ":" + re.getMessage());
				if ((!_messageState.hasResponseProcessed())
						&& (null != _responseProcessor)) {
					_responseProcessor.channelException(re);
					_channelState = ChannelState.CHANNEL_EXCEPTION;
				}
			}
		}
		super.channelClosed(ctx, e);
	}

	@Override
	public String toString() {
		return "GenericHttpResponseHandler ["
				+ "_keepAlive=" + _keepAlive + ", _messageState="
				+ _messageState + ", _channelState=" + _channelState + "]";
	}
}