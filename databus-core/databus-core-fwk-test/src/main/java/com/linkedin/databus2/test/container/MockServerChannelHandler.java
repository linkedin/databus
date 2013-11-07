package com.linkedin.databus2.test.container;
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
import java.nio.channels.ClosedChannelException;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.timeout.WriteTimeoutException;

import com.linkedin.databus2.test.TestUtil;

/**
 * A simple channel handler, which allows to intercept and interfere with
 * the flow of the messages thru the channel. The class is meant
 *  for testing purposes.
 */
public class MockServerChannelHandler extends SimpleChannelHandler
{
  public static Logger LOG = Logger.getLogger(MockServerChannelHandler.class);
  private boolean _throwWTOException = false;
  private boolean _saveTheFuture = false;
  private boolean _disableWriteComplete = false;
  private boolean _delayWriteComplete = false;
  private ChannelFuture _future = null;

  public MockServerChannelHandler()
	{

	}

	public void enableThrowWTOException(boolean enable) {
		_throwWTOException = enable;
	}
	public void enableSaveTheFuture(boolean enable) {
		_saveTheFuture = enable;
	}
	public void disableWriteComplete(boolean disable) {
		_disableWriteComplete = disable;
	}
	public void delayWriteComplete(boolean delay) {
	  _delayWriteComplete = delay;
	}

	// =>=>=> UPstream from server to client
	// <=<=<= DNxtream from client to server

	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
	  Thread t = null;
	  if(LOG.isDebugEnabled())
	    LOG.debug("=>=>=>=WRite complete start" + e);
	  if(_disableWriteComplete) {
	    LOG.info("Injecting exceptions into writeComplete");
      _future.setFailure(new WriteTimeoutException("Mocked WriteTimeout"));
      _future.setFailure(new ClosedChannelException());
      TestUtil.sleep(16000); // 16s sleep
	  } else if(_delayWriteComplete) {
	    LOG.info("Injecting delay into writeComplete");
	    t = startExceptionThread(ctx);
	    LOG.info("waiting for timeout");
	    TestUtil.sleep(10);
	  }
	  super.writeComplete(ctx, e);

	  LOG.debug("=>=>=> Write complete end");
	  if(t != null)
	    t.interrupt(); // interrupt to close the channel
	}

	@Override
	public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		if(LOG.isDebugEnabled()) {
			LOG.debug("<=<=<=<=WRite requested" + e.getMessage());
			printMessage("<=<=<=<=", e);
		}
		MessageEvent newMessage = e;
		if(_saveTheFuture) {
			if(_future == null) {
				// save the real future - so we can fail it
				_future = e.getFuture();
				// to make sure client's Netty thread will not get the update - we substitute a fake future
				ChannelFuture newFuture = Channels.future(ctx.getChannel());
				newMessage = new DownstreamMessageEvent(ctx.getChannel(), newFuture, e.getMessage(), e.getRemoteAddress());
				if(LOG.isDebugEnabled())
						LOG.debug("Saving the future:" + _future);
			}
		}
		super.writeRequested(ctx, newMessage);
	}

	@Override
	public void messageReceived(
	                            ChannelHandlerContext ctx, MessageEvent e) throws Exception {
	  if(LOG.isDebugEnabled())
	    printMessage("msgReceived=>=>=>=>", e);
	  if(_throwWTOException) {
	    _throwWTOException = false;
	    // throw WriteTimeout exception
	    startExceptionThread(ctx);
	  }
	  super.messageReceived(ctx, e);
	}

	private Thread startExceptionThread(final ChannelHandlerContext ctx) {
	  Thread t = new Thread( new Runnable() {
	    @Override
	    public void run() {
	      LOG.info("***** ABOUT TO FIRE time out exception");
	      //_future.setFailure(new WriteTimeoutException("MOCK write timeout exception1"));
	      Channels.fireExceptionCaught(ctx, new WriteTimeoutException("MOCK write timeout exception2"));
	      LOG.info("**** Exception in the hole ");
	      try {
	        Thread.sleep(100000); // sleep will be interrupted
	      } catch (InterruptedException e) {
	        LOG.info("sleep interrupted");
	      }

	      ctx.getChannel().close(); //close the channel asynchronously
	    }
	  }, "Write Timeout thread");
	  t.setDaemon(true);
	  t.start();
	  return t;
	}

	//auxiliary method to print messages
	private void printMessage(String prefix, MessageEvent e) {
		Object msgO = e.getMessage();
		String resp;
		if(msgO instanceof HttpRequest) {
			HttpRequest msgReq = (HttpRequest)msgO;
			//Matcher result = pattern.matcher(msgReq.getUri());
			resp = msgReq.getUri();
		} else if(msgO instanceof HttpResponse){
			HttpResponse msgReq = (HttpResponse)msgO;
			resp = msgReq.toString();
		} else  if(msgO instanceof HttpChunk){
			HttpChunk msgReq = (HttpChunk)msgO;
			resp = msgReq.toString();
		} else {
			ChannelBuffer msg = (ChannelBuffer)msgO;
			byte[] bytes = new byte[msg.capacity()];
			msg.readBytes(bytes);
			msg.setIndex(0, bytes.length);
			StringBuilder out = new StringBuilder("MSG: ").append(e.getChannel().getRemoteAddress());
			out.append("\nMESSAGE length=").append(bytes.length).append("\n").append(new String(bytes));
			resp = out.toString();
		}
		LOG.debug(prefix + resp);
	}

}
