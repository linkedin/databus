package com.linkedin.databus2.core.container;
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


import java.nio.ByteOrder;
import java.util.List;
import java.util.Vector;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.container.request.SimpleBinaryDatabusRequestDecoder;
import com.linkedin.databus2.core.container.request.SimpleBinaryDatabusResponseEncoder;

/** Used only for Espresso (databus3-relay) unit tests. */
public class DummyPipelineFactory
{
  public static final String RESPONSE_AGGREGATOR_NAME = "response aggregator";
  public static final String COMMAND_CAPTURE_NAME = "command capture";

  private static final int READ_TIMEOUT_MS = 500;

  public static class DummyServerPipelineFactory implements ChannelPipelineFactory
  {
    private final CommandsRegistry _cmdsRegistry;
    private final ByteOrder _byteOrder;

    public DummyServerPipelineFactory(CommandsRegistry cmdsRegistry, ByteOrder byteOrder)
    {
      _cmdsRegistry = cmdsRegistry;
      _byteOrder = byteOrder;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
      ChannelPipeline pipeline = Channels.pipeline();

      ExtendedReadTimeoutHandler readTimeoutHandler =
          new ExtendedReadTimeoutHandler("server container 1", null, READ_TIMEOUT_MS, true);
      pipeline.addLast("read timeout", readTimeoutHandler);

      pipeline.addLast("request decoder",
                       new SimpleBinaryDatabusRequestDecoder(_cmdsRegistry, readTimeoutHandler, _byteOrder));
      pipeline.addLast(COMMAND_CAPTURE_NAME, new SimpleObjectCaptureHandler());
      pipeline.addLast(SimpleBinaryDatabusRequestDecoder.REQUEST_EXEC_HANDLER_NAME,
                       new SimpleObjectCaptureHandler());
      pipeline.addLast("response encoder", new SimpleBinaryDatabusResponseEncoder());

      return pipeline;
    }
  }

  public static class DummyClientPipelineFactory implements ChannelPipelineFactory
  {

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
      ChannelPipeline pipeline = Channels.pipeline();
      pipeline.addLast(RESPONSE_AGGREGATOR_NAME,
                       new SimpleResponseBytesAggregatorHandler(BinaryProtocol.BYTE_ORDER));

      return pipeline;
    }
  }

  /**
   * A simple channel handler which records all message that  pass through it. The class is meant
   * mostly for testing purposes.*/
  public static class SimpleObjectCaptureHandler extends SimpleChannelUpstreamHandler
  {

    private final List<Object> _messages;

    public SimpleObjectCaptureHandler()
    {
      _messages = new Vector<Object>();
    }

    public void clear()
    {
      _messages.clear();
    }

    public List<Object> getMessages()
    {
      return _messages;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
      _messages.add(e.getMessage());
      super.messageReceived(ctx, e);
    }

  }

  /**
   * A simple handler for testing purposes. It aggregates all ChannelBuffer responses into a single
   * channel buffer. */
  public static class SimpleResponseBytesAggregatorHandler extends SimpleChannelUpstreamHandler
  {
    private final ChannelBuffer _buffer;

    public SimpleResponseBytesAggregatorHandler(ByteOrder byteOrder)
    {
      _buffer = new DynamicChannelBuffer(byteOrder, 1000);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
      if (e.getMessage() instanceof ChannelBuffer)
      {
        ChannelBuffer buf = (ChannelBuffer)e.getMessage();
        _buffer.writeBytes(buf);
      }

      super.messageReceived(ctx, e);
    }

    public ChannelBuffer getBuffer()
    {
      return _buffer;
    }

    public void clear()
    {
      _buffer.clear();
    }

  }


}
