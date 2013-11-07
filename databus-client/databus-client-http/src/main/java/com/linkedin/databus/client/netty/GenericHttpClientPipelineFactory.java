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


import static org.jboss.netty.channel.Channels.pipeline;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.util.Timer;

import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.ExtendedWriteTimeoutHandler;
import com.linkedin.databus2.core.container.HttpRequestLoggingHandler;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.netty.ConnectionChannelRegistrationHandler;
import com.linkedin.databus2.core.container.netty.InboundContainerStatisticsCollectingHandler;

/**
 * Creates a pipeline for /register response processing.
 */
public class GenericHttpClientPipelineFactory implements ChannelPipelineFactory
{
  public static final String MODULE = GenericHttpClientPipelineFactory.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String READ_TIMEOUT_HANDLER_NAME = "client request read timeout handler";

  //private final HttpResponseProcessor _responseProcessor;
  private final GenericHttpResponseHandler _handler;
  private final ContainerStatisticsCollector _containerStatsCollector;
  private final Timer _timeoutTimer;
  private final long _writeTimeoutMs;
  private final long _readTimeoutMs;
  private final ChannelGroup _channelGroup; //provides automatic channel closure on shutdown

  public GenericHttpClientPipelineFactory(GenericHttpResponseHandler handler,
                                          ContainerStatisticsCollector containerStatsCollector,
                                          Timer timeoutTimer,
                                          long writeTimeoutMs,
                                          long readTimeoutMs,
                                          ChannelGroup channelGroup)
  {
    //_responseProcessor = responseProcessor;
    _handler = handler;
    _containerStatsCollector = containerStatsCollector;
    _timeoutTimer = timeoutTimer;
    _writeTimeoutMs = writeTimeoutMs;
    _readTimeoutMs = readTimeoutMs;
    _channelGroup = channelGroup;
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
      // Create a default pipeline implementation.
      ChannelPipeline pipeline = pipeline();


      if (_channelGroup != null)
        pipeline.addLast("auto group register ",
                         new ConnectionChannelRegistrationHandler(_channelGroup));

      if (Logger.getRootLogger().isTraceEnabled())
      {
        LOG.debug("Adding Netty tracing");
        pipeline.addLast("netty client traffic",
                         new LoggingHandler("netty client traffic", InternalLogLevel.DEBUG, true));
      }

      if (null != _containerStatsCollector)
      {
        pipeline.addLast("inbound statistics collector",
                         new InboundContainerStatisticsCollectingHandler(
                             _containerStatsCollector));
      }


      ExtendedReadTimeoutHandler readTimeoutHandler =
          new ExtendedReadTimeoutHandler("client call ",
                                         _timeoutTimer,
                                         _readTimeoutMs,
                                         true);
      pipeline.addLast(READ_TIMEOUT_HANDLER_NAME, readTimeoutHandler);

      pipeline.addLast("codec", new HttpClientCodec());
      pipeline.addLast("http logger", new HttpRequestLoggingHandler());

      // Remove the following line if you don't want automatic content decompression.
      pipeline.addLast("inflater", new HttpContentDecompressor());

      //pipeline.addLast("handler", new GenericHttpResponseHandler(_responseProcessor, _keepAlive));
      pipeline.addLast("handler", _handler);

      //add a handler to deal with write timeouts
      pipeline.addLast("client request write timeout handler",
                       new ExtendedWriteTimeoutHandler("netty client traffic",
                                                       _timeoutTimer,
                                                       _writeTimeoutMs,
                                                       true));
      return pipeline;
  }
}
