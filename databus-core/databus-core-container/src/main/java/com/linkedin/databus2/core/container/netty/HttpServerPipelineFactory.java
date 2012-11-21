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
package com.linkedin.databus2.core.container.netty;

import static org.jboss.netty.channel.Channels.pipeline;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;

import com.linkedin.databus2.core.container.netty.ConnectionChannelRegistrationHandler;
import com.linkedin.databus2.core.container.netty.DatabusRequestExecutionHandler;
import com.linkedin.databus2.core.container.netty.HttpRequestHandler;
import com.linkedin.databus2.core.container.netty.OutboundContainerStatisticsCollectingHandler;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;
import com.linkedin.databus2.core.container.ExtendedWriteTimeoutHandler;
import com.linkedin.databus2.core.container.HttpRequestLoggingHandler;

/**
 * Creates a standard pipeline factory for processing incoming requests
 */
public class HttpServerPipelineFactory implements ChannelPipelineFactory
{

	private final ServerContainer _serverContainer;

	public HttpServerPipelineFactory(ServerContainer serverContainer)
	{
		super();
		_serverContainer = serverContainer;
	}

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
      //TODO   DDS-305: Rework the netty stats collector to use event-based stats aggregation
      /*  NettyStats nettyStats = _serverContainer.getNettyStats();
        CallCompletion getPipelineCompletion = nettyStats.isEnabled() ?
            nettyStats.getPipelineFactory_GetPipelineCallTracker().startCall() :
            null;*/

        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        //pipeline.addLast("in traffic",
        //                 new LoggingHandler("in traffic", InternalLogLevel.INFO, true));

        pipeline.addLast("auto group register ",
                         new ConnectionChannelRegistrationHandler(_serverContainer.getHttpChannelGroup()));

        if (Logger.getRootLogger().isTraceEnabled())
        {
          pipeline.addLast("netty server traffic",
                           new LoggingHandler("netty server traffic", InternalLogLevel.DEBUG, true));
        }

        pipeline.addLast("outbound statistics collector",
                         new OutboundContainerStatisticsCollectingHandler(
                             _serverContainer.getContainerStatsCollector()));

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("http logger", new HttpRequestLoggingHandler());

        ExtendedReadTimeoutHandler readTimeoutHandler =
            new ExtendedReadTimeoutHandler("server container " + _serverContainer.getContainerStaticConfig().getId(),
                                           _serverContainer.getNetworkTimeoutTimer(),
                                           _serverContainer.getContainerStaticConfig().getReadTimeoutMs(),
                                           true);

        HttpRequestHandler reqHandler = new HttpRequestHandler(_serverContainer, readTimeoutHandler);
        pipeline.addLast("handler", reqHandler);

        // Remove the following line if you don't want automatic content compression.
        // FIXME DDS-307: Create a configuration option to control the relay response compression
        //pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("executionHandler", _serverContainer.getNettyExecHandler());

        DatabusRequestExecutionHandler dbusRequestHandler =
            new DatabusRequestExecutionHandler(_serverContainer);
        pipeline.addLast("databusRequestRunner", dbusRequestHandler);

        //add a handler to deal with write timeouts
        pipeline.addLast("server container write timeout handler",
                         new ExtendedWriteTimeoutHandler("server container " + _serverContainer.getContainerStaticConfig().getId(),
                                                         _serverContainer.getNetworkTimeoutTimer(),
                                                         _serverContainer.getContainerStaticConfig().getWriteTimeoutMs(),
                                                         true));

        //pipeline.addLast("traffic 20",
        //                 new LoggingHandler("traffic 20", InternalLogLevel.INFO, true));

        return pipeline;
    }
}
