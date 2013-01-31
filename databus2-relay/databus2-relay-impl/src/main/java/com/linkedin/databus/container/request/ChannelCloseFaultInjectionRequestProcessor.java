package com.linkedin.databus.container.request;
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
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFuture;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

public class ChannelCloseFaultInjectionRequestProcessor implements
		RequestProcessor {
	  public static final String MODULE = ChannelCloseFaultInjectionRequestProcessor.class.getName();
	  public static final Logger LOG = Logger.getLogger(MODULE);

	private final ExecutorService _executorService;
	private final HttpRelay _relay;
	
	public ChannelCloseFaultInjectionRequestProcessor( ExecutorService executorService,
													   HttpRelay relay)
	{
		super();
		_relay = relay;
		_executorService = executorService;
	}
	
	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
			RequestProcessingException {
		// Close the channel
		
		LOG.debug("Waiting for raw channel to close");

		ChannelFuture future = request.getResponseContent().getRawChannel().close().awaitUninterruptibly();
		
		
		try {
			future.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOG.debug("Done waiting for raw channel to close");

		return request;
	}

	@Override
	public ExecutorService getExecutorService() {
		// TODO Auto-generated method stub
		return _executorService;
	}

}
