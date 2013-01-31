package com.linkedin.databus2.core.container.request;
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
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.linkedin.databus2.core.container.request.InvalidRequestType;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Manages the container configration.
 * 
 * GET /config  - returns the current container configuration
 * POST/PUT /config?param1=value1&.... - changes the current configuration and returns the new config
 * 
 * @author cbotev
 *
 */
public class ConfigRequestProcessor implements RequestProcessor {
	
	public static final String MODULE = ConfigRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	public final static String COMMAND_NAME = "config";
	
	private final ExecutorService _executorService;
	private final ServerContainer _serverContainer;
	
	public ConfigRequestProcessor(ExecutorService executorService, ServerContainer serverContainer)
	{
		_executorService = executorService;
		_serverContainer = serverContainer;
	}

	@Override
	public ExecutorService getExecutorService() {
		return _executorService;
	}

	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException, 
			RequestProcessingException {
		LOG.debug("Method=" + request.getRequestType());
		if (request.getRequestType() == HttpMethod.GET)
		{
			doGetConfig(request);
		}
		else if (request.getRequestType() == HttpMethod.POST || request.getRequestType() == HttpMethod.PUT)
		{
			doPutConfig(request);
		}
		else 
		{
			throw new InvalidRequestType(request.getRequestType(), request.getName());
		}
		return request;
	}
	
	private void serializeConfig(ServerContainer.RuntimeConfig config, 
	                             WritableByteChannel destChannel) throws IOException
	{
      StringWriter out  = new StringWriter(10240);
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(out, config);
      out.close();
      byte[] dataBytes = out.toString().getBytes();
      destChannel.write(ByteBuffer.wrap(dataBytes));
	}
	
	private void doGetConfig(DatabusRequest request) throws IOException, 
			RequestProcessingException
	{
		ServerContainer.RuntimeConfig config = request.getConfig();
		serializeConfig(config, request.getResponseContent());
	}

	private void doPutConfig(DatabusRequest request) throws IOException, 
			RequestProcessingException
	{
		Properties cmdParams = request.getParams();
		try
		{
		  _serverContainer.getContainerRuntimeConfigMgr().loadConfig(cmdParams);
		}
		catch (InvalidConfigException ice)
		{
		  throw new RequestProcessingException("config load failed", ice);
		}
		
		ServerContainer.RuntimeConfig newConfig 
		    = _serverContainer.getContainerRuntimeConfigMgr().getReadOnlyConfig();
		serializeConfig(newConfig, request.getResponseContent());
	}

}
