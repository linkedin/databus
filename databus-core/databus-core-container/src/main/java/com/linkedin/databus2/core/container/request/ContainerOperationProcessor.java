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
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Manages the container operation request.
 *
 * GET /operation/shutdown  - shutdown the container
 *
 */
public class ContainerOperationProcessor implements RequestProcessor {

	public static final String MODULE = ContainerOperationProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	public final static String COMMAND_NAME = "operation";

	private final ExecutorService _executorService;
	private final ServerContainer _serverContainer;

	public ContainerOperationProcessor(ExecutorService executorService, ServerContainer serverContainer)
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
	    String action = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, "");
	    if (action.equals("shutdown"))
	    {
	      String pid = getPid();
	      String response="{\"Container\":\"set-shutdown\",\"pid\":\""+ pid + "\"}";
          request.getResponseContent().write(ByteBuffer.wrap(response.getBytes()));
	      Thread runThread = new Thread(new Runnable()
	      {
	        @Override
	        public void run()
	        {
	          _serverContainer.shutdown();
	        }
	      });
	      runThread.start();
	    }
	    else if (action.equals("pause"))
	    {
	      _serverContainer.pause();
	      request.getResponseContent().write(ByteBuffer.wrap("{\"Container\":\"set-pause\"}".getBytes()));
	    }
        else if (action.equals("resume"))
        {
          _serverContainer.resume();
          request.getResponseContent().write(ByteBuffer.wrap("{\"Container\":\"set-resume\"}".getBytes()));
        }
        else if (action.equals("getpid"))
        {
          String pid = getPid();
          String response="{\"pid\":\""+ pid + "\"}";
          request.getResponseContent().write(ByteBuffer.wrap(response.getBytes()));
        }
	    else
	    {
	      throw new InvalidRequestParamValueException(COMMAND_NAME, "request path", action);
	    }
	    return request;
	}

  private String getPid()
  {
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String[] values=name.split("@");
    String pid = values[0];
    return pid;
  }
}
