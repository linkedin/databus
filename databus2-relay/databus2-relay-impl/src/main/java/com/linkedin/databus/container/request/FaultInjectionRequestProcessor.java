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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

/*
 * This wrapper processor delegates the request to real or fake request processor 
 * depending upon user input.
 * 
 * Users can fake the commands using http://..../useFake?command=<command>
 */
public class FaultInjectionRequestProcessor implements RequestProcessor 
{
	public static final String MODULE = FaultInjectionRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	  
	private final ExecutorService _executorService;
	private final HttpRelay _relay;
	
	private Map<String, RequestProcessor> _realProcessors;
	private Map<String, RequestProcessor> _fakeProcessors;
	private Set<String> _commandsToFake;
	
	public static final String USE_FAKE_COMMAND = "useFake";
	public static final String USE_REAL_COMMAND = "useReal";
	public static final String COMMAND_KEY_NAME = "cmd";
	 
	public FaultInjectionRequestProcessor( ExecutorService executorService,
										   HttpRelay relay)
	{
		super();
		_relay = relay;
		_executorService = executorService;
		
		_realProcessors = new HashMap<String, RequestProcessor>();
		_fakeProcessors = new HashMap<String, RequestProcessor>();

		_commandsToFake = new HashSet<String>();		
	}
	
	public void register(String command, RequestProcessor realProcessor, RequestProcessor fakeProcessor, boolean startFaking)
	{
		_realProcessors.put(command, realProcessor);
		_fakeProcessors.put(command, fakeProcessor);
		if (startFaking)
			addCommandsToFake(command);
	}
	
	public void addCommandsToFake(String command)
	{
		if (! _fakeProcessors.containsKey(command))
			throw new RuntimeException("Command (" + command + ") cannot be faked as there is no fake processor set !!");
		
		LOG.info("Command (" + command + ") to be faked !!");
		_commandsToFake.add(command);
	}
	
	public void removeFakeCommand(String command)
	{
		LOG.info("Command (" + command + ") will not be faked anymore !!");
		_commandsToFake.remove(USE_FAKE_COMMAND);
	}
	
	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
			RequestProcessingException, DatabusException 
	{
		String command = request.getName();
		
		if ( command.equalsIgnoreCase(USE_FAKE_COMMAND))
		{
			String cmd = request.getRequiredStringParam(COMMAND_KEY_NAME);
			addCommandsToFake(cmd);
			String result = "{ \"fake\": \"true\", \"command\" : \"" + cmd + "\", \"result\" : \"true\" }";			
			request.getResponseContent().write(ByteBuffer.wrap(result.getBytes()));
		} else if (command.equalsIgnoreCase(USE_REAL_COMMAND)) {
			String cmd = request.getRequiredStringParam(COMMAND_KEY_NAME);
			removeFakeCommand(cmd);
			String result = "{ \"fake\": \"false\", \"command\" : \"" + cmd + "\", \"result\" : \"true\" }";
			request.getResponseContent().write(ByteBuffer.wrap(result.getBytes()));
		} else {
			// Relay Commands
			if ( _commandsToFake.contains(command)) 
			{
				LOG.debug("Executing fake processor for command :(" + command + ")");
				_fakeProcessors.get(command).process(request);
			} else {
				LOG.debug("Executing real processor for command :(" + command + ")");
				_realProcessors.get(command).process(request);
			}
		}
		
		return request;
	}

	@Override
	public ExecutorService getExecutorService() {
		// TODO Auto-generated method stub
		return _executorService;
	}

}
