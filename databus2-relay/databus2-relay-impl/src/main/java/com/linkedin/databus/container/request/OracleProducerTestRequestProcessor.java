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
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.producers.db.OracleTxlogEventReader;

public class OracleProducerTestRequestProcessor implements RequestProcessor 
{
	public static final String MODULE = OracleProducerTestRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private OracleTxlogEventReader _producer = null;
	private ExecutorService _service = null;

	public static final String RESET_CATCHUPSCN_COMMAND = "resetCatchupScn";
	public static final String COMMAND_NAME = "testOracleProducer";

	public OracleProducerTestRequestProcessor(OracleTxlogEventReader producer,
			ExecutorService service)
	{
		_producer = producer;
		_service = service;
	}

	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
	RequestProcessingException, DatabusException 
	{
	    String command = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, "");

		if ( command.equalsIgnoreCase(RESET_CATCHUPSCN_COMMAND))
		{
			LOG.info("Resetting Catchup Target Max Scn !!");
			_producer.setCatchupTargetMaxScn(-1);
			String result = "{ \"command\" : \"" + command + "\", \"result\" : \"true\" }";			
			request.getResponseContent().write(ByteBuffer.wrap(result.getBytes(Charset.defaultCharset())));
		}  else {
			throw new DatabusException("Unknown command :" + command);
		}

		return request;
	}

	@Override
	public ExecutorService getExecutorService() 
	{
		return _service;
	}

}
