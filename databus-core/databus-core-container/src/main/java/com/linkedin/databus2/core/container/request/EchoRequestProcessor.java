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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;


public class EchoRequestProcessor implements RequestProcessor {
  
  public final static String COMMAND_NAME = "echo";
  public final static String REPEAT_PARAM = "repeat";
	
	private final ExecutorService _executorService;
	
	public EchoRequestProcessor(ExecutorService executorService)
	{
		_executorService = executorService;
	}

	@Override
	public ExecutorService getExecutorService() {
		return _executorService;
	}

	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException, RequestProcessingException {
		byte[] echoBytes = request.toString().getBytes(Charset.defaultCharset());
		
		int repeat = request.getOptionalIntParam(REPEAT_PARAM, 1);
        byte[] responseBytes = new byte[repeat * (echoBytes.length + 2)]; 
		
		for (int i = 0; i < repeat; ++i)
		{
		  int destIdx = i * (echoBytes.length + 2);
		  System.arraycopy(echoBytes, 0, responseBytes, destIdx, echoBytes.length);
		  destIdx += echoBytes.length;
		  responseBytes[destIdx] = (byte)'\r';
          responseBytes[destIdx + 1] = (byte)'\n';
		}

		request.getResponseContent().write(ByteBuffer.wrap(responseBytes));
		
		return request;
	}

}
