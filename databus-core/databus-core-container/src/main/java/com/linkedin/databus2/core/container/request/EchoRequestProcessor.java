package com.linkedin.databus2.core.container.request;

import java.io.IOException;
import java.nio.ByteBuffer;
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
		byte[] echoBytes = request.toString().getBytes();
		
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
