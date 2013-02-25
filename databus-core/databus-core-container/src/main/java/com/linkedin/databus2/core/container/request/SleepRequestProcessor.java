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
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;


public class SleepRequestProcessor implements RequestProcessor {

	public static final String MODULE = SleepRequestProcessor.class.getName();
	public static Logger LOG = Logger.getLogger(MODULE);
	public final static String COMMAND_NAME = "sleep";

	public static final String MIN_SLEEP_TIME_PARAM = "sleepMin";
	public static final String MAX_SLEEP_TIME_PARAM = "sleepMax";
    public static final String MIN_RESP_SIZE_PARAM = "responseSizeMin";
    public static final String MAX_RESP_SIZE_PARAM = "responseSizeMax";
	public static final int DEFAULT_MIN_SLEEP_TIME_MS = 0;
	public static final int DEFAULT_MAX_SLEEP_TIME_MS = 100;
	public static final int DEFAULT_MIN_RESP_SIZE = 1000;
    public static final int DEFAULT_MAX_RESP_SIZE = 10000;

    private static final int DEFAULT_RESPONSE_CHUNK_SIZE = 32000;
    private static final String BODY_PATTERN = "DeAdBeEf";

	private final ExecutorService _executorService;

	public SleepRequestProcessor(ExecutorService executorService)
	{
		_executorService = executorService;
	}

	@Override
	public ExecutorService getExecutorService() {
		return _executorService;
	}

	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
			RequestProcessingException {

	    long startNanos = System.nanoTime();

		int minTime = request.getOptionalIntParam(MIN_SLEEP_TIME_PARAM, DEFAULT_MIN_SLEEP_TIME_MS);
		int maxTime = request.getOptionalIntParam(MAX_SLEEP_TIME_PARAM, DEFAULT_MAX_SLEEP_TIME_MS);
		if (minTime > maxTime)
		{
			throw new InvalidRequestParamValueException(request.getName(), MIN_SLEEP_TIME_PARAM, "minTime > maxTime");
		}

        int minRespSize = request.getOptionalIntParam(MIN_RESP_SIZE_PARAM, DEFAULT_MIN_RESP_SIZE);
        int maxRespSize = request.getOptionalIntParam(MAX_RESP_SIZE_PARAM, DEFAULT_MAX_RESP_SIZE);
        if (minRespSize > maxRespSize)
        {
            throw new InvalidRequestParamValueException(request.getName(), MIN_SLEEP_TIME_PARAM,
                                                        "responseSizeMin > responseSizeMax");
        }

		Random rng = new Random();

		int respSize = minRespSize + rng.nextInt(maxRespSize - minRespSize);
		ByteBuffer bb = ByteBuffer.allocate(DEFAULT_RESPONSE_CHUNK_SIZE).order(ByteOrder.nativeOrder());

		int patternSize = BODY_PATTERN.getBytes().length;

		for (int i = 0; i < respSize / DEFAULT_RESPONSE_CHUNK_SIZE; ++i)
		{
          bb.clear();
		  for (int j = 0; j < DEFAULT_RESPONSE_CHUNK_SIZE / patternSize; ++j)
		  {
		    bb.put(BODY_PATTERN.getBytes());
		  }

          bb.rewind();
		  request.getResponseContent().write(bb);
		}

		if (respSize % DEFAULT_RESPONSE_CHUNK_SIZE > 0)
		{
          bb.clear();
          for (int j = 0; j <= (respSize % DEFAULT_RESPONSE_CHUNK_SIZE) / patternSize; ++j)
          {
            bb.put(BODY_PATTERN.getBytes());
          }

          bb.flip();
          request.getResponseContent().write(bb);
		}

        int sleepTime = maxTime > minTime ? minTime + rng.nextInt(maxTime - minTime) : 0;

		long endNanos = System.nanoTime();
		long elapsedMillis = (int)((endNanos - startNanos + 1)/1000000);
		if (sleepTime > elapsedMillis)
		try
		{
			Thread.sleep(sleepTime - elapsedMillis);
		}
		catch (InterruptedException ie)
		{
		}

		return request;
	}

}
