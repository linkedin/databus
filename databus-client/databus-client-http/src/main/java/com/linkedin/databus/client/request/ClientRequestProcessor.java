package com.linkedin.databus.client.request;
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



import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.AbstractRequestProcesser;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.log4j.Logger;


/**
 * Created with IntelliJ IDEA. User: ssubrama Date: 10/8/12 Time: 4:05 PM To change this template use File | Settings |
 * File Templates.
 */
public class ClientRequestProcessor extends AbstractRequestProcesser
{
  public static final String MODULE = ClientRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String COMMAND_NAME = "clientCommand";
  public static final String CLIENT_INFO_KEY = "printClientInfo";
  public static final String RESET_RELAY_CONNECTIONS = "resetRelayConnections";

  private final DatabusHttpClientImpl _client;
  private final ExecutorService _executorService;

  public ClientRequestProcessor(ExecutorService executorService, DatabusHttpClientImpl client)
  {
    _executorService = executorService;
    _client = client;
  }

  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request)
  throws IOException, RequestProcessingException, DatabusException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    if (category == null)
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "category", "null");
    }
    LOG.info("Processing command " + category);

    if (category.equals(CLIENT_INFO_KEY))
    {
      Map<String, String> outMap = null;
      outMap = _client.printClientInfo();
      writeJsonObjectToResponse(outMap, request);
    }
    else if (category.equals(RESET_RELAY_CONNECTIONS))
    {
      _client.resetRelayConnections();
    }
    else
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "category", category);
    }

    return request;
  }
}
