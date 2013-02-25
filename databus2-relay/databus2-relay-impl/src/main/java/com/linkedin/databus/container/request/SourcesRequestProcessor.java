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
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

public class SourcesRequestProcessor implements RequestProcessor
{

  public static final String MODULE = SourcesRequestProcessor.class.getName();
  public static Logger LOG = Logger.getLogger(MODULE);
  public static final String COMMAND_NAME = "sources";
  public static final String VERSION_PARAM_NAME = "v";

  private final ExecutorService _executorService;
  private final HttpRelay _relay;

  public SourcesRequestProcessor(ExecutorService executorService,
                                 HttpRelay relay)
  {
    super();
    _executorService = executorService;
    _relay = relay;
  }

  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    int protoVersion = request.getOptionalIntParam(VERSION_PARAM_NAME, 1);

    ObjectMapper mapper = new ObjectMapper();
    StringWriter out = new StringWriter(10240);

    Collection<LogicalSource> sources = _relay.getSourcesIdNameRegistry().getAllSources();
    if (1 == protoVersion)
    {
      ArrayList<IdNamePair> sourcePairs = new ArrayList<IdNamePair>(sources.size());
      for (LogicalSource source: sources) sourcePairs.add(new IdNamePair(source.getId().longValue(), source.getName()));
      mapper.writeValue(out, sourcePairs);
    }
    else if (2 == protoVersion) mapper.writeValue(out, sources);
    else throw new InvalidRequestParamValueException(COMMAND_NAME, VERSION_PARAM_NAME,
                                                     Integer.toString(protoVersion));


    byte[] resultBytes = out.toString().getBytes();

    request.getResponseContent().write(ByteBuffer.wrap(resultBytes));

    HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
    if (null != relayStatsCollector)
    {
      HttpStatisticsCollector connStatsCollector = (HttpStatisticsCollector)
          request.getParams().get(relayStatsCollector.getName());
      if (null != connStatsCollector)
      {
        connStatsCollector.registerSourcesCall();
      }
      else
      {
        relayStatsCollector.registerSourcesCall();
      }
    }

    return request;
  }

}
