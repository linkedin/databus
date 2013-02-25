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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.schemas.SchemaRegistryService;

public class RegisterRequestProcessor implements RequestProcessor
{
  public static final String MODULE = RegisterRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public final static String COMMAND_NAME = "register";
  public final static String SOURCES_PARAM = "sources";

  private final ExecutorService _executorService;
  private final HttpRelay _relay;

  public RegisterRequestProcessor(ExecutorService executorService, HttpRelay relay)
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
    try {
      Collection<LogicalSource> logicalSources = null;
      ArrayList<RegisterResponseEntry> registeredSources = new ArrayList<RegisterResponseEntry>(20);
      HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();

      String sources = request.getParams().getProperty(SOURCES_PARAM);
      if (null == sources)
      {
        //return all schemas
        logicalSources = _relay.getSourcesIdNameRegistry().getAllSources();
      }
      else
      {
        String[] sourceIds = sources.split(",");
        logicalSources = new ArrayList<LogicalSource>(sourceIds.length);

        for (String sourceId: sourceIds)
        {
          int srcId;
          String trimmedSourceId = sourceId.trim();
          try
          {
            srcId = Integer.valueOf(trimmedSourceId);
            LogicalSource lsource = _relay.getSourcesIdNameRegistry().getSource(srcId);
            if (null != lsource) logicalSources.add(lsource);
            else
            {
              LOG.error("No source name for source id: " + srcId);
              throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM, sourceId);
            }
          }
          catch (NumberFormatException nfe)
          {
            relayStatsCollector.registerInvalidRegisterCall();
            throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM, sourceId);
          }
        }
      }

      SchemaRegistryService schemaRegistry = _relay.getSchemaRegistryService();

      for (LogicalSource lsource: logicalSources)
      {
        Map<Short, String> versionedSchemas = null;
        try
        {
          versionedSchemas = schemaRegistry.fetchAllSchemaVersionsByType(lsource.getName());
        }
        catch (DatabusException ie)
        {
          if (null != relayStatsCollector) relayStatsCollector.registerInvalidRegisterCall();
          throw new RequestProcessingException(ie);
        }

        if ((null == versionedSchemas) || ( versionedSchemas.isEmpty()))
        {
          if (null != relayStatsCollector) relayStatsCollector.registerInvalidRegisterCall();
          LOG.error("Problem with sourceId " + lsource.getId() + " sources string = " + sources);
        } else {

          for (Entry<Short, String> e : versionedSchemas.entrySet())
          {
            registeredSources.add(new RegisterResponseEntry(lsource.getId().longValue(),
                                                            e.getKey(),e.getValue()));
          }
        }
      }

      StringWriter out = new StringWriter(102400);
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(out, registeredSources);

      byte[] resultBytes = out.toString().getBytes();

      request.getResponseContent().write(ByteBuffer.wrap(resultBytes));


      if (null != relayStatsCollector)
      {
        HttpStatisticsCollector connStatsCollector = (HttpStatisticsCollector)
        request.getParams().get(relayStatsCollector.getName());
        if (null != connStatsCollector)
        {
          connStatsCollector.registerRegisterCall(registeredSources);
        }
        else
        {
          relayStatsCollector.registerRegisterCall(registeredSources);
        }
      }

      return request;

  }
  catch (InvalidRequestParamValueException e)
  {
    HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
    if (null != relayStatsCollector)
      relayStatsCollector.registerInvalidRegisterCall();
    throw e;
  }
}

}
