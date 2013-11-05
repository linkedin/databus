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
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

/**
 * Generates random data events for a source and stores them in an event buffer for testing purposes.
 * @author cbotev
 *
 */
public class GenerateDataEventsRequestProcessor implements RequestProcessor
{

  public static final String MODULE = GenerateDataEventsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String COMMAND_NAME = "genDataEvents";
  public static final String SOURCES_NAME_PARAM = "src_ids";
  public static final String SCN_PARAM = "fromScn";
  public static final String EVENTS_PER_SEC_PARAM = "eventsPerSec";
  public static final String DURATION_MS = "duration";
  public static final String NUM_EVENTS_TO_GENERATE = "eventsToGenerate";
  public static final String PERCENT_BUFFER_TO_GENERATE = "percentBuffer";
  public static final String KEY_MIN_PARAM = "keyMin";
  public static final String KEY_MAX_PARAM = "keyMax";

  private final ExecutorService _executorService;
  private final HttpRelay _relay;
  private final DbusEventsStatisticsCollector _relayStatsCollector;
  private final DatabusEventProducer _producer;

  public GenerateDataEventsRequestProcessor(ExecutorService executorService,
                                            HttpRelay relay,
                                            DatabusEventProducer eventProducer)
  {
    super();

    _executorService = executorService;
    _relay = relay;
    _producer = eventProducer;
    //currently generateDataEvents does not support generation across multiple partitions
    //for statistics, we'll make up a "virtual" partition
    _relayStatsCollector = new DbusEventsStatisticsCollector(
       _relay.getContainerStaticConfig().getId(),
       _producer.toString(),
       true,
       false,
       _relay.getMbeanServer());
       _relay.getInBoundStatsCollectors().addStatsCollector("GenerateDataEventsRequestProcessor",
                                                         _relayStatsCollector);
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
    String action = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, "");
    if (action.equals("check"))
    {
      boolean genRunning = _producer.checkRunning();
      StringBuilder resBuilder = new StringBuilder(1024);
      Formatter fmt = new Formatter(resBuilder);
      fmt.format("{\"genDataEventsRunning\":\"%b\"}", genRunning);

      request.getResponseContent().write(ByteBuffer.wrap(resBuilder.toString().getBytes()));
    }
    else if (action.equals("stop"))
    {
      _producer.stopGeneration();
      request.getResponseContent().write(ByteBuffer.wrap("{\"genDataEventsRunning\":\"send-stop\"}".getBytes()));
    } else if (action.equals("suspend"))
    {
      _producer.suspendGeneration();
      request.getResponseContent().write(ByteBuffer.wrap("{\"genDataEventsRunning\":\"send-suspend\"}".getBytes()));
    } else if (action.equals("resume"))
    {
      long numEventToGenerate = request.getOptionalLongParam(NUM_EVENTS_TO_GENERATE, Long.MAX_VALUE);
      long keyMin = request.getOptionalLongParam(KEY_MIN_PARAM, 0L);
      long keyMax = request.getOptionalLongParam(KEY_MAX_PARAM, Long.MAX_VALUE);
      int percentOfBufferToGenerate = request.getOptionalIntParam(PERCENT_BUFFER_TO_GENERATE, Integer.MAX_VALUE);
      _producer.resumeGeneration(numEventToGenerate, percentOfBufferToGenerate, keyMin, keyMax);
      request.getResponseContent().write(ByteBuffer.wrap("{\"genDataEventsRunning\":\"send-resume\"}".getBytes()));
    }
    else if (action.equals("start"))
    {
      long fromScn = request.getRequiredLongParam(SCN_PARAM);
      long durationMs = request.getRequiredLongParam(DURATION_MS);
      int eventsPerSec = request.getRequiredIntParam(EVENTS_PER_SEC_PARAM);
      long numEventToGenerate = request.getOptionalLongParam(NUM_EVENTS_TO_GENERATE, Long.MAX_VALUE);
      int percentOfBufferToGenerate = request.getOptionalIntParam(PERCENT_BUFFER_TO_GENERATE, Integer.MAX_VALUE);
      long keyMin = request.getOptionalLongParam(KEY_MIN_PARAM, 0L);
      long keyMax = request.getOptionalLongParam(KEY_MAX_PARAM, Long.MAX_VALUE);
      String sourcesListStr = request.getRequiredStringParam(SOURCES_NAME_PARAM);
      String[] sourcesStrArray = sourcesListStr.split(",");
      List<IdNamePair> sourcesIdList = new ArrayList<IdNamePair>(sourcesStrArray.length);
      for (String sourceIdStr: sourcesStrArray)
      {
        try
        {
          Integer id = Integer.valueOf(sourceIdStr);
          LogicalSource source = _relay.getSourcesIdNameRegistry().getSource(id);
          if (null != source) sourcesIdList.add(source.asIdNamePair());
          else LOG.error("unable to find source id: " + id);
        }
        catch (NumberFormatException nfe)
        {
          throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_NAME_PARAM, sourceIdStr);
        }

      }

      //We have to use the global stats collector because the generation can go beyond the lifespan
      //of the connection
      boolean tryStart = _producer.startGeneration(fromScn, eventsPerSec, durationMs,
                                                   numEventToGenerate, percentOfBufferToGenerate,
                                                   keyMin, keyMax,
                                                   sourcesIdList, _relayStatsCollector);
      StringBuilder resBuilder = new StringBuilder(1024);
      Formatter fmt = new Formatter(resBuilder);
      fmt.format("{\"genDataEventsStarted\":\"%b\"}", tryStart);

      request.getResponseContent().write(ByteBuffer.wrap(resBuilder.toString().getBytes()));
    }
    else
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "request path", action);
    }

    return request;
  }

}
