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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.databus2.core.container.request.AbstractStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.GoldenGateEventProducer;

public class RelayStatsRequestProcessor extends AbstractStatsRequestProcessor
{

  public static final String MODULE = RelayStatsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String COMMAND_NAME = "relayStats";

  private final static String OUTBOUND_HTTP_TOTAL_KEY = "outbound/http/total";
  private final static String OUTBOUND_HTTP_SOURCES_KEY = "outbound/http/sources";
  private final static String OUTBOUND_HTTP_SOURCE_PREFIX = "outbound/http/source/";
  private final static String OUTBOUND_HTTP_CLIENTS_KEY = "outbound/http/clients";
  private final static String OUTBOUND_HTTP_CLIENT_PREFIX = "outbound/http/client/";
  private final static String INBOUND_GG_PSOURCES_PREFIX = "inbound/gg/psources/";
  private final static String INBOUND_GG_PSOURCE_PREFIX = "inbound/gg/psource/";

  private final HttpRelay _relay;

  public RelayStatsRequestProcessor(ExecutorService executorService, HttpRelay relay)
  {
    super(COMMAND_NAME, executorService);
    _relay = relay;
  }

  @Override
  public boolean doProcess(String category, DatabusRequest request)
         throws IOException, RequestProcessingException
  {
    boolean success = true;

    if (category.equals(OUTBOUND_HTTP_TOTAL_KEY))
    {
      processOutboundHttpTotalStats(request);
    }
    else if (category.equals(OUTBOUND_HTTP_SOURCES_KEY))
    {
      processOutboundHttpSourcesList(request);
    }
    else if (category.startsWith(OUTBOUND_HTTP_SOURCE_PREFIX))
    {
      processOutboundHttpSourceStats(request);
    }
    else if (category.equals(OUTBOUND_HTTP_CLIENTS_KEY))
    {
      processOutboundHttpClientsList(request);
    }
    else if (category.startsWith(OUTBOUND_HTTP_CLIENT_PREFIX))
    {
      processOutboundHttpClientStats(request);
    }
    else if (category.startsWith(INBOUND_GG_PSOURCES_PREFIX))
    {
      processInboundGGStats(request, category);
    }
    else if (category.startsWith(INBOUND_GG_PSOURCE_PREFIX)) // specific source
    {
      processInboundGGStats(request, category);
    }
    else
    {
      success = false;
    }

    return success;
  }

  private void processInboundGGStats(DatabusRequest request, String category) throws IOException, RequestProcessingException
  {
    if(!(_relay instanceof DatabusRelayMain)) {
      throw new IllegalArgumentException(category + " for relay which is not DatabusRelayMain");
    }

    String psourceName = null;
    if(category.startsWith(INBOUND_GG_PSOURCE_PREFIX)) {
      psourceName = category.substring(INBOUND_GG_PSOURCE_PREFIX.length());
      if(psourceName == null || psourceName.length() <= 0) {
        throw new InvalidRequestParamValueException(request.getName(), INBOUND_GG_PSOURCE_PREFIX, null);
      }
      LOG.info("get parser stats for source " + psourceName);
    }

    List<String> phSourceNames = new ArrayList<String>();
    EventProducer [] prods = ((DatabusRelayMain)_relay).getProducers();
    GGParserStatistics stat = null;
    for(EventProducer prod : prods) {
      if (prod != null && (prod instanceof GoldenGateEventProducer)) {
        GoldenGateEventProducer ggProducer = (GoldenGateEventProducer)prod;
        String pSrcName = ggProducer.getParserStats().getPhysicalSourceName();
        phSourceNames.add(pSrcName);
        if(psourceName != null && psourceName.equals(pSrcName)) // remember the stats object
          stat = ggProducer.getParserStats();
      }
    }

    if(psourceName != null) {
      if(stat == null)
        throw new InvalidRequestParamValueException(request.getName(), INBOUND_GG_PSOURCE_PREFIX, psourceName);
      writeJsonObjectToResponse(stat, request);
    } else {
      writeJsonObjectToResponse(phSourceNames, request);
    }

  }

  private void processOutboundHttpTotalStats(DatabusRequest request) throws IOException
  {
    DbusHttpTotalStats totalStats =  _relay.getHttpStatisticsCollector().getTotalStats();
    if (null == totalStats) return;

    writeJsonObjectToResponse(totalStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(totalStats, request);
    }
  }

  private void processOutboundHttpSourcesList(DatabusRequest request) throws IOException
  {
    List<Integer> sourcesList = _relay.getHttpStatisticsCollector().getSources();
    writeJsonObjectToResponse(sourcesList, request);
  }

  private void processOutboundHttpClientsList(DatabusRequest request) throws IOException
  {
    List<String> clientsList = _relay.getHttpStatisticsCollector().getPeers();
    writeJsonObjectToResponse(clientsList, request);
  }

  private void processOutboundHttpSourceStats(DatabusRequest request)
                                              throws IOException, RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String sourceIdStr = category.substring(OUTBOUND_HTTP_SOURCE_PREFIX.length());
    int sourceId = -1;
    try
    {
      sourceId = Integer.valueOf(sourceIdStr);
    }
    catch (NumberFormatException nfe)
    {
      throw new InvalidRequestParamValueException(request.getName(), OUTBOUND_HTTP_SOURCE_PREFIX,
                                                  sourceIdStr);
    }

    DbusHttpTotalStats sourceStats = _relay.getHttpStatisticsCollector().getSourceStats(sourceId);
    if (null == sourceStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), OUTBOUND_HTTP_SOURCE_PREFIX,
                                                  sourceIdStr);
    }

    writeJsonObjectToResponse(sourceStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(sourceStats, request);
    }

  }

  private void processOutboundHttpClientStats(DatabusRequest request)
                                              throws IOException, RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String client = category.substring(OUTBOUND_HTTP_CLIENT_PREFIX.length());

    DbusHttpTotalStats clientStats = _relay.getHttpStatisticsCollector().getPeerStats(client);
    if (null == clientStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), OUTBOUND_HTTP_CLIENT_PREFIX,
                                                  client);
    }

    writeJsonObjectToResponse(clientStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(clientStats, request);
    }
  }
}
