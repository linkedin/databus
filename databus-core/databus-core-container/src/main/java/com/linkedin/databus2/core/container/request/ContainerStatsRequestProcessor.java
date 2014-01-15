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
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.avro.io.JsonEncoder;
import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStats;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStats;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.databus2.core.container.netty.ServerContainer;

public class ContainerStatsRequestProcessor extends AbstractStatsRequestProcessor
{

  public static final String MODULE = ContainerStatsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String COMMAND_NAME = "containerStats";
  private final static String OUTBOUND_EVENTS_TOTAL_KEY = "outbound/events/total";
  private final static String OUTBOUND_EVENTS_SOURCES_KEY = "outbound/events/sources";
  private final static String OUTBOUND_EVENTS_SOURCE_PREFIX = "outbound/events/source/";
  private final static String OUTBOUND_EVENTS_PSOURCES_KEY = "outbound/events/psources";
  protected final static String OUTBOUND_EVENTS_PSOURCE_PREFIX = "outbound/events/psource/";
  private final static String OUTBOUND_EVENTS_CLIENTS_KEY = "outbound/events/clients";
  private final static String OUTBOUND_EVENTS_CLIENT_PREFIX = "outbound/events/client/";
  private final static String INBOUND_EVENTS_TOTAL_KEY = "inbound/events/total";
  private final static String INBOUND_EVENTS_SOURCES_KEY = "inbound/events/sources";
  private final static String INBOUND_EVENTS_SOURCE_PREFIX = "inbound/events/source/";
  private final static String INBOUND_EVENTS_PSOURCES_KEY = "inbound/events/psources";
  protected final static String INBOUND_EVENTS_PSOURCE_PREFIX = "inbound/events/psource/";

  private final ServerContainer _container;
  private final ContainerStatisticsCollector _containerStatsCollector;

  public ContainerStatsRequestProcessor(ExecutorService executorService,
                                        ServerContainer container)
  {
    super(COMMAND_NAME, executorService);
    _container = container;
    _containerStatsCollector = _container.getContainerStatsCollector();
  }


  @Override
  public boolean doProcess(String category, DatabusRequest request)
         throws IOException, RequestProcessingException
  {
    boolean success = true;

    if (category.equals(OUTBOUND_EVENTS_TOTAL_KEY))
    {
      processEventsTotalStats(_container.getOutboundEventStatisticsCollector(), request);
    }
    else if (category.equals(OUTBOUND_EVENTS_SOURCES_KEY))
    {
      processEventsSourcesList(_container.getOutboundEventStatisticsCollector(), request);
    }
    else if (category.equals(OUTBOUND_EVENTS_PSOURCES_KEY))
    {
      processEventsPhysicalSourcesList(_container.getOutBoundStatsCollectors(), request);
    }
    else if (category.startsWith(OUTBOUND_EVENTS_SOURCE_PREFIX))
    {
      processEventsSourceStats(_container.getOutboundEventStatisticsCollector(),
                               OUTBOUND_EVENTS_SOURCE_PREFIX,
                               request);
    }
    else if (category.startsWith(OUTBOUND_EVENTS_PSOURCE_PREFIX))
    {
      processPhysicalPartitionStats(_container.getOutBoundStatsCollectors(),
    		  					OUTBOUND_EVENTS_PSOURCE_PREFIX,
                               request);
    }
    else if (category.equals(OUTBOUND_EVENTS_CLIENTS_KEY))
    {
      processEventsPeersList(_container.getOutboundEventStatisticsCollector(), request);
    }
    else if (category.startsWith(OUTBOUND_EVENTS_CLIENT_PREFIX))
    {
      processEventsPeerStats(_container.getOutboundEventStatisticsCollector(),
                             OUTBOUND_EVENTS_CLIENT_PREFIX,
                             request);
    }
    else if (category.equals(INBOUND_EVENTS_TOTAL_KEY))
    {
      processEventsTotalStats(_container.getInboundEventStatisticsCollector(), request);
    }
    else if (category.equals(INBOUND_EVENTS_SOURCES_KEY))
    {
      processEventsSourcesList(_container.getInboundEventStatisticsCollector(), request);
    }
    else if (category.equals(INBOUND_EVENTS_PSOURCES_KEY))
    {
      processEventsPhysicalSourcesList(_container.getInBoundStatsCollectors(), request);
    }
    else if (category.startsWith(INBOUND_EVENTS_SOURCE_PREFIX))
    {
      processEventsSourceStats(_container.getInboundEventStatisticsCollector(),
                               INBOUND_EVENTS_SOURCE_PREFIX,
                               request);
    }
    else if (category.startsWith(INBOUND_EVENTS_PSOURCE_PREFIX))
    {
      processPhysicalPartitionStats(_container.getInBoundStatsCollectors(),
    		  					INBOUND_EVENTS_PSOURCE_PREFIX,
                               request);
    }
    else if (category.equals("container"))
    {
      processContainerStats(request);
    }
    else if (category.equals("netty"))
    {
      processNettyStats(request);
    }
    else if (category.equals("outbound/total"))
    {
      processOutboundTrafficTotalStats(request);
    }
    else if (category.equals("outbound/clients"))
    {
      processOutboundTrafficClientsList(request);
    }
    else if (category.startsWith("outbound/client/"))
    {
      processOutboundTrafficClientStats(request);
    }
    else if (category.equals("inbound/total"))
    {
      processInboundTrafficTotalStats(request);
    }
    else
    {
      success = false;
    }

    return success;
  }


  private void processNettyStats(DatabusRequest request) throws IOException
  {
    //FIXME DDS-305
    /* NettyStats nettyStats = _configManager.getNettyStats();
    writeJsonObjectToResponse(nettyStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      String enabledStr = request.getParams().getProperty(ENABLED_PARAM);
      if (null != enabledStr)
      {
        boolean newEnabled = Boolean.parseBoolean(enabledStr);
        nettyStats.setEnabled(newEnabled);
      }

      String resetStr = request.getParams().getProperty(RESET_PARAM);
      if (null != resetStr)
      {
        nettyStats.reset();
      }
    }*/
  }

  private void processContainerStats(DatabusRequest request) throws IOException
  {
    ContainerStats containerStats = _containerStatsCollector.getContainerStats();
    if (null == containerStats)
    {
      return;
    }
    writeJsonObjectToResponse(containerStats, request);
  }

  private void processOutboundTrafficTotalStats(DatabusRequest request) throws IOException
  {

    ContainerTrafficTotalStatsMBean outboundTrafficTotalStatsMBean =
      _containerStatsCollector.getOutboundTrafficTotalStats();
    if (null == outboundTrafficTotalStatsMBean) return;

    //String json = outboundTrafficTotalStatsMBean.toJson();
    JsonEncoder jsonEncoder =
        outboundTrafficTotalStatsMBean.createJsonEncoder(
            Channels.newOutputStream(request.getResponseContent()));
    outboundTrafficTotalStatsMBean.toJson(jsonEncoder, null);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(outboundTrafficTotalStatsMBean, request);
    }
  }


  private void processOutboundTrafficClientsList(DatabusRequest request) throws IOException
  {
    List<String> clientsList = _containerStatsCollector.getOutboundClients();
    writeJsonObjectToResponse(clientsList, request);
  }

  private void processOutboundTrafficClientStats(DatabusRequest request)
                                                 throws IOException, RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String prefix = "outbound/client/";
    String client = category.substring(prefix.length());

    ContainerTrafficTotalStats clientStats = _containerStatsCollector.getOutboundClientStats(client);
    if (null == clientStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), prefix, client);
    }

    JsonEncoder jsonEncoder = clientStats.createJsonEncoder(
        Channels.newOutputStream(request.getResponseContent()));
    clientStats.toJson(jsonEncoder, null);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(clientStats, request);
    }

  }

  private void processInboundTrafficTotalStats(DatabusRequest request) throws IOException
  {
    ContainerTrafficTotalStatsMBean inboundTrafficTotalStatsMBean =
      _containerStatsCollector.getInboundTrafficTotalStats();
    if (null == inboundTrafficTotalStatsMBean) return;

    //String json = inboundTrafficTotalStatsMBean.toJson();
    JsonEncoder jsonEncoder =
        inboundTrafficTotalStatsMBean.createJsonEncoder(
            Channels.newOutputStream(request.getResponseContent()));
    inboundTrafficTotalStatsMBean.toJson(jsonEncoder, null);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(inboundTrafficTotalStatsMBean, request);
    }
  }

  private void processEventsTotalStats(DbusEventsStatisticsCollector statsCollector,
                                       DatabusRequest request) throws IOException
  {
    if (null == statsCollector) return;

    DbusEventsTotalStats totalStatsMBean = statsCollector.getTotalStats();
    if (null == totalStatsMBean) return;

    writeJsonObjectToResponse(totalStatsMBean, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(totalStatsMBean, request);
    }
  }

  private void processEventsSourcesList(DbusEventsStatisticsCollector statsCollector,
                                        DatabusRequest request) throws IOException
  {
    if (null == statsCollector) return;

    List<Integer> sourcesList = statsCollector.getSources();
    writeJsonObjectToResponse(sourcesList, request);
  }

  private void processEventsPhysicalSourcesList(StatsCollectors<DbusEventsStatisticsCollector> statsCollector,
                                        DatabusRequest request) throws IOException
  {
    if (null == statsCollector) return;

    List<String> sourcesList = statsCollector.getStatsCollectorKeys();
    writeJsonObjectToResponse(sourcesList, request);
  }


  private void processEventsSourceStats(DbusEventsStatisticsCollector statsCollector,
                                        String prefix,
                                        DatabusRequest request)
                                        throws IOException, RequestProcessingException
  {
    if (null == statsCollector) return;

    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String sourceIdStr = category.substring(prefix.length());
    int sourceId = -1;
    try
    {
      sourceId = Integer.valueOf(sourceIdStr);
    }
    catch (NumberFormatException nfe)
    {
      throw new InvalidRequestParamValueException("bad srcId:" + request.getName(), prefix, sourceIdStr);
    }

    DbusEventsTotalStats sourceStats = null;
    sourceStats = statsCollector.getSourceStats(sourceId);

    if (null == sourceStats)
    {
      LOG.warn("no stats for this srcId: " + request.getName() + "prefix=" +  prefix + "source ids " + sourceIdStr);
      sourceStats = new DbusEventsTotalStats(0, sourceIdStr, false, false, null);
    }

    writeJsonObjectToResponse(sourceStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(sourceStats, request);
    }

  }

  private void processPhysicalPartitionStats(StatsCollectors<DbusEventsStatisticsCollector> statsCollectors,
                                        String prefix,
                                        DatabusRequest request)
                                        throws IOException, RequestProcessingException
  {
    if (null == statsCollectors) return;

    
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String sourceIdStr = category.substring(prefix.length());
    //allow DBNAME/partitionid for REST api
    sourceIdStr = sourceIdStr.replace('/', ':');

    DbusEventsStatisticsCollector s = statsCollectors.getStatsCollector(sourceIdStr);
     
    DbusEventsTotalStats sourceStats = (s==null)  ? null : s.getTotalStats();
    if (null == sourceStats)
    {
      LOG.warn("no stats for this srcId: " + request.getName() + "prefix=" +  prefix + "source ids " + sourceIdStr);
      sourceStats = new DbusEventsTotalStats(0, sourceIdStr, false, false, null);
    }

    writeJsonObjectToResponse(sourceStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(sourceStats, request);
    }

  }

  private void processEventsPeersList(DbusEventsStatisticsCollector statsCollector,
                                      DatabusRequest request) throws IOException
  {
    if (null == statsCollector) return;

    List<String> clientsList = statsCollector.getPeers();
    writeJsonObjectToResponse(clientsList, request);
  }

  private void processEventsPeerStats(DbusEventsStatisticsCollector statsCollector,
                                      String prefix,
                                      DatabusRequest request)
                                      throws IOException, RequestProcessingException
  {
    if (null == statsCollector) return;

    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String client = category.substring(prefix.length());

    DbusEventsTotalStats clientStats = statsCollector.getPeerStats(client);
    if (null == clientStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), prefix, client);
    }

    writeJsonObjectToResponse(clientStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(clientStats, request);
    }

  }

}
