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
import java.util.concurrent.ExecutorService;

import org.jboss.netty.handler.codec.http.HttpMethod;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.monitoring.mbean.DbusEventStatsCollectorsPartitioner;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.container.request.ContainerStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;

/**
 *
 * ContainerStatsRequestProcessor for HTTP Relay
 */
public class RelayContainerStatsRequestProcessor extends ContainerStatsRequestProcessor
{

  private final HttpRelay _relay;

  public RelayContainerStatsRequestProcessor(ExecutorService executorService,
                                             HttpRelay relay)
  {
    super(executorService, relay);
    _relay = relay;
  }

  @Override
  public boolean doProcess(String category, DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    /**
     * If this is a /[in|out]bound/events/psource type query, we will intercept to check if this
     * is a stats request at DB aggregate level.
     *
     * Query Structure:
     *<pre>
     * Partition Level query :
     *    http://<RelayHost>:<RelayPort>/containerStats/inbound/events/psource/<DB>:<Partition> 
     * DB Aggregate Level
     *    http://<RelayHost>:<RelayPort>/containerStats/inbound/events/psource/<DB>
     *
     * More documentation found in
     *  https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+v2.0++Protocol#Databusv2.0Protocol-EventBufferStatistics
     * </pre>
     */
    if (category.startsWith(INBOUND_EVENTS_PSOURCE_PREFIX))
    {
      processStats(_relay.getInBoundStatsCollectors(),
                                       _relay.getDbInboundStatsCollectors(),
                                       INBOUND_EVENTS_PSOURCE_PREFIX, request);
      return true;
    }
    else if (category.startsWith(OUTBOUND_EVENTS_PSOURCE_PREFIX))
    {
      processStats(_relay.getOutBoundStatsCollectors(),
                                       _relay.getDbOutboundStatsCollectors(),
                                       OUTBOUND_EVENTS_PSOURCE_PREFIX, request);
      return true;
    }
    else
    {
      return super.doProcess(category, request);
    }
  }

  private void processStats(StatsCollectors<DbusEventsStatisticsCollector> globalStatsCollector,
                            DbusEventStatsCollectorsPartitioner resourceGroupStatsCollector,
                            String prefix,
                            DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    String reqPathStr = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String reqPathSuffix = reqPathStr.substring(prefix.length());
    // allow DBNAME/partitionid for REST api
    reqPathSuffix = reqPathSuffix.replace('/', ':');

    DbusEventsTotalStats sourceStats = null;
    if (reqPathSuffix.contains(":"))
    {
      // This is a request for a specific partition
      if (null != globalStatsCollector)
      {
        DbusEventsStatisticsCollector s =
            globalStatsCollector.getStatsCollector(reqPathSuffix);
        sourceStats = (s == null) ? null : s.getTotalStats();
      }
    }
    else
    {
      // This is a request at DB aggregate level
      if (null != resourceGroupStatsCollector)
      {
        StatsCollectors<DbusEventsStatisticsCollector> c =
                  resourceGroupStatsCollector.getDBStatsCollector(reqPathSuffix);
        if (null != c)
          sourceStats = c.getStatsCollector().getTotalStats();
      }
    }

    if (null == sourceStats)
    {
      LOG.warn("No Stats for this source=" + request.getName() + ", prefix=" + prefix
          + ", DB/Physical Partition String=" + reqPathSuffix);
      sourceStats = new DbusEventsTotalStats(0, reqPathSuffix, false, false, null);
    }

    writeJsonObjectToResponse(sourceStats, request);

    if (request.getRequestType() == HttpMethod.PUT
        || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(sourceStats, request);
    }
  }
}
