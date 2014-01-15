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


import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.client.registration.DatabusMultiPartitionRegistration;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.databus2.core.container.request.AbstractStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;

public class ClientStatsRequestProcessor extends AbstractStatsRequestProcessor
{

  public static final String MODULE = ClientStatsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String COMMAND_NAME = "clientStats";

  private final static String OUTBOUND_EVENTS_TOTAL_KEY = "outbound/events/total";
  private final static String OUTBOUND_EVENTS_SOURCES_KEY = "outbound/events/sources";
  private final static String OUTBOUND_EVENTS_SOURCE_PREFIX = "outbound/events/source/";
  private final static String OUTBOUND_EVENTS_CLIENTS_KEY = "outbound/events/clients";
  private final static String OUTBOUND_EVENTS_CLIENT_PREFIX = "outbound/events/client/";
  private final static String INBOUND_HTTP_TOTAL_KEY = "inbound/http/total";
  private final static String INBOUND_HTTP_SOURCES_KEY = "inbound/http/sources";
  private final static String INBOUND_HTTP_SOURCE_PREFIX = "inbound/http/source/";
  private final static String INBOUND_HTTP_RELAYS_KEY = "inbound/http/relays";
  private final static String INBOUND_HTTP_RELAYS_PREFIX = "inbound/http/relay/";
  private final static String INBOUND_EVENTS_TOTAL_KEY = "inbound/events/total";
  private final static String BOOTSTRAP_EVENTS_TOTAL_KEY = "bootstrap/events/total";
  private final static String INBOUND_EVENTS_SOURCES_KEY = "inbound/events/sources";  // was "outbound/events/sources"


  /** Stream/relay events (inbound into client lib's event buffer) for the registration. */
  private final static String INBOUND_EVENTS_REG_KEY_PREFIX = "inbound/events/registration/";

  /** Bootstrap events (inbound into client lib's event buffer) for the registration. */
  private final static String BOOTSTRAP_EVENTS_REG_KEY_PREFIX = "bootstrap/events/registration/";

  /** Stream/relay-mode callbacks for the registration (outbound to consumer/app). */
  // This key is very poorly named...
  private final static String INBOUND_CALLBACKS_REG_KEY_PREFIX = "inbound/callbacks/registration/";

  /** Bootstrap-mode callbacks for the registration (outbound to consumer/app). */
  private final static String BOOTSTRAP_CALLBACKS_REG_KEY_PREFIX = "bootstrap/callbacks/registration/";

  /**
   * Unified relay/bootstrap statistics for the registration (both inbound to the client
   * lib's event buffer and outbound to the consumer/app, but mostly the latter).
   */
  // "Registration" might be misnomer?  Client impl's loop is over relay groups, each
  // corresponding to a set of subscriptions (sources) and each with its own UnifiedClientStats
  // object.
  private final static String UNIFIED_REG_KEY_PREFIX = "unified/registration/";
  /**
   * Unified relay/bootstrap statistics aggregated across registrations (both inbound to
   * the client lib's event buffer and outbound to the consumer/app, but mostly the latter).
   */
  // Note that there is currently no corresponding REST interface to the aggregated
  // ConsumerCallbackStats objects.
  private final static String UNIFIED_TOTAL_KEY = "unified/total";

  private final DatabusHttpClientImpl _client;

  public ClientStatsRequestProcessor(ExecutorService executorService, DatabusHttpClientImpl client)
  {
    super(COMMAND_NAME, executorService);
    _client = client;
  }

  @Override
  public boolean doProcess(String category, DatabusRequest request)
         throws IOException, RequestProcessingException
  {
    boolean success = true;

    if (category.equals(OUTBOUND_EVENTS_TOTAL_KEY))
    {
      processEventsTotalStats(_client.getOutboundEventStatisticsCollector(), request);
    }
    else if (category.equals(OUTBOUND_EVENTS_SOURCES_KEY))
    {
      processEventsSourcesList(_client.getOutboundEventStatisticsCollector(), request);
    }
    else if (category.startsWith(OUTBOUND_EVENTS_SOURCE_PREFIX))
    {
      processEventsSourceStats(_client.getOutboundEventStatisticsCollector(),
                               OUTBOUND_EVENTS_SOURCE_PREFIX,
                               request);
    }
    else if (category.equals(OUTBOUND_EVENTS_CLIENTS_KEY))
    {
      processEventsPeersList(_client.getOutboundEventStatisticsCollector(), request);
    }
    else if (category.startsWith(OUTBOUND_EVENTS_CLIENT_PREFIX))
    {
      processEventsPeerStats(_client.getOutboundEventStatisticsCollector(),
                             OUTBOUND_EVENTS_CLIENT_PREFIX,
                             request);
    }
    else if (category.equals(INBOUND_HTTP_TOTAL_KEY))
    {
      processOutboundHttpTotalStats(request);
    }
    else if (category.equals(INBOUND_HTTP_SOURCES_KEY))
    {
      processOutboundHttpSourcesList(request);
    }
    else if (category.startsWith(INBOUND_HTTP_SOURCE_PREFIX))
    {
      processOutboundHttpSourceStats(request);
    }
    else if (category.equals(INBOUND_HTTP_RELAYS_KEY))
    {
      processOutboundHttpClientsList(request);
    }
    else if (category.startsWith(INBOUND_HTTP_RELAYS_PREFIX))
    {
      processOutboundHttpClientStats(request);
    }
    else if (category.equals(INBOUND_EVENTS_TOTAL_KEY))
    {
      processEventsTotalStats(_client.getInboundEventStatisticsCollector(), request);
    }
    else if (category.equals(BOOTSTRAP_EVENTS_TOTAL_KEY))
    {
      processEventsTotalStats(_client.getBootstrapEventsStatsCollector(), request);
    }
    else if (category.equals(INBOUND_EVENTS_SOURCES_KEY))
    {
      processEventsSourcesList(_client.getInboundEventStatisticsCollector(), request);
    }
    else if (category.startsWith(INBOUND_EVENTS_REG_KEY_PREFIX))
    {
      /**
       * TODO :  The reason why there is diverging code below is because V2 and V3 is using different registration objects
       * which results in different containers for V2 and V3 registrations.
       * Once we make V3 use DatabusRegistration, we should be having one call.
       *
       * Note: Couldnt use instanceof for differentiating V2/V3 as V3 Client is not visible here.
       */
       if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )
         processInboundEventsRegistration(request);
       else
         processInboundEventsRegistrationV3(request);
    }
    else if (category.startsWith(BOOTSTRAP_EVENTS_REG_KEY_PREFIX))
    {
        /**
         * TODO :  The reason why there is diverging code below is because V2 and V3 is using different registration objects
         * which results in different containers for V2 and V3 registrations.
         * Once we make V3 use DatabusRegistration, we should be having one call.
         *
         *   Note: Couldnt use instanceof for differentiating V2/V3 as V3 Client is not visible here.
         */
       if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )
         processBootstrapEventsRegistration(request);
       else
         processBootstrapEventsRegistrationV3(request);
    }
    else if (category.startsWith(INBOUND_CALLBACKS_REG_KEY_PREFIX))
    {
        /**
         * TODO :  The reason why there is diverging code below is because V2 and V3 is using different registration objects
         * which results in different containers for V2 and V3 registrations.
         * Once we make V3 use DatabusRegistration, we should be having one call.
         *
         * Note: Couldnt use instanceof for differentiating V2/V3 as V3 Client is not visible here.
         */
       if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )
         processInboundCallbacksRegistration(request);
       else
         processInboundCallbacksRegistrationV3(request);
    }
    else if (category.startsWith(BOOTSTRAP_CALLBACKS_REG_KEY_PREFIX))
    {
        /**
         * TODO : The reason why there is diverging code below is because V2 and V3 is using different registration objects
         * which results in different containers for V2 and V3 registrations.
         * Once we make V3 use DatabusRegistration, we should be having one call.
         *
         * Note: Couldnt use instanceof for differentiating V2/V3 as V3 Client is not visible here.
         */
       if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )
         processBootstrapCallbacksRegistration(request);
       else
         processBootstrapCallbacksRegistrationV3(request);
    }
    else if (category.startsWith(UNIFIED_REG_KEY_PREFIX))
    {
        /**
         * TODO : The reason why there is diverging code below is because V2 and V3 is using different registration objects
         * which results in different containers for V2 and V3 registrations.
         * Once we make V3 use DatabusRegistration, we should be having one call.
         *
         * Note: Couldn't use instanceof for differentiating V2/V3 as V3 Client is not visible here.
         */
       if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )
         processUnifiedRegistration(request);
       else
         processUnifiedRegistrationV3(request);
    }
    else if (category.equals(UNIFIED_TOTAL_KEY))
    {
      processUnifiedTotalStats(_client.getUnifiedClientStatsCollectors(), request);
    }
    else
    {
      success = false;
    }

    return success;
  }

  private void processUnifiedTotalStats(StatsCollectors<UnifiedClientStats> statsCollectors,
                                       DatabusRequest request) throws IOException
  {
    if (null == statsCollectors) return;

    UnifiedClientStats unifiedTotalStats = statsCollectors.getStatsCollector();
    if (null == unifiedTotalStats) return;

    writeJsonObjectToResponse(unifiedTotalStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(unifiedTotalStats, request);
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
      throw new InvalidRequestParamValueException(request.getName(), prefix, sourceIdStr);
    }

    DbusEventsTotalStats sourceStats = statsCollector.getSourceStats(sourceId);
    if (null == sourceStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), prefix, sourceIdStr);
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

  private void processOutboundHttpTotalStats(DatabusRequest request) throws IOException
  {
    DbusHttpTotalStats totalStats =  _client.getHttpStatsCollector().getTotalStats();
    if (null == totalStats) return;

    writeJsonObjectToResponse(totalStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(totalStats, request);
    }
  }

  private void processOutboundHttpSourcesList(DatabusRequest request) throws IOException
  {
    List<Integer> sourcesList = _client.getHttpStatsCollector().getSources();
    writeJsonObjectToResponse(sourcesList, request);
  }

  private void processOutboundHttpClientsList(DatabusRequest request) throws IOException
  {
    List<String> clientsList = _client.getHttpStatsCollector().getPeers();
    writeJsonObjectToResponse(clientsList, request);
  }

  private void processOutboundHttpSourceStats(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String sourceIdStr = category.substring(INBOUND_HTTP_SOURCE_PREFIX.length());
    int sourceId = -1;
    try
    {
      sourceId = Integer.valueOf(sourceIdStr);
    }
    catch (NumberFormatException nfe)
    {
      throw new InvalidRequestParamValueException(request.getName(), INBOUND_HTTP_SOURCE_PREFIX, sourceIdStr);
    }

    DbusHttpTotalStats sourceStats = _client.getHttpStatsCollector().getSourceStats(sourceId);
    if (null == sourceStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), INBOUND_HTTP_SOURCE_PREFIX, sourceIdStr);
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
    String client = category.substring(INBOUND_HTTP_RELAYS_PREFIX.length());

    DbusHttpTotalStats clientStats = _client.getHttpStatsCollector().getPeerStats(client);
    if (null == clientStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), INBOUND_HTTP_RELAYS_PREFIX, client);
    }

    writeJsonObjectToResponse(clientStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(clientStats, request);
    }
  }

  private void processInboundEventsRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusRegistration reg = findRegistration(request, INBOUND_EVENTS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getRelayEventStats().getTotalStats(), request);
  }

  private void processBootstrapEventsRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusRegistration reg = findRegistration(request, BOOTSTRAP_EVENTS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getBootstrapEventStats().getTotalStats(), request);
  }

  private void processInboundCallbacksRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusRegistration reg = findRegistration(request, INBOUND_CALLBACKS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getRelayCallbackStats(), request);
  }

  private void processBootstrapCallbacksRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusRegistration reg = findRegistration(request, BOOTSTRAP_CALLBACKS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getBootstrapCallbackStats(), request);
  }

  private void processUnifiedRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusRegistration reg = findRegistration(request, UNIFIED_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getUnifiedClientStats(), request);
  }

  private DatabusRegistration findRegistration(DatabusRequest request, String prefix) throws RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String registrationIdStr = category.substring(prefix.length());
    RegistrationId regId = new RegistrationId(registrationIdStr);

    Collection<DatabusRegistration> regs = _client.getAllRegistrations();

    for (DatabusRegistration r : regs)
    {
      if (regId.equals(r.getRegistrationId()))
        return r;

      if (r instanceof DatabusMultiPartitionRegistration)
      {
        Map<DbusPartitionInfo, DatabusRegistration> childRegs =  ((DatabusMultiPartitionRegistration)r).getPartitionRegs();
        for (Entry<DbusPartitionInfo, DatabusRegistration> e : childRegs.entrySet())
          if ( regId.equals(e.getValue().getRegistrationId()))
            return e.getValue();
      }

    }
    throw new RequestProcessingException("Unable to find registration (" + regId + ") ");
  }

  private void processInboundEventsRegistrationV3(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findV3Registration(request, INBOUND_EVENTS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getRelayEventStats().getTotalStats(), request);
  }

  private void processBootstrapEventsRegistrationV3(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findV3Registration(request, BOOTSTRAP_EVENTS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getBootstrapEventStats().getTotalStats(), request);
  }

  private void processInboundCallbacksRegistrationV3(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findV3Registration(request, INBOUND_CALLBACKS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getRelayCallbackStats(), request);
  }

  private void processBootstrapCallbacksRegistrationV3(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findV3Registration(request, BOOTSTRAP_CALLBACKS_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getBootstrapCallbackStats(), request);
  }

  private void processUnifiedRegistrationV3(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findV3Registration(request, UNIFIED_REG_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getUnifiedClientStats(), request);
  }

  private DatabusV3Registration findV3Registration(DatabusRequest request, String prefix)
    throws InvalidRequestParamValueException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String registrationIdStr = category.substring(prefix.length());

    DatabusV3Registration reg = _client.getRegistration(new RegistrationId(registrationIdStr));
    if ( null == reg )
    {
        LOG.warn("Invalid registrationId: " + registrationIdStr );
        throw new InvalidRequestParamValueException(request.getName(), prefix, "No data available for this RegistrationId yet" );
    }
    return reg;
  }
}
