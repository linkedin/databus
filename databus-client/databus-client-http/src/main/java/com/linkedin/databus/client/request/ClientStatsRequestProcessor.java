package com.linkedin.databus.client.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.monitoring.RegistrationStatsInfo;
import com.linkedin.databus.client.pub.DatabusV3MultiPartitionRegistration;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
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
  private final static String INBOUND_EVENTS_SOURCES_KEY = "outbound/events/sources";
  private final static String REGISTRATIONS_KEY = "registrations";
  private final static String MP_REGISTRATIONS_KEY = "mpRegistrations";
  private final static String REGISTRATION_KEY_PREFIX = "registration/";
  private final static String INBOUND_EVENTS_REGISTRATION_KEY_PREFIX = "inbound/events/registration/";

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
    else if (category.equals(REGISTRATIONS_KEY))
    {
    	processRegistrations(request);
    }
    else if (category.equals(MP_REGISTRATIONS_KEY))
    {
        processMPRegistrations(request);
    }
    else if ( category.startsWith(REGISTRATION_KEY_PREFIX))
    {
    	processRegistrationInfo(REGISTRATION_KEY_PREFIX,request);
    }
    else if (category.startsWith(INBOUND_EVENTS_REGISTRATION_KEY_PREFIX))
    {
       processInboundEventsRegistration(INBOUND_EVENTS_REGISTRATION_KEY_PREFIX, request);
    }
    else
    {
      success = false;
    }

    return success;
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
      throw new InvalidRequestParamValueException(request.getName(), INBOUND_HTTP_SOURCE_PREFIX,
                                                  sourceIdStr);
    }

    DbusHttpTotalStats sourceStats = _client.getHttpStatsCollector().getSourceStats(sourceId);
    if (null == sourceStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), INBOUND_HTTP_SOURCE_PREFIX,
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
    String client = category.substring(INBOUND_HTTP_RELAYS_PREFIX.length());

    DbusHttpTotalStats clientStats = _client.getHttpStatsCollector().getPeerStats(client);
    if (null == clientStats)
    {
      throw new InvalidRequestParamValueException(request.getName(), INBOUND_HTTP_RELAYS_PREFIX,
                                                  client);
    }

    writeJsonObjectToResponse(clientStats, request);

    if (request.getRequestType() == HttpMethod.PUT || request.getRequestType() == HttpMethod.POST)
    {
      enableOrResetStatsMBean(clientStats, request);
    }
  }

  private void processRegistrations(DatabusRequest request)
  throws IOException, RequestProcessingException
  {
	  Map<RegistrationId, DatabusV3Registration> registrationIdMap = _client.getRegistrationIdMap();

	  if ( null == registrationIdMap)
		  throw new InvalidRequestParamValueException(request.getName(), REGISTRATIONS_KEY, "Present only for Databus V3 clients");

	  Map<String, List<DatabusSubscription>> regIds = new TreeMap<String, List<DatabusSubscription>>();
      for (Map.Entry<RegistrationId, DatabusV3Registration> entry : registrationIdMap.entrySet())
	  {
        DatabusV3Registration reg = entry.getValue();
        if ( reg instanceof DatabusV3MultiPartitionRegistration)
        	continue;
        List<DatabusSubscription> dsl = reg.getSubscriptions();
        regIds.put(entry.getKey().getId(), dsl);
	  }

	  writeJsonObjectToResponse(regIds, request);

	  return;
  }

  /**
   * Exposes the mapping between a mpRegistration -> Set of individual registrations
   *
   */
  private void processMPRegistrations(DatabusRequest request)
  throws IOException, RequestProcessingException
  {
      Map<RegistrationId, DatabusV3Registration> registrationIdMap = _client.getRegistrationIdMap();

      if ( null == registrationIdMap)
          throw new InvalidRequestParamValueException(request.getName(), REGISTRATIONS_KEY, "Present only for Databus V3 clients");

      Map<String, List<String>> ridList = new TreeMap<String, List<String>>();
      for (Map.Entry<RegistrationId, DatabusV3Registration> entry : registrationIdMap.entrySet())
      {
          DatabusV3Registration reg = entry.getValue();
          if (reg instanceof DatabusV3MultiPartitionRegistration)
          {
            Collection<DatabusV3Registration> dvrList = ((DatabusV3MultiPartitionRegistration) reg).getPartionRegs().values();
            List<String> mpRegList =  new ArrayList<String>();
            for (DatabusV3Registration dvr : dvrList)
            {
            	mpRegList.add(dvr.getId().getId());
            }
            ridList.put(entry.getKey().getId(), mpRegList);
          }
      }

      writeJsonObjectToResponse(ridList, request);

      return;
  }

  private void processRegistrationInfo(String prefix, DatabusRequest request)
		  throws IOException, RequestProcessingException
  {
	  String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
	  String regIdStr = category.substring(prefix.length());

	  Map<RegistrationId, DatabusV3Registration> regIdMap = _client.getRegistrationIdMap();
	  if ( null == regIdMap)
		  throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "No registrations available !! ");


	  RegistrationId regId = new RegistrationId(regIdStr);
	  DatabusV3Registration reg = regIdMap.get(regId);
      DatabusSourcesConnection sourcesConn = _client.getDatabusSourcesConnection(regIdStr);

	  if ( null == reg)
		  throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "Registration with id " + regId + " not present !!");

	  RegistrationStatsInfo regStatsInfo = new RegistrationStatsInfo(reg, sourcesConn);
	  writeJsonObjectToResponse(regStatsInfo, request);
  }

  private void processInboundEventsRegistration(String prefix, DatabusRequest request)
		  throws IOException, RequestProcessingException
  {
	  String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
	  String registrationIdStr = category.substring(prefix.length());


      DatabusV3Registration reg = _client.getRegistration(new RegistrationId(registrationIdStr));
      if ( null == reg )
      {
          LOG.warn("Invalid registrationId: " + registrationIdStr );
          throw new InvalidRequestParamValueException(request.getName(), INBOUND_EVENTS_REGISTRATION_KEY_PREFIX, "No data available for this RegistrationId yet" );
      }
	  writeJsonObjectToResponse(reg.getRelayEventStats().getTotalStats(), request);
  }

}
