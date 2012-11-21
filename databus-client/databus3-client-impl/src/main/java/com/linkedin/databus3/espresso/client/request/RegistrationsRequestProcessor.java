package com.linkedin.databus3.espresso.client.request;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.monitoring.RegistrationStatsInfo;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DeregisterResult;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.request.ClientStatsRequestProcessor;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.container.request.AbstractStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus3.espresso.client.DatabusHttpV3ClientImpl;
import com.linkedin.databus3.espresso.client.DatabusV3ConsumerRegistration;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumerRegistration;

public class RegistrationsRequestProcessor extends
		AbstractStatsRequestProcessor
		{
	public static final String MODULE = ClientStatsRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	public static final String COMMAND_NAME = "regops";

	public final static String FETCHMAXSCN_KEY_PREFIX = "fetchMaxScn/";
	public final static String FLUSHMAXSCN_KEY_PREFIX = "flush/";
	public final static String DEREGISTER_KEY_PREFIX = "deregister/";
	public final static String REGISTRATIONS_KEY = "registrations";
	public final static String REGISTRATION_KEY_PREFIX = "registration/";
	public final static String INBOUND_EVENTS_REGISTRATION_KEY_PREFIX = "stats/inbound/events/";
    public final static String BOOTSTRAP_EVENTS_REGISTRATION_KEY_PREFIX = "stats/bootstrap/events/";
    public final static String INBOUND_CALLBACKS_REGISTRATION_KEY_PREFIX = "stats/inbound/callbacks/";
    public final static String BOOTSTRAP_CALLBACKS_REGISTRATION_KEY_PREFIX = "stats/bootstrap/callbacks/";

	private final DatabusHttpV3ClientImpl _client;
	private boolean _modifyEnable = false;

	public final static String maxScntimeoutPerRetryParam = "maxScntimeoutPerRetry";
	public final static String numMaxScnRetriesParam = "numMaxScnRetries";
	public final static String flushTimeoutParam = "flushTimeout";

	public RegistrationsRequestProcessor(ExecutorService executorService,
			 DatabusHttpV3ClientImpl client)
	{
		this(executorService,client,false);
	}

	public RegistrationsRequestProcessor(ExecutorService executorService,
										 DatabusHttpV3ClientImpl client,
										 boolean modifyEnable)
	{
		super(COMMAND_NAME, executorService);
		_client = client;
		_modifyEnable = modifyEnable;
	}


	private DatabusV3ConsumerRegistration getRegistration(String prefix, DatabusRequest request)
			throws RequestProcessingException
	{
		String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
		String regIdStr = category.substring(prefix.length());

		Map<RegistrationId, DatabusV3Registration> regIdMap = _client.getRegistrationIdMap();

		if ( null == regIdMap)
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "No registrations available !! ");


		RegistrationId regId = new RegistrationId(regIdStr);

		DatabusV3Registration reg = regIdMap.get(regId);

		if ( null == reg)
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "Registration with id " + regId + " not present !!");

		if (!(reg instanceof DatabusV3ConsumerRegistration))
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "Registration with id " + regId + " does not support this operation !!");

		return (DatabusV3ConsumerRegistration)reg;
	}


	private void doFlush(DatabusRequest request)
			throws RequestProcessingException, IOException
	{
		DatabusV3ConsumerRegistration reg = getRegistration(FLUSHMAXSCN_KEY_PREFIX, request);
		long maxScnTimeout = getLongRequiredParam(FLUSHMAXSCN_KEY_PREFIX, reg.getId(), maxScntimeoutPerRetryParam, request);
		long retry = getLongRequiredParam(FLUSHMAXSCN_KEY_PREFIX, reg.getId(), numMaxScnRetriesParam, request);
		long flushTimeout = getLongRequiredParam(FLUSHMAXSCN_KEY_PREFIX, reg.getId(), flushTimeoutParam, request);
		FetchMaxSCNRequest req = new FetchMaxSCNRequest(maxScnTimeout, (int)retry);

		if ( ! _modifyEnable)
			throw new InvalidRequestParamValueException(request.getName(), FLUSHMAXSCN_KEY_PREFIX, "Flush not allowed !!" );

		RelayFlushMaxSCNResult result = null;
		try
		{
			result = _client.flush(reg, flushTimeout, req);
		} catch (InterruptedException ie) {
			LOG.error("Got interruped while doing flush for regId :" + reg.getId());
		}
		writeJsonObjectToResponse(result, request);
	}

	private void doFetchMaxScn(DatabusRequest request)
			throws RequestProcessingException, IOException
	{
		DatabusV3ConsumerRegistration reg = getRegistration(FETCHMAXSCN_KEY_PREFIX, request);
		long maxScnTimeout = getLongRequiredParam(FETCHMAXSCN_KEY_PREFIX, reg.getId(), maxScntimeoutPerRetryParam, request);
		long retry = getLongRequiredParam(FETCHMAXSCN_KEY_PREFIX, reg.getId(), numMaxScnRetriesParam, request);

		FetchMaxSCNRequest req = new FetchMaxSCNRequest(maxScnTimeout, (int)retry);
		RelayFindMaxSCNResult result = null;
		try
		{
			result = _client.fetchMaxSCN(reg, req);
		} catch (InterruptedException ie) {
			LOG.error("Got interruped while doing flush for regId :" + reg.getId());
		}
		writeJsonObjectToResponse(result, request);
	}

	private void doDeregister(DatabusRequest request)
			throws RequestProcessingException, IOException
	{
		DatabusV3ConsumerRegistration reg = getRegistration(DEREGISTER_KEY_PREFIX, request);

		if ( ! _modifyEnable)
			throw new InvalidRequestParamValueException(request.getName(), DEREGISTER_KEY_PREFIX, "Deregistration not allowed !!" );

		DeregisterResult result = _client.deregister(reg);
		writeJsonObjectToResponse(result, request);
	}

	private Long getLongRequiredParam(String prefix, RegistrationId regId, String paramName, DatabusRequest request)
			throws RequestProcessingException
	{
		Properties p = request.getParams();

		String valStr = p.getProperty(paramName);

		if ( null == valStr)
		{
			throw new InvalidRequestParamValueException(request.getName(), prefix, "Param (" + paramName + ") not present. RegId :" + regId);
		}

		Long v = null;
		try
		{
			v = Long.parseLong(valStr);
		} catch (Exception ex) {
			LOG.error("Got exception while converting (" + valStr + ") to long !!");
			throw new InvalidRequestParamValueException(request.getName(), prefix, "Param (" + paramName + ") not present. RegId :" + regId);
		}
		return v;
	}

	private void processRegistrations(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		Map<RegistrationId, DatabusV3Registration> registrationIdMap = _client.getRegistrationIdMap();

		if ( null == registrationIdMap)
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATIONS_KEY, "Present only for Databus V3 clients");

		Map<String, List<DatabusSubscription>> regIds = new TreeMap<String, List<DatabusSubscription>>();
		for (RegistrationId rid : registrationIdMap.keySet())
		{
			DatabusV3Registration reg = registrationIdMap.get(rid);
			List<DatabusSubscription> dsl = reg.getSubscriptions();
			regIds.put(rid.getId(), dsl);
		}

		writeJsonObjectToResponse(regIds, request);

		return;
	}

	private void processRegistrationInfo(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		DatabusV3ConsumerRegistration reg = getRegistration(REGISTRATION_KEY_PREFIX, request);

		DatabusSourcesConnection sourcesConn = _client.getDatabusSourcesConnection(reg.getId().toString());
		RegistrationStatsInfo regStatsInfo = new RegistrationStatsInfo(reg, sourcesConn);

		if (reg instanceof PartitionMultiplexingConsumerRegistration)
		{
		  SCN mpScn = ((PartitionMultiplexingConsumerRegistration) reg).getParent().getCurScn();
		  regStatsInfo.setHighWatermark(mpScn);
		}

		writeJsonObjectToResponse(regStatsInfo, request);
	}

  private void processInboundEventsRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findRegistration(request, INBOUND_EVENTS_REGISTRATION_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getRelayEventStats().getTotalStats(), request);
  }

  private void processBootstrapEventsRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findRegistration(request, BOOTSTRAP_EVENTS_REGISTRATION_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getBootstrapEventStats().getTotalStats(), request);
  }

  private void processInboundCallbacksRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findRegistration(request, INBOUND_CALLBACKS_REGISTRATION_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getRelayCallbackStats(), request);
  }

  private void processBootstrapCallbacksRegistration(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    DatabusV3Registration reg = findRegistration(request, BOOTSTRAP_CALLBACKS_REGISTRATION_KEY_PREFIX);
    writeJsonObjectToResponse(reg.getBootstrapCallbackStats(), request);
  }

  private DatabusV3Registration findRegistration(DatabusRequest request,
                                                 String prefix) throws InvalidRequestParamValueException
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

	@Override
	protected boolean doProcess(String category, DatabusRequest request)
			throws IOException, RequestProcessingException
	{
	    boolean success = true;

	    if (category.equals(REGISTRATIONS_KEY))
	    {
	    	processRegistrations(request);
	    } else if (category.startsWith(REGISTRATION_KEY_PREFIX)) {
	    	processRegistrationInfo(request);
	    } else if (category.startsWith(INBOUND_EVENTS_REGISTRATION_KEY_PREFIX)) {
	    	processInboundEventsRegistration(request);
        } else if (category.startsWith(BOOTSTRAP_EVENTS_REGISTRATION_KEY_PREFIX)) {
          processBootstrapEventsRegistration(request);
        } else if (category.startsWith(INBOUND_CALLBACKS_REGISTRATION_KEY_PREFIX)) {
          processInboundCallbacksRegistration(request);
        } else if (category.startsWith(BOOTSTRAP_CALLBACKS_REGISTRATION_KEY_PREFIX)) {
          processBootstrapCallbacksRegistration(request);
	    } else if (category.startsWith(FETCHMAXSCN_KEY_PREFIX)) {
	    	doFetchMaxScn(request);
	    } else if (category.startsWith(FLUSHMAXSCN_KEY_PREFIX)) {
	    	doFlush(request);
	    } else if (category.startsWith(DEREGISTER_KEY_PREFIX)) {
	    	doDeregister(request);
	    } else {
	    	success = false;
	    }

		return success;
	}
}
