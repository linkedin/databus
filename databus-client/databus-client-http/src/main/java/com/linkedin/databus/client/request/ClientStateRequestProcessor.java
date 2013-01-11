package com.linkedin.databus.client.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.DbusPartitionInfoImpl;
import com.linkedin.databus.client.monitoring.RegistrationStatsInfo;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DatabusV3MultiPartitionRegistration;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.DatabusRegistration.RegistrationState;
import com.linkedin.databus.client.registration.DatabusMultiPartitionRegistration;
import com.linkedin.databus.client.registration.DatabusV2ClusterRegistrationImpl;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.container.request.AbstractStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

public class ClientStateRequestProcessor extends AbstractStatsRequestProcessor 
{
	public static final String MODULE = ClientStatsRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	public static final String COMMAND_NAME = "clientState";
	  
	private final DatabusHttpClientImpl _client;

	/** All first-level registrations listed **/
	private final static String REGISTRATIONS_KEY = "registrations";

	/** First-level registration info listed **/
	private final static String REGISTRATION_KEY_PREFIX = "registration/";

	/** All Client Clusters supported by this client instance **/
	private final static String CLIENT_CLUSTERS_KEY = "clientClusters";

	/** Partitions supported by this client cluster with their registrations **/
	private final static String CLIENT_CLUSTER_KEY = "clientPartitions/";

	/** Registration info supported by this client cluster with their registrations **/
	private final static String CLIENT_CLUSTER_PARTITION_REG_KEY = "clientPartition/";
	
	/** Multi Partition Registrations active in this client instance **/
	private final static String MP_REGISTRATIONS_KEY = "mpRegistrations";

	/** Pause all registrations active in this client instance **/
	private final static String PAUSE_ALL_REGISTRATIONS = "registrations/pause";
	
	/** Pause registration identified by the registration id **/
	private final static String PAUSE_REGISTRATION = "registration/pause";
	
	/** Resume all registrations paused in this client instance **/
	private final static String RESUME_ALL_REGISTRATIONS = "registrations/resume";
	
	/** Resume registration identified by the registration id **/
	private final static String RESUME_REGISTRATION = "registration/resume";
	
	
	public ClientStateRequestProcessor(ExecutorService executorService, DatabusHttpClientImpl client)
	{
	    super(COMMAND_NAME, executorService);
	    _client = client;
	}

	  
	@Override
	protected boolean doProcess(String category, DatabusRequest request)
			throws IOException, RequestProcessingException
	{
	    boolean success = true;

	    if (category.equals(REGISTRATIONS_KEY))
	    {
	        /**
	         * TODO: The reason why there is diverging code below is because V2 and V3 is using different registration objects
	         * which results in different containers for V2 and V3 registrations.
	         * Once we make V3 use DatabusRegistration, we should be having one call.  
	         * 
	         * Note: Couldnt use instanceof for differentiating V2/V3 as V3 Client is not visible here.
	         */
	       if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )	
	    	   processRegistrations(request);
	       else
	    	   processV3Registrations(request);
	    } else if (category.startsWith(PAUSE_REGISTRATION)) {
	    	pauseRegistration(request);
	    } else if (category.startsWith(RESUME_REGISTRATION)) {
	    	resumeRegistration(request);
	    } else if (category.startsWith(REGISTRATION_KEY_PREFIX)) {
		    if ( _client.getClass().getSimpleName().equalsIgnoreCase("DatabusHttpClientImpl") )	
		    	processRegistrationInfo(request);
		    else
		    	processV3RegistrationInfo(REGISTRATION_KEY_PREFIX, request);
	    } else if (category.startsWith(CLIENT_CLUSTERS_KEY)) {
	    	processClusters(request);
	    } else if (category.startsWith(CLIENT_CLUSTER_KEY)) {
	    	processCluster(request);
	    } else if (category.startsWith(CLIENT_CLUSTER_PARTITION_REG_KEY)) {
	    	processPartition(request);
	    } else if (category.equals(MP_REGISTRATIONS_KEY)) {
	        processMPRegistrations(request);
	    } else if (category.equals(PAUSE_ALL_REGISTRATIONS)) {
	    	pauseAllRegistrations(request);
	    } else if (category.equals(RESUME_ALL_REGISTRATIONS)) {
	    	resumeAllRegistrations(request);
	    } else {
	    	success = false;
	    }
	    
	    return success;
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
	  	
	private DatabusV3Registration findV3Registration(RegistrationId regId, DatabusRequest request)
			  throws IOException, RequestProcessingException
    {
		Map<RegistrationId, DatabusV3Registration> regIdMap = _client.getRegistrationIdMap();
		if ( null == regIdMap)
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "No registrations available !! ");


		DatabusV3Registration reg = regIdMap.get(regId);
		if ( null == reg)
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "Registration with id " + regId + " not present !!");
		return reg;
    }
	  
	private void processV3RegistrationInfo(String prefix, DatabusRequest request)
			  throws IOException, RequestProcessingException
	{
		String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
		String regIdStr = category.substring(prefix.length());
		RegistrationId regId = new RegistrationId(regIdStr);
		DatabusV3Registration reg = findV3Registration(regId, request);
		DatabusSourcesConnection sourcesConn = _client.getDatabusSourcesConnection(regIdStr);

		if ( null == reg)
			throw new InvalidRequestParamValueException(request.getName(), REGISTRATION_KEY_PREFIX, "Registration with id " + regId + " not present !!");

		RegistrationStatsInfo regStatsInfo = new RegistrationStatsInfo(reg, sourcesConn);
		writeJsonObjectToResponse(regStatsInfo, request);
	}
	
	  private void processV3Registrations(DatabusRequest request)
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

	  
	private void processRegistrations(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		Collection<RegInfo> regs = getAllRegistrations();
		writeJsonObjectToResponse(regs, request);
	}
	
	private void processRegistrationInfo(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		DatabusRegistration r = findRegistration(request, REGISTRATION_KEY_PREFIX);
		writeJsonObjectToResponse(getRegistration(r.getRegistrationId()), request);
	}
	
	
	private void processClusters(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		Collection<DbusClusterInfo> clusters = getAllClientClusters();
		writeJsonObjectToResponse(clusters, request);
	}
	
	private void processCluster(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
		String clusterName = category.substring(CLIENT_CLUSTER_KEY.length());
		Collection<PartitionInfo> clusters = getClusterPartitions(clusterName);
		writeJsonObjectToResponse(clusters, request);
	}
	
	private void processPartition(DatabusRequest request)
			throws IOException, RequestProcessingException
	{
		String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
		String clusterPartitionName = category.substring(CLIENT_CLUSTER_PARTITION_REG_KEY.length());
		String[] toks = clusterPartitionName.split("[:/]");
		if ( toks.length != 2)
			throw new RequestProcessingException("Cluster and partition info are expected to be in pattern = <cluster>[/:]<partition> but was " + clusterPartitionName);
		RegInfo reg = getPartitionRegistration(toks[0], new Long(toks[1]));
		writeJsonObjectToResponse(reg, request);
	}
	
	
	private DatabusV2ClusterRegistrationImpl getClusterRegistration(String clusterName)
			throws RequestProcessingException
	{
		Collection<DatabusMultiPartitionRegistration> regs = _client.getAllClientClusterRegistrations();

		for (DatabusMultiPartitionRegistration reg : regs)
		{
			if ( reg instanceof DatabusV2ClusterRegistrationImpl)
			{
				DatabusV2ClusterRegistrationImpl r = (DatabusV2ClusterRegistrationImpl)reg;
				if (clusterName.equals(r.getClusterInfo().getName()))
					return r;
			}
		}

		throw new RequestProcessingException("No Registration found for cluster (" + clusterName + ") !!");
	}

	private Collection<DbusClusterInfo> getAllClientClusters()
	{
		List<DbusClusterInfo> clusters = new ArrayList<DbusClusterInfo>();

		Collection<DatabusMultiPartitionRegistration> regs = _client.getAllClientClusterRegistrations();
		for (DatabusMultiPartitionRegistration reg :regs)
		{
			if ( reg instanceof DatabusV2ClusterRegistrationImpl)
			{
				DatabusV2ClusterRegistrationImpl r = (DatabusV2ClusterRegistrationImpl)reg;
				clusters.add(r.getClusterInfo());
			}
		}
		return clusters;
	}
	
	private void pauseRegistration(DatabusRequest request) throws IOException, RequestProcessingException
	{
		DatabusRegistration r = findRegistration(request, PAUSE_REGISTRATION);
		LOG.info("REST call to pause registration : " + r.getRegistrationId());

		if ( r.getState().isRunning())
		{
			if ( r.getState() != RegistrationState.PAUSED)
				r.pause();
		}
		writeJsonObjectToResponse(new RegStatePair(r.getState(), r.getRegistrationId()), request);
	}
	
	private void resumeRegistration(DatabusRequest request) throws IOException, RequestProcessingException
	{
		DatabusRegistration r = findRegistration(request, RESUME_REGISTRATION);
		LOG.info("REST call to resume registration : " + r.getRegistrationId());

		if ( r.getState().isRunning())
		{
			if ( (r.getState() == RegistrationState.PAUSED) || 
					(r.getState() == RegistrationState.SUSPENDED_ON_ERROR))
				r.resume();
		}
		writeJsonObjectToResponse(new RegStatePair(r.getState(), r.getRegistrationId()), request);
	}
	
	private void pauseAllRegistrations(DatabusRequest request) throws IOException
	{
		LOG.info("REST call to pause all registrations");
		Collection<DatabusRegistration> regs = _client.getAllRegistrations();
		for (DatabusRegistration r : regs)
		{
			if ( r.getState().isRunning())
			{
				if ( r.getState() != RegistrationState.PAUSED)
					r.pause();
			}
		}
		writeJsonObjectToResponse(getAllRegState(), request);
	}
	
	
	private void resumeAllRegistrations(DatabusRequest request) throws IOException
	{
		LOG.info("REST call to resume all registrations");
		Collection<DatabusRegistration> regs = _client.getAllRegistrations();
		for (DatabusRegistration r : regs)
		{
			if ( r.getState().isRunning())
			{
				if ( (r.getState() == RegistrationState.PAUSED) || 
						(r.getState() == RegistrationState.SUSPENDED_ON_ERROR))
					r.resume();
			}
		}
		writeJsonObjectToResponse(getAllRegState(), request);
	}
	private Collection<RegStatePair> getAllRegState()
	{
		List<RegStatePair> regList = new ArrayList<RegStatePair>();

		Collection<DatabusRegistration> regs = _client.getAllRegistrations();

		for (DatabusRegistration r : regs)
		{
			RegStatePair r1 = new RegStatePair(r.getState(),r.getRegistrationId());
			
			regList.add(r1);
		}
		return regList;
	}
	
	private Collection<RegInfo> getAllRegistrations()
	{
		List<RegInfo> regList = new ArrayList<RegInfo>();

		Collection<DatabusRegistration> regs = _client.getAllRegistrations();

		for (DatabusRegistration r : regs)
		{
			RegInfo regInfo = null;
			if (r instanceof DatabusMultiPartitionRegistration)
			{
				Map<DbusPartitionInfo, DatabusRegistration> childRegs =  ((DatabusMultiPartitionRegistration)r).getPartitionRegs();
				Map<DbusPartitionInfo,RegInfo> childR = new HashMap<DbusPartitionInfo,RegInfo>();
				for (Entry<DbusPartitionInfo, DatabusRegistration> e : childRegs.entrySet())
				{	
					childR.put(e.getKey(), 
							new RegInfo(e.getValue().getState(), e.getValue().getRegistrationId(), e.getValue().getStatus(), e.getValue().getFilterConfig(), e.getValue().getSubscriptions()));
				}
				regInfo = new RegInfo(r.getState(), r.getRegistrationId(), r.getStatus(), r.getFilterConfig(), r.getSubscriptions(),true,childR);
			} else {
				regInfo = new RegInfo(r.getState(),r.getRegistrationId(),r.getStatus(),r.getFilterConfig(),r.getSubscriptions());
			}
			regList.add(regInfo);
		}
		return regList;
	}

	private RegInfo getRegistration(RegistrationId regId)
			throws RequestProcessingException
	{
		Collection<DatabusRegistration> regs = _client.getAllRegistrations();

		for (DatabusRegistration r : regs)
		{
			if ( regId.equals(r.getRegistrationId()) )
			{
				if ( r instanceof DatabusMultiPartitionRegistration)
				{
					Map<DbusPartitionInfo, DatabusRegistration> childRegs =  ((DatabusMultiPartitionRegistration)r).getPartitionRegs();
					Map<DbusPartitionInfo,RegInfo> childR = new HashMap<DbusPartitionInfo,RegInfo>();
					for (Entry<DbusPartitionInfo, DatabusRegistration> e : childRegs.entrySet())
					{	
						childR.put(e.getKey(), 
								new RegInfo(e.getValue().getState(), e.getValue().getRegistrationId(), e.getValue().getStatus(), e.getValue().getFilterConfig(), e.getValue().getSubscriptions()));
					}

					return new RegInfo(r.getState(), r.getRegistrationId(), r.getStatus(), r.getFilterConfig(), r.getSubscriptions(),true,childR);
				} else {
					return new RegInfo(r.getState(),r.getRegistrationId(),r.getStatus(),r.getFilterConfig(),r.getSubscriptions());
				}
			}
		}

		throw new RequestProcessingException("Unable to find regId (" + regId + ")");
	}

	private Collection<PartitionInfo> getClusterPartitions(String cluster) 
			throws RequestProcessingException
	{
		DatabusV2ClusterRegistrationImpl reg = getClusterRegistration(cluster);
		List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();

		Map<DbusPartitionInfo, DatabusRegistration> regMap = reg.getPartitionRegs();
		for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
		{
			PartitionInfo p = new PartitionInfo(e.getKey().getPartitionId(), e.getValue().getRegistrationId());
			partitions.add(p);
		}
		return partitions;
	}

	private RegInfo getPartitionRegistration(String cluster, long partition)
			throws RequestProcessingException
	{
		DatabusRegistration r = getChildRegistration(cluster, partition);

		return new RegInfo(r.getState(), r.getRegistrationId(), r.getStatus(), r.getFilterConfig(), r.getSubscriptions());
	}


	private DatabusRegistration getChildRegistration(String cluster, long partition) 
			throws RequestProcessingException
	{
		DatabusV2ClusterRegistrationImpl reg = getClusterRegistration(cluster);
		DbusPartitionInfo p = new DbusPartitionInfoImpl(partition);

		DatabusRegistration r = reg.getPartitionRegs().get(p);

		if ( null == r)
			throw new RequestProcessingException("Partition(" + partition + ") for cluster (" + cluster + ") not found !!");

		return r;
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
	
	private static class PartitionInfo
	{
		private final long partition;

		private final RegistrationId regId;

		public long getPartition() {
			return partition;
		}

		public RegistrationId getRegId() {
			return regId;
		}

		public PartitionInfo(long partition, RegistrationId regId) {
			super();
			this.partition = partition;
			this.regId = regId;
		}
	}


	private static class RegStatePair
	{
		private final DatabusRegistration.RegistrationState _state;

		private final RegistrationId _regId;
		
		public DatabusRegistration.RegistrationState getState() {
			return _state;
		}

		public RegistrationId getRegId() {
			return _regId;
		}
		
		public RegStatePair(RegistrationState state, RegistrationId regId)
		{
			_regId = regId;
			_state = state;
		}
	}
	
	
	private static class RegInfo
	{
		private final DatabusRegistration.RegistrationState state;

		private final RegistrationId regId;

		private final String status;

		private final DbusKeyCompositeFilterConfig filter;

		private final Collection<DatabusSubscription> subs;

		private final boolean isMultiPartition;

		private final Map<DbusPartitionInfo, RegInfo> childRegistrations;

		public DatabusRegistration.RegistrationState getState() {
			return state;
		}

		public RegistrationId getRegId() {
			return regId;
		}

		public String getStatus() {
			return status;
		}

		public DbusKeyCompositeFilterConfig getFilter() {
			return filter;
		}

		public Collection<DatabusSubscription> getSubs() {
			return subs;
		}


		public boolean isMultiPartition() {
			return isMultiPartition;
		}

		public Map<DbusPartitionInfo, RegInfo> getChildRegistrations() {
			return childRegistrations;
		}

		public RegInfo(RegistrationState state, RegistrationId regId,
				DatabusComponentStatus status,
				DbusKeyCompositeFilterConfig filter,
				Collection<DatabusSubscription> subs) {
			this(state,regId,status,filter,subs,false,null);
		}

		public RegInfo(RegistrationState state, RegistrationId regId,
				DatabusComponentStatus status,
				DbusKeyCompositeFilterConfig filter,
				Collection<DatabusSubscription> subs, boolean isMultiPartition,
				Map<DbusPartitionInfo, RegInfo> childRegistrations) {
			super();
			this.state = state;
			this.regId = regId;
			this.status = status.toString();
			this.filter = filter;
			this.subs = subs;
			this.isMultiPartition = isMultiPartition;
			this.childRegistrations = childRegistrations;
		}


	}
}
