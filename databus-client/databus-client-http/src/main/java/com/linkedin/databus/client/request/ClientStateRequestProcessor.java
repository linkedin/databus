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
import com.linkedin.databus.client.pub.RegistrationState;
import com.linkedin.databus.client.registration.DatabusMultiPartitionRegistration;
import com.linkedin.databus.client.registration.DatabusV2ClusterRegistrationImpl;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.container.request.AbstractStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * 
 * Request processor to support REST API for
 * 
 * (a) Listing the registration ids (both V2/V3 top and partition level registrations).
 * (b) Inspecting the status of a given registration by id (c) Listing all the client
 * clusters (both V2 and V3) registered to the client instance. (d) List all the active
 * partitions for a given V2/V3 client cluster. (e) Pause/Resume a given V2/V3
 * registration (both top-level and partition (child) level). (f) Pause/Resume all the V2
 * and V3 registrations (both top-level and partition (child) level) (g) List all the
 * MPRegistrations (V3).
 * 
 * Please note that Top-level registrations are those that were created as a result of one
 * of "registerXXX()" calls on databus-client. In the case of multi-partition
 * registrations (like MPRegistration, V2/V3 CLB), only the parent registration is
 * considered the top-level registration. Per-partition (child) registrations which were
 * created as part of partition migration are NOT top-level registrations.
 */
public class ClientStateRequestProcessor extends AbstractStatsRequestProcessor
{
  public static final String          MODULE                           =
                                                                           ClientStateRequestProcessor.class.getName();
  public static final Logger          LOG                              =
                                                                           Logger.getLogger(MODULE);
  public static final String          COMMAND_NAME                     = "clientState";

  private final DatabusHttpClientImpl _client;

  /** All first-level registrations listed **/
  private final static String         REGISTRATIONS_KEY                = "registrations";

  /** First-level registration info listed **/
  private final static String         REGISTRATION_KEY_PREFIX          = "registration/";

  /** All Client Clusters supported by this client instance **/
  private final static String         CLIENT_CLUSTERS_KEY              = "clientClusters";

  /** Partitions supported by this client cluster with their registrations **/
  private final static String         CLIENT_CLUSTER_KEY               =
                                                                           "clientPartitions/";

  /** Registration info supported by this client cluster with their registrations **/
  private final static String         CLIENT_CLUSTER_PARTITION_REG_KEY =
                                                                           "clientPartition/";

  /** Multi Partition Registrations active in this client instance **/
  private final static String         MP_REGISTRATIONS_KEY             =
                                                                           "mpRegistrations";

  /** Pause all registrations active in this client instance **/
  private final static String         PAUSE_ALL_REGISTRATIONS          =
                                                                           "registrations/pause";

  /** Pause registration identified by the registration id **/
  private final static String         PAUSE_REGISTRATION               =
                                                                           "registration/pause";

  /** Resume all registrations paused in this client instance **/
  private final static String         RESUME_ALL_REGISTRATIONS         =
                                                                           "registrations/resume";

  /** Resume registration identified by the registration id **/
  private final static String         RESUME_REGISTRATION              =
                                                                           "registration/resume";

  public ClientStateRequestProcessor(ExecutorService executorService,
                                     DatabusHttpClientImpl client)
  {
    super(COMMAND_NAME, executorService);
    _client = client;
  }

  @Override
  protected boolean doProcess(String category, DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    boolean success = true;

    if (category.equals(REGISTRATIONS_KEY))
    {
      processRegistrations(request);
    }
    else if (category.startsWith(PAUSE_REGISTRATION))
    {
      pauseResumeRegistration(request, true);
    }
    else if (category.startsWith(RESUME_REGISTRATION))
    {
      pauseResumeRegistration(request, false);
    }
    else if (category.startsWith(REGISTRATION_KEY_PREFIX))
    {
      processRegistrationInfo(request);
    }
    else if (category.startsWith(CLIENT_CLUSTERS_KEY))
    {
      processClusters(request);
    }
    else if (category.startsWith(CLIENT_CLUSTER_KEY))
    {
      processCluster(request);
    }
    else if (category.startsWith(CLIENT_CLUSTER_PARTITION_REG_KEY))
    {
      processPartition(request);
    }
    else if (category.equals(MP_REGISTRATIONS_KEY))
    {
      processMPRegistrations(request);
    }
    else if (category.equals(PAUSE_ALL_REGISTRATIONS))
    {
      pauseAllRegistrations(request);
    }
    else if (category.equals(RESUME_ALL_REGISTRATIONS))
    {
      resumeAllRegistrations(request);
    }
    else
    {
      success = false;
    }
    return success;
  }

  /**
   * Exposes the mapping between a mpRegistration -> Set of individual registrations
   * 
   */
  private void processMPRegistrations(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    Map<RegistrationId, DatabusV3Registration> registrationIdMap =
        _client.getRegistrationIdMap();

    if (null == registrationIdMap)
      throw new InvalidRequestParamValueException(request.getName(),
                                                  REGISTRATIONS_KEY,
                                                  "Present only for Databus V3 clients");

    Map<String, List<String>> ridList = new TreeMap<String, List<String>>();
    for (Map.Entry<RegistrationId, DatabusV3Registration> entry : registrationIdMap.entrySet())
    {
      DatabusV3Registration reg = entry.getValue();
      if (reg instanceof DatabusV3MultiPartitionRegistration)
      {
        Collection<DatabusV3Registration> dvrList =
            ((DatabusV3MultiPartitionRegistration) reg).getPartionRegs().values();
        List<String> mpRegList = new ArrayList<String>();
        for (DatabusV3Registration dvr : dvrList)
        {
          mpRegList.add(dvr.getRegistrationId().getId());
        }
        ridList.put(entry.getKey().getId(), mpRegList);
      }
    }

    writeJsonObjectToResponse(ridList, request);

    return;
  }

  /**
   * Provides an individual registrations details. The individual registration can be
   * either that of V2/V3 and top-level or child. Top-level registrations are those that
   * were created as a result of one of "registerXXX()" calls on databus-client. In the
   * case of multi-partition registrations (like MPRegistration, V2/V3 CLB), only the
   * parent registration is considered the top-level registration. Per-partition (child)
   * registrations which were created as part of partition migration are NOT top-level
   * registrations. The output format can be different depending on whether it is a V2/V3
   * as we are dumping the entire Registration in the case of V2. In the case of V3, we
   * create an intermediate objects. These are legacy formats which when changed could
   * cause the integ-tests to fail.
   * 
   * @param request
   *          DatabusRequest corresponding to the REST call.
   * @throws IOException
   *           if unable to write to output channel.
   * @throws RequestProcessingException
   *           when registration could not be located.
   */
  private void processRegistrationInfo(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    boolean found = true;

    // V2 Registration lookup first
    RegistrationStatsInfo regStatsInfo = null;
    try
    {
      DatabusRegistration r = findV2Registration(request, REGISTRATION_KEY_PREFIX);
      writeJsonObjectToResponse(r, request);
    }
    catch (RequestProcessingException ex)
    {
      found = false;
    }

    // V3 Registration lookup if not found
    if (!found)
    {
      DatabusV3Registration reg = findV3Registration(request, REGISTRATION_KEY_PREFIX); // if
                                                                                        // reg
                                                                                        // is
                                                                                        // null,
                                                                                        // the
                                                                                        // callee
                                                                                        // throws
                                                                                        // an
                                                                                        // exception.
      DatabusSourcesConnection sourcesConn =
          _client.getDatabusSourcesConnection(reg.getRegistrationId().getId());
      regStatsInfo = new RegistrationStatsInfo(reg, sourcesConn);
      writeJsonObjectToResponse(regStatsInfo, request);
    }
  }

  /**
   * Displays all top-level registrations registered to the client (both V2 and V3).
   * Top-level registrations are those that were created as a result of one of
   * "registerXXX()" calls on databus-client. In the case of multi-partition registrations
   * (like MPRegistration, V2/V3 CLB), only the parent registration is considered the
   * top-level registration. Per-partition (child) registrations which were created as
   * part of partition migration are NOT top-level registrations.
   * 
   * @param request
   *          DatabusRequest corresponding to the REST API.
   * @throws IOException
   *           when unable to write to ourput channel
   */
  private void processRegistrations(DatabusRequest request) throws IOException
  {
    Map<String, Collection<DatabusSubscription>> regIds =
        new TreeMap<String, Collection<DatabusSubscription>>();

    // V2 Registration
    Collection<RegInfo> regs = getAllTopLevelV2Registrations();
    if (null != regs)
    {
      for (RegInfo r : regs)
      {
        regIds.put(r.getRegId().getId(), r.getSubs());
      }
    }

    Map<RegistrationId, DatabusV3Registration> registrationIdMap =
        _client.getRegistrationIdMap();
    // V3 Registration
    if (null != registrationIdMap)
    {
      for (Map.Entry<RegistrationId, DatabusV3Registration> entry : registrationIdMap.entrySet())
      {
        DatabusV3Registration reg = entry.getValue();
        List<DatabusSubscription> dsl = reg.getSubscriptions();
        regIds.put(entry.getKey().getId(), dsl);
      }
    }
    writeJsonObjectToResponse(regIds, request);
  }

  /**
   * 
   * Proved list of V2 and V3 Client clusters which are used (registered).
   * 
   * @param request
   *          DatabusRequest corresponding to the REST call.
   * @throws IOException
   *           when unable to write to output channel.
   */
  private void processClusters(DatabusRequest request) throws IOException
  {
    Map<RegistrationId, DbusClusterInfo> clusters = _client.getAllClientClusters();
    writeJsonObjectToResponse(clusters.values(), request);
  }

  /**
   * Provide the list of partitions corresponding to the V2/V3 client cluster.
   * 
   * @param request
   *          DatabusRequest corresponding to the REST call.
   * @throws IOException
   *           when unable to write to output channel.
   * @throws RequestProcessingException
   *           when cluster not found.
   */
  private void processCluster(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String clusterName = category.substring(CLIENT_CLUSTER_KEY.length());
    List<PartitionInfo> clusters = new ArrayList<PartitionInfo>();
    RequestProcessingException rEx = null;
    Collection<PartitionInfo> v2Clusters = null;

    // Check as if this is V2 Cluster first
    boolean found = true;
    try
    {
      v2Clusters = getV2ClusterPartitions(clusterName);
      clusters.addAll(v2Clusters);
    }
    catch (RequestProcessingException ex)
    {
      found = false;
      rEx = ex;
    }

    // Try as V3 cluster if it is not V2.
    if (!found)
    {
      Collection<PartitionInfo> v3Clusters = null;
      try
      {
        v3Clusters = getV3ClusterPartitions(clusterName);
        clusters.addAll(v3Clusters);
        found = true;
      }
      catch (RequestProcessingException ex)
      {
        found = false;
        rEx = ex;
      }
    }

    if (!found)
      throw rEx;

    writeJsonObjectToResponse(clusters, request);
  }

  /**
   * Provide a partition information belonging to a V2/V3 client cluster and hosted in
   * this client instance
   * 
   * @param request
   *          DatabusRequest corresponding to the REST call.
   * @throws IOException
   *           when unable to write to output channel.
   * @throws RequestProcessingException
   *           when cluster not found or when partition is not hosted in this instance
   */
  private void processPartition(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String clusterPartitionName =
        category.substring(CLIENT_CLUSTER_PARTITION_REG_KEY.length());
    /**
     * API: curl
     * http://<HOST>:<PORT>/clientState/clientPartition/<CLUSTER_NAME>/<PARTITION> curl
     * http://<HOST>:<PORT>/clientState/clientPartition/<CLUSTER_NAME>:<PARTITION>
     */
    String[] toks = clusterPartitionName.split("[:/]");
    if (toks.length != 2)
      throw new RequestProcessingException("Cluster and partition info are expected to be in pattern = <cluster>[/:]<partition> but was "
          + clusterPartitionName);
    RegInfo reg = null;
    boolean found = true;
    // Try as a V2 Partition
    try
    {
      reg = getV2PartitionRegistration(toks[0], new Long(toks[1]));
    }
    catch (RequestProcessingException ex)
    {
      found = false;
    }

    // If not found, try as V3
    if (!found)
    {
      reg = getV3PartitionRegistration(toks[0], new Long(toks[1]));
    }
    writeJsonObjectToResponse(reg, request);
  }

  private DatabusV2ClusterRegistrationImpl getV2ClusterRegistration(String clusterName) throws RequestProcessingException
  {
    Collection<DatabusMultiPartitionRegistration> regs =
        _client.getAllClientClusterRegistrations();

    for (DatabusMultiPartitionRegistration reg : regs)
    {
      if (reg instanceof DatabusV2ClusterRegistrationImpl)
      {
        DatabusV2ClusterRegistrationImpl r = (DatabusV2ClusterRegistrationImpl) reg;
        if (clusterName.equals(r.getClusterInfo().getName()))
          return r;
      }
    }

    throw new RequestProcessingException("No Registration found for cluster ("
        + clusterName + ") !!");
  }

  private DatabusV3MultiPartitionRegistration getV3ClusterRegistration(String clusterName) throws RequestProcessingException
  {
    // There is a one-to-one mapping between clusterName to
    // DatabusV3MultiPartitionRegistration
    Map<RegistrationId, DbusClusterInfo> clusterMap = _client.getAllClientClusters();

    for (Entry<RegistrationId, DbusClusterInfo> e : clusterMap.entrySet())
    {
      if (clusterName.equalsIgnoreCase(e.getValue().getName()))
      {
        DatabusV3Registration reg = _client.getRegistration(e.getKey());
        if (reg instanceof DatabusV3MultiPartitionRegistration)
        {
          return (DatabusV3MultiPartitionRegistration) reg;
        }
        break;
      }
    }

    throw new RequestProcessingException("No Registration found for cluster ("
        + clusterName + ") !!");
  }

  /**
   * Pause or resume a V2 or V3 registration. The registration can be a top-level or
   * child-level registration Top-level registrations are those that were created as a
   * result of one of "registerXXX()" calls on databus-client. In the case of
   * multi-partition registrations (like MPRegistration, V2/V3 CLB), only the parent
   * registration is considered the top-level registration. Per-partition (child)
   * registrations which were created as part of partition migration are NOT top-level
   * registrations.
   * 
   * @param request
   *          Databus request corresponding to the REST call.
   * @param doPause
   *          true if wanted to pause, false if to be resumed
   * @throws IOException
   *           if unable to write output to channel
   * @throws RequestProcessingException
   *           when registration could not be found.
   */
  private void pauseResumeRegistration(DatabusRequest request, boolean doPause) throws IOException,
      RequestProcessingException
  {
    DatabusRegistration r = null;
    DatabusV3Registration r2 = null;

    boolean found = true;
    boolean isRunning = false;
    boolean isPaused = false;
    boolean isSuspended = false;
    RegistrationId regId = null;
    RequestProcessingException rEx = null;
    RegStatePair regStatePair = null;
    try
    {
      r = findV2Registration(request, PAUSE_REGISTRATION);
      isRunning = r.getState().isRunning();
      isPaused = (r.getState() == DatabusRegistration.RegistrationState.PAUSED);
      isSuspended =
          (r.getState() == DatabusRegistration.RegistrationState.SUSPENDED_ON_ERROR);
      regId = r.getRegistrationId();
    }
    catch (RequestProcessingException ex)
    {
      found = false;
      rEx = ex;
    }

    if (!found)
    {
      try
      {
        r2 = findV3Registration(request, PAUSE_REGISTRATION);
        found = true;
        isRunning = r2.getState().isRunning();
        isPaused = (r2.getState() == RegistrationState.PAUSED);
        isSuspended = (r2.getState() == RegistrationState.SUSPENDED_ON_ERROR);
        regId = r.getRegistrationId();
      }
      catch (RequestProcessingException ex)
      {
        found = false;
        rEx = ex;
      }
    }

    if (!found)
      throw rEx;

    LOG.info("REST call to pause registration : " + regId);

    if (isRunning)
    {
      if (doPause)
      {
        if (!isPaused)
        {
          if (null != r)
          {
            r.pause();
            regStatePair = new RegStatePair(r.getState(), r.getRegistrationId());
          }
          else
          {
            r2.pause();
            regStatePair = new RegStatePair(r2.getState().name(), r2.getRegistrationId());
          }
        }
      }
      else
      {
        if (isPaused || isSuspended)
        {
          if (null != r)
          {
            r.resume();
            regStatePair = new RegStatePair(r.getState(), r.getRegistrationId());
          }
          else
          {
            r2.resume();
            regStatePair = new RegStatePair(r2.getState().name(), r2.getRegistrationId());
          }
        }
      }
    }
    writeJsonObjectToResponse(regStatePair, request);
  }

  /**
   * Pause all registrations (both V2 and V3 in this client instance) which are in running
   * state.
   * 
   * @param request
   *          DatabusRequest corresponding to the REST call.
   * @throws IOException
   *           when unable to write the output.
   */
  private void pauseAllRegistrations(DatabusRequest request) throws IOException
  {
    LOG.info("REST call to pause all registrations");

    /**
     * Get the top-level V2 registrations and pause them. The child-level registrations by
     * the top-level registrations that aggregates them.
     */
    Collection<DatabusRegistration> regs = _client.getAllRegistrations();
    if (null != regs)
    {
      for (DatabusRegistration r : regs)
      {
        if (r.getState().isRunning())
        {
          if (r.getState() != DatabusRegistration.RegistrationState.PAUSED)
            r.pause();
        }
      }
    }

    /**
     * Get the top-level V3 registrations and pause them. The child-level registrations by
     * the top-level registrations that aggregates them.
     */
    Map<RegistrationId, DatabusV3Registration> regMap = _client.getRegistrationIdMap();
    Collection<RegInfo> topLevelRegs = getAllTopLevelV3Registrations();
    /**
     * Important Note: There is an important implementation difference on which
     * registrations are stored in the global registration data-structure maintained by
     * the client (DatabusHttp[V3]ClientImpls) between V2 and V3.
     * 
     * 1. In the case of V2, only top-level registrations are stored in the global
     * data-structure (DatabusHttpClientImpl.regList 2. In the case of V3, all
     * registrations are stored in the global data-structure.
     * 
     * In the case of V3, this is needed so that all registrations can act on the relay
     * external view change. This can be refactored in the future by moving the
     * relay-external view change to registration impl ( reduce the complexity in
     * ClientImpl ). The V2 implementation did not have this logic and was following a
     * more intuitive structure of preserving the hierarchy.
     */
    if ((null != regMap) && (null != topLevelRegs))
    {
      for (RegInfo reg : topLevelRegs)
      {
        DatabusV3Registration r = regMap.get(reg.getRegId());
        if (r.getState().isRunning())
        {
          if (r.getState() != RegistrationState.PAUSED)
            r.pause();
        }
      }
    }
    writeJsonObjectToResponse(getAllTopLevelRegStates(), request);
  }

  /**
   * Resume all registrations paused or suspended (both V2 and V3 in this client instance)
   * 
   * @param request
   *          DatabusRequest corresponding to the REST call.
   * @throws IOException
   *           when unable to write the output.
   */
  private void resumeAllRegistrations(DatabusRequest request) throws IOException
  {
    LOG.info("REST call to resume all registrations");

    /**
     * Get the top-level V2 registrations and pause them. The child-level registrations by
     * the top-level registrations that aggregates them.
     */
    Collection<DatabusRegistration> regs = _client.getAllRegistrations();
    if (null != regs)
    {
      for (DatabusRegistration r : regs)
      {
        if (r.getState().isRunning())
        {
          if ((r.getState() == DatabusRegistration.RegistrationState.PAUSED)
              || (r.getState() == DatabusRegistration.RegistrationState.SUSPENDED_ON_ERROR))
            r.resume();
        }
      }
    }

    /**
     * Get the top-level V3 registrations and pause them. The child-level registrations by
     * the top-level registrations that aggregates them.
     */
    Map<RegistrationId, DatabusV3Registration> regMap = _client.getRegistrationIdMap();
    Collection<RegInfo> topLevelRegs = getAllTopLevelV3Registrations();
    /**
     * Important Note: There is an important implementation difference on which
     * registrations are stored in the global registration data-structure maintained by
     * the client (DatabusHttp[V3]ClientImpls) between V2 and V3.
     * 
     * 1. In the case of V2, only top-level registrations are stored in the global
     * data-structure (DatabusHttpClientImpl.regList 2. In the case of V3, all
     * registrations are stored in the global data-structure.
     * 
     * In the case of V3, this is needed so that all registrations can act on the relay
     * external view change. This can be refactored in the future by moving the
     * relay-external view change to registration impl ( reduce the complexity in
     * ClientImpl ). The V2 implementation did not have this logic and was following a
     * more intuitive structure of preserving the hierarchy.
     */
    if ((null != regMap) && (null != topLevelRegs))
    {
      for (RegInfo reg : topLevelRegs)
      {
        DatabusV3Registration r = regMap.get(reg.getRegId());
        if (r.getState().isRunning())
        {
          if ((r.getState() == RegistrationState.PAUSED)
              || (r.getState() == RegistrationState.SUSPENDED_ON_ERROR))
            r.resume();
        }
      }
    }
    writeJsonObjectToResponse(getAllTopLevelRegStates(), request);
  }

  /**
   * Generate regStatePair for all the top-level registrations (both V2 and V3).
   * 
   * @return
   */
  private Collection<RegStatePair> getAllTopLevelRegStates()
  {
    List<RegStatePair> regList = new ArrayList<RegStatePair>();

    Collection<RegInfo> regs = getAllTopLevelRegistrations();
    for (RegInfo reg : regs)
    {
      regList.add(new RegStatePair(reg.getState(), reg.getRegId()));
    }
    return regList;
  }

  /**
   * Returns all the top-level registrations (both V2 and V3). Top-level registrations are
   * those that were created as a result of one of "registerXXX()" calls on
   * databus-client. In the case of multi-partition registrations (like MPRegistration,
   * V2/V3 CLB), only the parent registration is considered the top-level registration.
   * Per-partition (child) registrations which were created as part of partition migration
   * are NOT top-level registrations.
   * 
   * @return collection of top-level registrations (V2/V3)
   */
  private Collection<RegInfo> getAllTopLevelRegistrations()
  {
    List<RegInfo> regList = new ArrayList<RegInfo>();
    regList.addAll(getAllTopLevelV2Registrations());
    regList.addAll(getAllTopLevelV3Registrations());
    return regList;
  }

  /**
   * Returns all the top-level V3 registrations. Top-level registrations are those that
   * were created as a result of one of "registerXXX()" calls on databus-client. In the
   * case of multi-partition registrations (like MPRegistration, V3 CLB), only the parent
   * registration is considered the top-level registration. Per-partition (child)
   * registrations which were created as part of partition migration are NOT top-level
   * registrations.
   * 
   * @return collection of top-level registrations (V3)
   */
  private Collection<RegInfo> getAllTopLevelV3Registrations()
  {
    /**
     * Important Note: There is an important implementation difference on which
     * registrations are stored in the global registration data-structure maintained by
     * the client (DatabusHttp[V3]ClientImpls) between V2 and V3.
     * 
     * 1. In the case of V2, only top-level registrations are stored in the global
     * data-structure (DatabusHttpClientImpl.regList 2. In the case of V3, all
     * registrations are stored in the global data-structure.
     * 
     * In the case of V3, this is needed so that all registrations can act on the relay
     * external view change. This can be refactored in the future by moving the
     * relay-external view change to registration impl ( reduce the complexity in
     * ClientImpl ). The V2 implementation did not have this logic and was following a
     * more intuitive structure of preserving the hierarchy.
     */
    Map<RegistrationId, RegInfo> regListMap = new HashMap<RegistrationId, RegInfo>();
    /**
     * The _client.getRegistrationIdMap() has all registrations in one place. Top-Level
     * Registrations = Only those registrations whose getParent() == null.
     */
    Map<RegistrationId, DatabusV3Registration> regMap = _client.getRegistrationIdMap();
    for (Entry<RegistrationId, DatabusV3Registration> e : regMap.entrySet())
    {
      RegInfo regInfo = null;
      DatabusV3Registration r = e.getValue();
      // If not top-level, skip
      if (null != r.getParentRegistration())
      {
        continue;
      }

      Map<DbusPartitionInfo, RegInfo> childR = null;
      if (r instanceof DatabusV3MultiPartitionRegistration)
      {
        // ass the children regs to parent.
        Map<PhysicalPartition, DatabusV3Registration> childRegs =
            ((DatabusV3MultiPartitionRegistration) r).getPartionRegs();
        childR = new HashMap<DbusPartitionInfo, RegInfo>();
        for (Entry<PhysicalPartition, DatabusV3Registration> e2 : childRegs.entrySet())
        {
          childR.put(new DbusPartitionInfoImpl(e2.getKey().getId()),
                     new RegInfo(e.getValue().getState().name(),
                                 e.getValue().getRegistrationId(),
                                 e.getValue().getStatus(),
                                 null,
                                 e.getValue().getSubscriptions()));
        }
      }
      regInfo =
          new RegInfo(r.getState().name(),
                      r.getRegistrationId(),
                      r.getStatus(),
                      null,
                      r.getSubscriptions(),
                      true,
                      childR);
      regListMap.put(e.getKey(), regInfo);
    }
    return regListMap.values();
  }

  /**
   * Returns all the top-level V2 registrations. Top-level registrations are those that
   * were created as a result of one of "registerXXX()" calls on databus-client. In the
   * case of multi-partition registrations (like V2 CLB), only the parent registration is
   * considered the top-level registration. Per-partition (child) registrations which were
   * created as part of partition migration are NOT top-level registrations.
   * 
   * @return collection of top-level registrations (V2)
   */
  private Collection<RegInfo> getAllTopLevelV2Registrations()
  {
    List<RegInfo> regList = new ArrayList<RegInfo>();

    Collection<DatabusRegistration> regs = _client.getAllRegistrations();

    for (DatabusRegistration r : regs)
    {
      RegInfo regInfo = null;
      if (r instanceof DatabusMultiPartitionRegistration)
      {
        Map<DbusPartitionInfo, DatabusRegistration> childRegs =
            ((DatabusMultiPartitionRegistration) r).getPartitionRegs();
        Map<DbusPartitionInfo, RegInfo> childR =
            new HashMap<DbusPartitionInfo, RegInfo>();
        for (Entry<DbusPartitionInfo, DatabusRegistration> e : childRegs.entrySet())
        {
          childR.put(e.getKey(), new RegInfo(e.getValue().getState().name(),
                                             e.getValue().getRegistrationId(),
                                             e.getValue().getStatus(),
                                             e.getValue().getFilterConfig(),
                                             e.getValue().getSubscriptions()));
        }
        regInfo =
            new RegInfo(r.getState().name(),
                        r.getRegistrationId(),
                        r.getStatus(),
                        r.getFilterConfig(),
                        r.getSubscriptions(),
                        true,
                        childR);
      }
      else
      {
        regInfo =
            new RegInfo(r.getState().name(),
                        r.getRegistrationId(),
                        r.getStatus(),
                        r.getFilterConfig(),
                        r.getSubscriptions());
      }
      regList.add(regInfo);
    }
    return regList;
  }

  /**
   * Get the list of partitions hosted by this client for the V2 cluster.
   * 
   * @param cluster
   *          V2 CLuster for which we need to find out the partitions.
   * @return
   * @throws RequestProcessingException
   *           when unable to find the cluster.
   */
  private Collection<PartitionInfo> getV2ClusterPartitions(String cluster) throws RequestProcessingException
  {
    DatabusV2ClusterRegistrationImpl reg = getV2ClusterRegistration(cluster);
    List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();

    Map<DbusPartitionInfo, DatabusRegistration> regMap = reg.getPartitionRegs();
    for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
    {
      PartitionInfo p =
          new PartitionInfo(e.getKey().getPartitionId(), e.getValue().getRegistrationId());
      partitions.add(p);
    }
    return partitions;
  }

  /**
   * Get the list of partitions hosted by this client for the V3 cluster.
   * 
   * @param cluster
   *          V3 CLuster for which we need to find out the partitions.
   * @return
   * @throws RequestProcessingException
   *           when unable to find the cluster.
   */
  private Collection<PartitionInfo> getV3ClusterPartitions(String cluster) throws RequestProcessingException
  {
    DatabusV3MultiPartitionRegistration reg = getV3ClusterRegistration(cluster);
    List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();

    Map<PhysicalPartition, DatabusV3Registration> regMap = reg.getPartionRegs();
    for (Entry<PhysicalPartition, DatabusV3Registration> e : regMap.entrySet())
    {
      PartitionInfo p =
          new PartitionInfo(e.getKey().getId(), e.getValue().getRegistrationId());
      partitions.add(p);
    }
    return partitions;
  }

  /**
   * Helper method to get partition registration information for a given V2 Cluster
   * partition
   * 
   * @param cluster
   *          V2 Cluster
   * @param partition
   *          Partition in the cluster.
   * @return
   * @throws RequestProcessingException
   *           When cluster or partition is not hosted in this instance.
   */
  private RegInfo getV2PartitionRegistration(String cluster, long partition) throws RequestProcessingException
  {
    DatabusV2ClusterRegistrationImpl reg = getV2ClusterRegistration(cluster);
    DbusPartitionInfo p = new DbusPartitionInfoImpl(partition);
    DatabusRegistration r = reg.getPartitionRegs().get(p);

    if (null == r)
      throw new RequestProcessingException("Partition(" + partition + ") for cluster ("
          + cluster + ") not found !!");

    return new RegInfo(r.getState().name(),
                       r.getRegistrationId(),
                       r.getStatus(),
                       r.getFilterConfig(),
                       r.getSubscriptions());
  }

  /**
   * Helper method to get partition registration information for a given V3 Cluster
   * partition
   * 
   * @param cluster
   *          V3 Cluster
   * @param partition
   *          Partition in the cluster.
   * @return
   * @throws RequestProcessingException
   *           When cluster or partition is not hosted in this instance.
   */
  private RegInfo getV3PartitionRegistration(String cluster, long partition) throws RequestProcessingException
  {
    DatabusV3MultiPartitionRegistration reg = getV3ClusterRegistration(cluster);

    for (Entry<PhysicalPartition, DatabusV3Registration> e : reg.getPartionRegs()
                                                                .entrySet())
    {
      if (partition == e.getKey().getId())
      {
        DatabusV3Registration r = e.getValue();
        return new RegInfo(r.getState().name(),
                           r.getRegistrationId(),
                           r.getStatus(),
                           null,
                           r.getSubscriptions());
      }
    }

    throw new RequestProcessingException("Partition(" + partition + ") for cluster ("
        + cluster + ") not found !!");
  }

  /**
   * Helper method to locate a databus V2 registration by its registration id. This method
   * can locate both top-level (registered by one of _dbusClient.registerXXX()) and
   * individual-partition (child) registration that are aggregated inside a top-level
   * MultiPartition registration.
   * 
   * Please note that this can traverse the registration tree which is 1 level deep. In
   * other words, it will not work when we have MultiPartition registrations aggregated
   * inside another MultiPartition registrations.
   * 
   * @param regId
   *          Registration Id to be located
   * @param request
   *          Databus Request corresponding to the REST call.
   * @return
   * @throws RequestProcessingException
   *           when the registration is not found.
   */
  private DatabusRegistration findV2Registration(DatabusRequest request, String prefix) throws RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String registrationIdStr = category.substring(prefix.length());
    RegistrationId regId = new RegistrationId(registrationIdStr);

    Collection<DatabusRegistration> regs = _client.getAllRegistrations();

    if (null != regs)
    {
      for (DatabusRegistration r : regs)
      {
        if (regId.equals(r.getRegistrationId()))
        {
          return r;
        }

        /**
         * Important Note: There is an important implementation difference on which
         * registrations are stored in the global registration data-structure maintained
         * by the client (DatabusHttp[V3]ClientImpls) between V2 and V3.
         * 
         * 1. In the case of V2, only top-level registrations are stored in the global
         * data-structure (DatabusHttpClientImpl.regList 2. In the case of V3, all
         * registrations are stored in the global data-structure.
         * 
         * In the case of V3, this is needed so that all registrations can act on the
         * relay external view change. This can be refactored in the future by moving the
         * relay-external view change to registration impl ( reduce the complexity in
         * ClientImpl ). The V2 implementation did not have this logic and was following a
         * more intuitive structure of preserving the hierarchy. The below code handles
         * the discrepancy for V2.
         */
        if (r instanceof DatabusMultiPartitionRegistration)
        {
          Map<DbusPartitionInfo, DatabusRegistration> childRegs =
              ((DatabusMultiPartitionRegistration) r).getPartitionRegs();
          for (Entry<DbusPartitionInfo, DatabusRegistration> e : childRegs.entrySet())
          {
            if (regId.equals(e.getValue().getRegistrationId()))
            {
              return e.getValue();
            }
          }
        }
      }
    }
    throw new RequestProcessingException("Unable to find registration (" + regId + ") ");
  }

  /**
   * Helper method to locate a databus V3 registration by its registration id. This method
   * can locate both top-level (registered by one of _dbusClient.registerXXX()) and
   * individual-partition (child) registration that are aggregated inside a top-level
   * MultiPartition registration.
   * 
   * Please note that this can traverse the registration tree which is 1 level deep. In
   * other words, it will not work when we have MultiPartition registrations aggregated
   * inside another MultiPartition registrations.
   * 
   * @param regId
   *          Registration Id to be located
   * @param request
   *          Databus Request corresponding to the REST call.
   * @return
   * @throws RequestProcessingException
   *           when the registration is not found.
   */
  private DatabusV3Registration findV3Registration(RegistrationId regId,
                                                   DatabusRequest request) throws RequestProcessingException
  {
    Map<RegistrationId, DatabusV3Registration> regIdMap = _client.getRegistrationIdMap();
    if (null == regIdMap)
    {
      throw new InvalidRequestParamValueException(request.getName(),
                                                  REGISTRATION_KEY_PREFIX,
                                                  "No registrations available !! ");
    }

    /**
     * Important Note: There is an important implementation difference on which
     * registrations are stored in the global registration data-structure maintained by
     * the client (DatabusHttp[V3]ClientImpls) between V2 and V3.
     * 
     * 1. In the case of V2, only top-level registrations are stored in the global
     * data-structure (DatabusHttpClientImpl.regList 2. In the case of V3, all
     * registrations are stored in the global data-structure.
     * 
     * In the case of V3, this is needed so that all registrations can act on the relay
     * external view change. This can be refactored in the future by moving the
     * relay-external view change to registration impl ( reduce the complexity in
     * ClientImpl ). The V2 implementation did not have this logic and was following a
     * more intuitive structure of preserving the hierarchy.
     */
    for (DatabusV3Registration r : regIdMap.values())
    {
      if (regId.equals(r.getRegistrationId()))
      {
        return r;
      }
    }
    throw new InvalidRequestParamValueException(request.getName(),
                                                REGISTRATION_KEY_PREFIX,
                                                "Registration with id " + regId
                                                    + " not present !!");
  }

  private DatabusV3Registration findV3Registration(DatabusRequest request, String prefix) throws RequestProcessingException
  {
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    String regIdStr = category.substring(prefix.length());
    RegistrationId regId = new RegistrationId(regIdStr);
    return findV3Registration(regId, request);
  }

  private static class PartitionInfo
  {
    private final long           partition;

    private final RegistrationId regId;

    public long getPartition()
    {
      return partition;
    }

    public RegistrationId getRegId()
    {
      return regId;
    }

    public PartitionInfo(long partition, RegistrationId regId)
    {
      super();
      this.partition = partition;
      this.regId = regId;
    }
  }

  private static class RegStatePair
  {
    private final String         _state;

    private final RegistrationId _regId;

    public String getState()
    {
      return _state;
    }

    public RegistrationId getRegId()
    {
      return _regId;
    }

    public RegStatePair(DatabusRegistration.RegistrationState state, RegistrationId regId)
    {
      _regId = regId;
      _state = state.name();
    }

    public RegStatePair(String state, RegistrationId regId)
    {
      _regId = regId;
      _state = state;
    }
  }

  private static class RegInfo
  {
    private final String                          state;

    private final RegistrationId                  regId;

    private final String                          status;

    private final DbusKeyCompositeFilterConfig    filter;

    private final Collection<DatabusSubscription> subs;

    private final boolean                         isMultiPartition;

    private final Map<DbusPartitionInfo, RegInfo> childRegistrations;

    public String getState()
    {
      return state;
    }

    public RegistrationId getRegId()
    {
      return regId;
    }

    public String getStatus()
    {
      return status;
    }

    public DbusKeyCompositeFilterConfig getFilter()
    {
      return filter;
    }

    public Collection<DatabusSubscription> getSubs()
    {
      return subs;
    }

    public boolean isMultiPartition()
    {
      return isMultiPartition;
    }

    public Map<DbusPartitionInfo, RegInfo> getChildRegistrations()
    {
      return childRegistrations;
    }

    public RegInfo(String state,
                   RegistrationId regId,
                   DatabusComponentStatus status,
                   DbusKeyCompositeFilterConfig filter,
                   Collection<DatabusSubscription> subs)
    {
      this(state, regId, status, filter, subs, false, null);
    }

    public RegInfo(String state,
                   RegistrationId regId,
                   DatabusComponentStatus status,
                   DbusKeyCompositeFilterConfig filter,
                   Collection<DatabusSubscription> subs,
                   boolean isMultiPartition,
                   Map<DbusPartitionInfo, RegInfo> childRegistrations)
    {
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
