package com.linkedin.databus.client.monitoring;
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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.databus.client.BootstrapPullThread;
import com.linkedin.databus.client.ConnectionState;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.DispatcherState;
import com.linkedin.databus.client.GenericDispatcher;
import com.linkedin.databus.client.RelayPullThread;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusV3MultiPartitionRegistration;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;

public class RegistrationStatsInfo
{
	private RegistrationId regId;
	private RegistrationId parentRegId;
	private List<DatabusSubscription> subscriptions;
	private ServerInfo currentRelay;
	private ServerInfo currentBootstrapServer;
	private Set<ServerInfo> candidateRelays;
	private Set<ServerInfo> candidateBootstrapServers;
	private DispatcherState.StateId relayDispatcherConnectionState;
	private DispatcherState.StateId bootstrapDispatcherConnectionState;
	private ConnectionState.StateId relayPullerConnectionState;
	private ConnectionState.StateId bootstrapPullerConnectionState;
	private DatabusComponentStatus.Status  relayDispatcherComponentStatus;
	private DatabusComponentStatus.Status  bootstrapDispatcherComponentStatus;
	private DatabusComponentStatus.Status  relayPullerComponentStatus;
	private DatabusComponentStatus.Status  bootstrapPullerComponentStatus;
	private boolean _multiPartition = false;
	private List<RegistrationId> _childrenRegistrations = Collections.emptyList();
	private SCN _highWatermark;

	public RegistrationStatsInfo()
	{
	}

	public RegistrationStatsInfo(DatabusV3Registration reg, DatabusSourcesConnection sourcesConn)
    {
	  setRegId(reg.getId());
	  setParentRegId(null != reg.getParentRegistration() ? reg.getParentRegistration().getId() : null);
	  setSubscriptions(reg.getSubscriptions());

      if (reg instanceof DatabusV3MultiPartitionRegistration)
      {
        DatabusV3MultiPartitionRegistration mpReg = (DatabusV3MultiPartitionRegistration)reg;
        setMultiPartition(true);
        ArrayList<RegistrationId> childrenRegs =
        new ArrayList<RegistrationId>(mpReg.getPartionRegs().size());
        for (Map.Entry<PhysicalPartition, DatabusV3Registration> child: mpReg.getPartionRegs().entrySet())
        {
          childrenRegs.add(child.getValue().getId());
        }
        setChildrenRegistrations(childrenRegs);
      }

      if ( null != sourcesConn )
      {
        RelayPullThread rp = sourcesConn.getRelayPullThread();
        BootstrapPullThread bp = sourcesConn.getBootstrapPullThread();
        GenericDispatcher<DatabusCombinedConsumer>  rd = sourcesConn.getRelayDispatcher();
        GenericDispatcher<DatabusCombinedConsumer>  bd = sourcesConn.getBootstrapDispatcher();

      if ( null != rp)
      {
        setRelayPullerConnectionState(rp.getConnectionState().getStateId());
        if ( null != rp.getComponentStatus())
          setRelayPullerComponentStatus(rp.getComponentStatus().getStatus());

        setCurrentRelay(rp.getCurentServer());
        setCandidateRelays(rp.getServers());
        }

        if ( null != bp)
        {
          setBootstrapPullerConnectionState(bp.getConnectionState().getStateId());
          if ( null != bp.getComponentStatus())
            setBootstrapPullerComponentStatus(bp.getComponentStatus().getStatus());

          setCurrentBootstrapServer(bp.getCurentServer());
          setCandidateBootstrapServers(bp.getServers());
        }

        if ( null != rd)
        {
          if ( null != rd.getComponentStatus())
            setRelayDispatcherComponentStatus(rd.getComponentStatus().getStatus());

          if (null != rd.getDispatcherState())
            setRelayDispatcherConnectionState(rd.getDispatcherState().getStateId());
        }

        if ( null != bd)
        {
          if ( null != bd.getComponentStatus())
            setBootstrapDispatcherComponentStatus(bd.getComponentStatus().getStatus());
          if ( null != bd.getDispatcherState())
            setBootstrapDispatcherConnectionState(bd.getDispatcherState().getStateId());
        }
      }
    }

	public RegistrationId getRegId() {
		return regId;
	}

	public void setRegId(RegistrationId regId) {
		this.regId = regId;
	}

	public RegistrationId getParentRegId() {
    return parentRegId;
  }

  public void setParentRegId(RegistrationId regId) {
    parentRegId = regId;
  }

	public List<DatabusSubscription> getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(List<DatabusSubscription> subscriptions) {
		this.subscriptions = subscriptions;
	}

	public ServerInfo getCurrentRelay() {
		return currentRelay;
	}

	public void setCurrentRelay(ServerInfo currentRelay) {
		this.currentRelay = currentRelay;
	}

	public ServerInfo getCurrentBootstrapServer() {
		return currentBootstrapServer;
	}

	public void setCurrentBootstrapServer(ServerInfo currentBootstrapServer) {
		this.currentBootstrapServer = currentBootstrapServer;
	}

	public Set<ServerInfo> getCandidateRelays() {
		return candidateRelays;
	}

	public void setCandidateRelays(Set<ServerInfo> candidateRelays) {
		this.candidateRelays = candidateRelays;
	}

	public Set<ServerInfo> getCandidateBootstrapServers() {
		return candidateBootstrapServers;
	}

	public void setCandidateBootstrapServers(
			Set<ServerInfo> candidateBootstrapServers) {
		this.candidateBootstrapServers = candidateBootstrapServers;
	}

	public DispatcherState.StateId getRelayDispatcherConnectionState() {
		return relayDispatcherConnectionState;
	}

	public void setRelayDispatcherConnectionState(
			DispatcherState.StateId relayDispatcherConnectionState) {
		this.relayDispatcherConnectionState = relayDispatcherConnectionState;
	}

	public DispatcherState.StateId getBootstrapDispatcherConnectionState() {
		return bootstrapDispatcherConnectionState;
	}

	public void setBootstrapDispatcherConnectionState(
			DispatcherState.StateId bootstrapDispatcherConnectionState) {
		this.bootstrapDispatcherConnectionState = bootstrapDispatcherConnectionState;
	}

	public ConnectionState.StateId getRelayPullerConnectionState() {
		return relayPullerConnectionState;
	}

	public void setRelayPullerConnectionState(
			ConnectionState.StateId relayPullerConnectionState) {
		this.relayPullerConnectionState = relayPullerConnectionState;
	}

	public ConnectionState.StateId getBootstrapPullerConnectionState() {
		return bootstrapPullerConnectionState;
	}

	public void setBootstrapPullerConnectionState(
			ConnectionState.StateId bootstrapPullerConnectionState) {
		this.bootstrapPullerConnectionState = bootstrapPullerConnectionState;
	}

	public DatabusComponentStatus.Status  getRelayDispatcherComponentStatus() {
		return relayDispatcherComponentStatus;
	}

	public void setRelayDispatcherComponentStatus(
			DatabusComponentStatus.Status  relayDispatcherComponentStatus) {
		this.relayDispatcherComponentStatus = relayDispatcherComponentStatus;
	}

	public DatabusComponentStatus.Status  getBootstrapDispatcherComponentStatus() {
		return bootstrapDispatcherComponentStatus;
	}

	public void setBootstrapDispatcherComponentStatus(
			DatabusComponentStatus.Status  bootstrapDispatcherComponentStatus) {
		this.bootstrapDispatcherComponentStatus = bootstrapDispatcherComponentStatus;
	}

	public DatabusComponentStatus.Status  getRelayPullerComponentStatus() {
		return relayPullerComponentStatus;
	}

	public void setRelayPullerComponentStatus(
			DatabusComponentStatus.Status  relayPullerComponentStatus) {
		this.relayPullerComponentStatus = relayPullerComponentStatus;
	}

	public DatabusComponentStatus.Status getBootstrapPullerComponentStatus() {
		return bootstrapPullerComponentStatus;
	}

	public void setBootstrapPullerComponentStatus(
			DatabusComponentStatus.Status  bootstrapPullerComponentStatus) {
		this.bootstrapPullerComponentStatus = bootstrapPullerComponentStatus;
	}

  public boolean isMultiPartition()
  {
    return _multiPartition;
  }

  public void setMultiPartition(boolean multiPartition)
  {
    _multiPartition = multiPartition;
  }

  public List<RegistrationId> getChildrenRegistrations()
  {
    return _childrenRegistrations;
  }

  public void setChildrenRegistrations(List<RegistrationId> childrenRegistrations)
  {
    _childrenRegistrations = childrenRegistrations;
  }

  public SCN getHighWatermark()
  {
    return _highWatermark;
  }

  public void setHighWatermark(SCN highWatermark)
  {
    _highWatermark = highWatermark;
  }
}
