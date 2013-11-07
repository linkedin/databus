package com.linkedin.databus.cluster;
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


import org.apache.log4j.Logger;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

/**
 * Helix model Factory that wraps user provided DatabusClusterNotifier
 */
@StateModelInfo(initialState = "OFFLINE", states = { "ONLINE", "ERROR" })
public class DatabusClusterNotifierStateModel extends StateModel
{

	private static final Logger LOG = Logger.getLogger(DatabusClusterNotifierStateModel.class);
	
	/**
	 * This is the application logic callback - invoked only when quorum number of nodes have joined
	 */
	private final DatabusClusterNotifier _notifier;
	/**
	 * The partition number passed by cluster manager ; right now it's resourceName_partition
	 */
	private final String _partition;

	public DatabusClusterNotifierStateModel(String partition,DatabusClusterNotifier notifier)
	{
		_notifier = notifier;
		String[] ps = partition.split("_");
		if (ps.length >= 2)
		{
		    _partition = ps[ps.length-1];
		} else {
		    _partition = "-1";
		}
	}

	@Transition(to = "ONLINE", from = "OFFLINE")
	public void onBecomeOnlineFromOffline(Message message,
			NotificationContext context)
	
	{
		LOG.debug("Gained partition " + _partition);
		if (_notifier != null)
		{
		    _notifier.onGainedPartitionOwnership(Integer.parseInt(_partition));
		}
	}

	@Transition(to = "OFFLINE", from = "ONLINE")
	public void onBecomeOfflineFromOnline(Message message,
			NotificationContext context)
	{
		LOG.debug("Lost partition " + _partition);
		if (_notifier != null)
		{
		    _notifier.onLostPartitionOwnership(Integer.parseInt(_partition));
		}
	}

	@Transition(to = "DROPPED", from = "OFFLINE")
	public void onBecomeDroppedFromOffline(Message message,
			NotificationContext context)
	{
		LOG.debug("Dropped partition " + _partition);
		if (_notifier != null)
		{
		    _notifier.onLostPartitionOwnership(Integer.parseInt(_partition));
		}
	}
	
	

	@Transition(to = "OFFLINE", from = "ERROR")
	public void onBecomeOfflineFromError(Message message,
			NotificationContext context)
	{
		LOG.debug("Offline from error for partition " + _partition);
		if (_notifier != null)
		{
			_notifier.onError(Integer.parseInt(_partition));
		}
	}

	@Override
	public void reset()
	{
		LOG.debug("Partition " + _partition + " reset");
		if (_notifier != null)
		{
			_notifier.onReset(Integer.parseInt(_partition));
		}
	}

}
