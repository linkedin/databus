package com.linkedin.databus.cluster;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

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
