package com.linkedin.databus.cluster;

import org.apache.log4j.Logger;

import com.linkedin.helix.participant.statemachine.StateModelFactory;

public class DatabusClusterNotifierFactory extends StateModelFactory<DatabusClusterNotifierStateModel>
{
	
	private DatabusClusterNotifier _notifier;
	private static final Logger LOG = Logger.getLogger(DatabusClusterNotifierStateModel.class);

	public DatabusClusterNotifierFactory(DatabusClusterNotifier notifier)
	{
		_notifier = notifier;
	}

	@Override
	public DatabusClusterNotifierStateModel createNewStateModel(String partition)
	{
		LOG.warn("Creating a new callback object for partition=" + partition);
		return new DatabusClusterNotifierStateModel(partition,_notifier);
	}

}
