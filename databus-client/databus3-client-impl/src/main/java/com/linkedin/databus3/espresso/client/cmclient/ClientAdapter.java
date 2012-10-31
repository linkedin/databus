package com.linkedin.databus3.espresso.client.cmclient;

import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus3.espresso.cmclient.Constants;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.ExternalView;

/**
 * An adapter between the client and the RelayClusterManager
 * Provides functionality to instantiate a ClusterManager spectator,
 * listen to external view changes for changes in relays and sources they serve
 *
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ClientAdapter implements ExternalViewChangeListener, IAdapter
{
	private final boolean _enableExternalViewChangeNotification;
	private HelixManager _storageClusterManager;
	private final String _clusterName, _zkConnectString;
	private static Logger LOG = Logger.getLogger(ClientAdapter.class);

	private final List<DatabusExternalViewChangeObserver> _viewChangeObservers = new ArrayList<DatabusExternalViewChangeObserver>();

	private final Map<String, ExternalView> _externalViews = new ConcurrentHashMap<String, ExternalView>();
	private final Map<String, Map<ResourceKey, List<DatabusServerCoordinates>>> _resourceToServerCoordinatesMap =
			new ConcurrentHashMap<String, Map<ResourceKey, List<DatabusServerCoordinates>>>();
	private final Map<String, Map<DatabusServerCoordinates, List<ResourceKey>>> _serverCoordinatesToResourceMap =
			new ConcurrentHashMap<String, Map<DatabusServerCoordinates, List<ResourceKey>>>();

	private final Map<String, ExternalView> _oldExternalViews = new ConcurrentHashMap<String, ExternalView>();
	private final Map<String, Map<ResourceKey, List<DatabusServerCoordinates>>> _oldResourceToServerCoordinatesMap =
			new ConcurrentHashMap<String, Map<ResourceKey, List<DatabusServerCoordinates>>>();
	private final Map<String, Map<DatabusServerCoordinates, List<ResourceKey>>> _oldServerCoordinatesToResourceMap =
			new ConcurrentHashMap<String, Map<DatabusServerCoordinates, List<ResourceKey>>>();



	/**
	 *
	 * @param dbName
	 * @param zkConnectString
	 * @param clusterName
	 * @param file
	 * @throws Exception
	 */
	public ClientAdapter(String zkConnectString,
			String clusterName, String file, boolean enableDynamic) throws Exception
	{
		_clusterName = clusterName;
		_zkConnectString = zkConnectString;
		_enableExternalViewChangeNotification = enableDynamic;

		LOG.info("Creating a StorageAdapter for zkConnectString " + _zkConnectString + " on cluster " + _clusterName);

		if ( ! _enableExternalViewChangeNotification )
			LOG.info("Relay Cluster External View change notification is turned off for cluster :" + _clusterName);

		if (file == null)
		{
			_storageClusterManager = HelixManagerFactory.getZKHelixManager(clusterName, null, com.linkedin.helix.InstanceType.SPECTATOR , zkConnectString);
			_storageClusterManager.connect();
			_storageClusterManager.addExternalViewChangeListener(this);
		}
		else
		{
			// file based
		}
	}

	/**
	 *
	 * @param dbName
	 * @param zkConnectString
	 * @param clusterName
	 * @param cm
	 * @param enableDynamic
	 */
	protected ClientAdapter(String zkConnectString,
			                String clusterName,
			                HelixManager cm,
			                boolean enableDynamic)
	{
		_clusterName = clusterName;
		_zkConnectString = zkConnectString;
		_storageClusterManager = cm;
		_enableExternalViewChangeNotification = enableDynamic;
	}

	/**
	 * For a client to read the external view of the relay cluster
	 * This would give the mapping between which (PS,PP,LS) -> Relays
	 *
	 * cached = false, will fetch it from ZooKeeper
	 * cached = true, will return from the cache.
	 *
	 * @param resourceGroup
	 * @return
	 */
	@Override
	public Map<ResourceKey, List<DatabusServerCoordinates>> getExternalView(boolean cached, String dbName)
	{
		if (cached && _externalViews.containsKey(dbName))
		{
			assert(null != _resourceToServerCoordinatesMap);
			return _resourceToServerCoordinatesMap.get(dbName);
		}
		else
		{
			// Save old state and refresh
			if (_externalViews.containsKey(dbName))
			{
				_oldExternalViews.put(dbName, _externalViews.get(dbName));
			}
			if (_resourceToServerCoordinatesMap.containsKey(dbName) && _serverCoordinatesToResourceMap.containsKey(dbName))
			{
				_oldResourceToServerCoordinatesMap.put(dbName, _resourceToServerCoordinatesMap.get(dbName));
				_oldServerCoordinatesToResourceMap.put(dbName, _serverCoordinatesToResourceMap.get(dbName));
			}

			ExternalView ev = null;
			try
			{
				ev = _storageClusterManager.getClusterManagmentTool().getResourceExternalView(_clusterName, dbName);
			} catch (Exception e)
			{
				LOG.error("Error obtaining external view from cluster", e);
				ev = null;
			}
			if ( null != ev)
			{
				_externalViews.put(dbName, ev);
			}
			Map<ResourceKey, List<DatabusServerCoordinates>> rsc =  getExternalView(ev);
			Map<DatabusServerCoordinates, List<ResourceKey>> scr = getInverseExternalView(ev);

			if (null != rsc && null != scr)
			{
				_resourceToServerCoordinatesMap.put(dbName, rsc);
				_serverCoordinatesToResourceMap.put(dbName, scr);
			}
			return rsc;
		}
	}

	@Override
	public Map<DatabusServerCoordinates, List<ResourceKey>> getInverseExternalView(boolean cached, String dbName)
	{
		if (cached && _externalViews.containsKey(dbName))
		{
			assert(null != _serverCoordinatesToResourceMap);
			return _serverCoordinatesToResourceMap.get(dbName);
		}
		else
		{
			// Save old state and refresh
			if (_externalViews.containsKey(dbName))
			{
				_oldExternalViews.put(dbName, _externalViews.get(dbName));
			}
			if (_resourceToServerCoordinatesMap.containsKey(dbName) && _serverCoordinatesToResourceMap.containsKey(dbName))
			{
	 			_oldResourceToServerCoordinatesMap.put(dbName, _resourceToServerCoordinatesMap.get(dbName));
				_oldServerCoordinatesToResourceMap.put(dbName, _serverCoordinatesToResourceMap.get(dbName));
			}

			ExternalView ev = null;
			try
			{
				ev = _storageClusterManager.getClusterManagmentTool().getResourceExternalView(_clusterName, dbName);
			} catch ( Exception e)
			{
				LOG.error("Error obtaining external view from cluster", e);
				ev = null;
			}
			if ( null != ev)
				_externalViews.put(dbName, ev);
			Map<ResourceKey, List<DatabusServerCoordinates>> rsc =  getExternalView(ev);
			Map<DatabusServerCoordinates, List<ResourceKey>> scr = getInverseExternalView(ev);

			if (null != rsc && null != scr)
			{
				_resourceToServerCoordinatesMap.put(dbName, rsc);
				_serverCoordinatesToResourceMap.put(dbName, scr);
			}
			return scr;
		}
	}

	protected static Map<DatabusServerCoordinates, List<ResourceKey>> getInverseExternalView(ExternalView currentExternalView)
	{
		ZNRecord zn = null;
		if (null != currentExternalView)
			zn = currentExternalView.getRecord();
		Map<DatabusServerCoordinates, List<ResourceKey>> externalView = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
		if (null == zn || zn.getId().isEmpty())
		{
			LOG.error("External View for the relay cluster seems empty as ZNRecord passed is empty");
			return externalView;
		}

		Map<String, Map<String,String>> ev = zn.getMapFields();

		for ( String s : ev.keySet())
		{
			try
			{
				ResourceKey rk = new ResourceKey(s);
				Map<String, String> relayStates = ev.get(s);

				for (String relay: relayStates.keySet())
				{
					String []ip = relay.split("_");
					if (ip.length != 2)
						throw new ParseException("Relay not specified in the form of IPAddr_PortNo " + relay, ip.length );

					InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
					DatabusServerCoordinates rc = new DatabusServerCoordinates(ia, relayStates.get(relay));

					List<ResourceKey> rKeys = externalView.get(rc);

					if ( null == rKeys)
					{
						rKeys = new ArrayList<ResourceKey>();
						externalView.put(rc, rKeys);
					}
					rKeys.add(rk);
				}
			}
			catch(ParseException e)
			{
				LOG.error("Error parsing a resourceKey : " + s, e);
			}
		}
		return externalView;
	}


	protected static Map<ResourceKey, List<DatabusServerCoordinates>> getExternalView(ExternalView currentExternalView)
	{
		ZNRecord zn = null;
		if (null != currentExternalView)
			zn = currentExternalView.getRecord();
		Map<ResourceKey, List<DatabusServerCoordinates>> externalView = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();

		if ( zn == null || zn.getId().isEmpty())
		{
			LOG.error("External View for the relay cluster seems empty as ZNRecord passed is null");
			return externalView;
		}

		Map<String, Map<String,String>> ev = zn.getMapFields();

		for ( String s : ev.keySet())
		{
			try
			{
				ResourceKey rk = new ResourceKey(s);
				List<DatabusServerCoordinates> rcs = new ArrayList<DatabusServerCoordinates>();

				Map<String, String> relayStates = ev.get(s);
				for (String relay: relayStates.keySet())
				{
					String []ip = relay.split("_");
					if (ip.length != 2)
						throw new ParseException("Relay not specified in the form of IPAddr_PortNo " + relay, ip.length );

					InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
					DatabusServerCoordinates rc = new DatabusServerCoordinates(ia, relayStates.get(relay));
					rcs.add(rc);
				}

				externalView.put(rk, rcs);
			}
			catch(ParseException e)
			{
				LOG.error("Error parsing a resourceKey : " + s, e);
			}
		}
		return externalView;
	}

	/**
	 *
	 */
	@Override
	public synchronized void onExternalViewChange(List<ExternalView> externalViews, NotificationContext notificationCtxt)
	{
		if (notificationCtxt == null)
		{
			LOG.error("Got onExternalViewChange notification with null context");
			return;
		}

		if ( notificationCtxt.getType() == NotificationContext.Type.FINALIZE)
		{
			return;
		}
		LOG.debug("Databus Client got notified by Zookeeper on relay state change !! znRecords :" + externalViews);

		if ( _enableExternalViewChangeNotification )
		{
			for(Iterator<ExternalView> it= externalViews.iterator(); it.hasNext();)
			{
				ZNRecord zn = it.next().getRecord();

				LOG.info("Relay state changed for db :" + zn.getId() );

				String dbName = zn.getId();
				if (dbName.equals(Constants.LEADER_STANDBY_NAME))
				{
					LOG.debug("Skipping external view change notification for dbName " + dbName);
					continue;
				}
				// Update the ideal state of relay
				if (_externalViews.containsKey(dbName))
					_oldExternalViews.put(dbName, _externalViews.get(dbName));

				if (_resourceToServerCoordinatesMap.containsKey(dbName) && _serverCoordinatesToResourceMap.containsKey(dbName))
				{
					_oldResourceToServerCoordinatesMap.put(dbName, _resourceToServerCoordinatesMap.get(dbName));
					_oldServerCoordinatesToResourceMap.put(dbName, _serverCoordinatesToResourceMap.get(dbName));
				}

				ExternalView ev = new ExternalView(zn);
				if ( null != ev)
					_externalViews.put(dbName, ev);
				Map<ResourceKey, List<DatabusServerCoordinates>> rsc =  getExternalView(ev);
				Map<DatabusServerCoordinates, List<ResourceKey>> scr = getInverseExternalView(ev);

				if ( null != rsc && null != scr)
				{
					_resourceToServerCoordinatesMap.put(dbName, rsc);
					_serverCoordinatesToResourceMap.put(dbName, scr);
				}
				LOG.info("Updated external view for dbName " + dbName);

				for (DatabusExternalViewChangeObserver observer : _viewChangeObservers)
				{
					observer.onExternalViewChange(dbName,
							_oldResourceToServerCoordinatesMap.get(dbName),
							_oldServerCoordinatesToResourceMap.get(dbName),
							_resourceToServerCoordinatesMap.get(dbName),
							_serverCoordinatesToResourceMap.get(dbName));
				}
			}
		} else {
			LOG.info("Databus CLient skipping this ZK notification since it is turned off");
		}
		return;
	}


	@Override
	public void addExternalViewChangeObservers(DatabusExternalViewChangeObserver observer)
	{
		boolean found  = false;
		for (DatabusExternalViewChangeObserver obs : _viewChangeObservers)
		{
			if (obs.equals(observer))
			{
				found  = true;
				break;
			}
		}

		if (!found)
			_viewChangeObservers.add(observer);
	}


	@Override
	public void removeExternalViewChangeObservers(DatabusExternalViewChangeObserver observer)
	{
		Iterator<DatabusExternalViewChangeObserver> itr = _viewChangeObservers.iterator();

		while (itr.hasNext())
		{
			DatabusExternalViewChangeObserver a = itr.next();

			if (a.equals(observer))
			{
				itr.remove();
				break;
			}
		}
	}

	@Override
	public int getNumPartitions(String dbName)
	{
		String key = dbName + Constants.NUM_PARTITIONS_SUFFIX;
		ConfigScope cs = new ConfigScopeBuilder().forCluster(_clusterName).build();
		Set<String> keySet = new HashSet<String>();
		keySet.add(key);
		Map<String, String> numPartitionsStr = _storageClusterManager.getClusterManagmentTool().getConfig(cs, keySet);
		int numPartitions = Integer.parseInt(numPartitionsStr.get(key));		
		LOG.info("Number of partitions for cluster " + _clusterName + " and dbName " + dbName + " is " + numPartitions); 
		return numPartitions;
	}

}
