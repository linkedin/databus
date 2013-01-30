package com.linkedin.databus.core.monitoring.mbean;
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


import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEvent.EventScanStatus;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.JmxUtil;
import com.linkedin.databus.core.util.ReadWriteSyncedObject;


/**
 * Container for all monitoring mbeans
 * @author cbotev
 */
public class DbusEventsStatisticsCollector extends ReadWriteSyncedObject
                                           implements DbusEventsStatisticsCollectorMBean,StatsCollectorMergeable<DbusEventsStatisticsCollector>
{
  public static final String MODULE = DbusEventsStatisticsCollector.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String NO_PEER = "none";

  private final DbusEventsTotalStats _totalStats;

  private final HashMap<Integer, DbusEventsTotalStats> _perSourceStats;
  private final HashMap<String, DbusEventsTotalStats> _perPeerStats;

  private final MBeanServer _mbeanServer;
  private final ObjectName _collectorObjName;

  private final int _id;
  private final String _name;
  private final String _sanitizedName;
  private final String _curPeer;
  private final String _perSourceNamePrefix;
  private final String _perPeerNamePrefix;
  private final AtomicBoolean _enabled;
  //cache the last event source index object
  private Integer _lastUsedSrcIdx = Integer.valueOf(1);

  public DbusEventsStatisticsCollector(int relayId, String name, boolean enabled, boolean threadSafe,
                                       MBeanServer mbeanServer)
  {
    this(relayId, name, enabled, threadSafe, NO_PEER, mbeanServer);
  }

  protected DbusEventsTotalStats makeDbusEventsTotalStats(int ownerId,
                                                          String dimension,
                                                          boolean enabled,
                                                          boolean threadSafe,
                                                          DbusEventsTotalStatsEvent initData)
  {
    return new DbusEventsTotalStats(ownerId, dimension, enabled, threadSafe, initData);
  }

  private DbusEventsStatisticsCollector(int relayId, String name, boolean enabled,
                                        boolean threadSafe, String client,
                                        MBeanServer mbeanServer
                                        )
  {
    super(threadSafe);

    _id = relayId;
    _name =  name ;
    _sanitizedName = AbstractMonitoringMBean.sanitizeString(_name);
    _mbeanServer = mbeanServer;
    _curPeer = client;
    _enabled = new AtomicBoolean(enabled);
    _perSourceNamePrefix = _sanitizedName + ".source.";
    _perPeerNamePrefix = _sanitizedName + ".peer.";


    _totalStats = makeDbusEventsTotalStats(_id, _sanitizedName + ".total", enabled, false,
                                           null);

    _perSourceStats = new HashMap<Integer, DbusEventsTotalStats>(100);

    _perPeerStats = new HashMap<String, DbusEventsTotalStats>(1000);

    ObjectName jmxName = null;
    try
    {
      Hashtable<String, String> mbeanProps = new Hashtable<String, String>(5);
      mbeanProps.put("name", _sanitizedName);
      mbeanProps.put("type", DbusEventsStatisticsCollector.class.getSimpleName());
      mbeanProps.put("relay", Integer.toString(relayId));
      jmxName = new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
    }
    catch (Exception e)
    {
      LOG.error("Error creating JMX object name", e);
    }

    _collectorObjName = jmxName;

    registerAsMBeans();
  }

  protected void registerAsMBeans()
  {
    if (null != _mbeanServer && null != _collectorObjName)
    {
      try
      {
        if (_mbeanServer.isRegistered(_collectorObjName))
        {
          LOG.warn("unregistering stale mbean: " + _collectorObjName);
          _mbeanServer.unregisterMBean(_collectorObjName);
        }

        _mbeanServer.registerMBean(this, _collectorObjName);
        _totalStats.registerAsMbean(_mbeanServer);
        LOG.info("MBean registered " + _collectorObjName);
      }
      catch (Exception e)
      {
        LOG.error("JMX registration failed", e);
      }
    }
  }

  public void unregisterMBeans()
  {
    if (null != _mbeanServer && null != _collectorObjName)
    {
      try
      {
        JmxUtil.unregisterMBeanSafely(_mbeanServer, _collectorObjName, LOG);
        _totalStats.unregisterMbean(_mbeanServer);

        for (String clientName: _perPeerStats.keySet())
        {
          _perPeerStats.get(clientName).unregisterMbean(_mbeanServer);
        }

        for (Integer srcId: _perSourceStats.keySet())
        {
          _perSourceStats.get(srcId).unregisterMbean(_mbeanServer);
        }


        LOG.info("MBean unregistered " + _collectorObjName);
      }
      catch (Exception e)
      {
        LOG.error("JMX deregistration failed", e);
      }
    }
  }

  /** Creates a copy */
  public DbusEventsStatisticsCollector createForPeerConnection(String client)
  {
    return new DbusEventsStatisticsCollector(_id, client, true, false, client, null);
  }

  @Override
  public DbusEventsTotalStats getTotalStats()
  {
    return _totalStats;
  }


/*//FIXME snagaraj
  @Override
  public DbusEventsTotalStats getPhysicalSourceStats(String physicalParitionStr)
  {
    PhysicalPartition ppart = PhysicalPartition.createFromSimpleString(physicalParitionStr);

    Lock readLock = acquireReadLock();
    try
    {
      DbusEventsTotalStats result =
          ppart.equals(_collectorPartition) ? _totalStats :
              null == _perPhysicalPartitionStats ? null : _perPhysicalPartitionStats.get(ppart);
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }
*/

  @Override
  public List<Integer> getSources()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return new ArrayList<Integer>(_perSourceStats.keySet());
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public DbusEventsTotalStats getSourceStats(int srcId)
  {
    Lock readLock = acquireReadLock();
    try
    {
      if (srcId != _lastUsedSrcIdx) _lastUsedSrcIdx = srcId;

      DbusEventsTotalStats result = _perSourceStats.get(_lastUsedSrcIdx);
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public List<String> getPeers()
  {
    Lock readLock = acquireReadLock();
    try
    {
      ArrayList<String> result = new ArrayList<String>(_perPeerStats.keySet());
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public DbusEventsTotalStats getPeerStats(String peer)
  {
    Lock readLock = acquireReadLock();
    try
    {
      DbusEventsTotalStats result = _perPeerStats.get(peer);
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  public void registerDataEvent(DbusEvent e)
  {
    if (!_enabled.get()) return;

    short srcId = e.srcId();
    if (! DbusEvent.isControlSrcId(srcId))
    {
      _totalStats.registerDataEvent(e);
      DbusEventsTotalStats data = getOrAddPerSourceCollector(srcId, null);
      data.registerDataEvent(e);
    }
    else
    {
      _totalStats.registerSysEvent(e);
    }

    if (NO_PEER != _curPeer)
    {
      DbusEventsTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, null);
      if (!DbusEvent.isControlSrcId(srcId))
      {
        clientStats.registerDataEvent(e);
      }
      else
      {
        clientStats.registerSysEvent(e);
      }

    }
  }

  public void registerDataEventFiltered(DbusEvent e)
  {
    if (!_enabled.get()) return;

    short srcId = e.srcId();
    if (!DbusEvent.isControlSrcId(srcId))
    {
      _totalStats.registerDataEventFiltered(e);
      DbusEventsTotalStats data = getOrAddPerSourceCollector(srcId, null);
      data.registerDataEventFiltered(e);

      if (NO_PEER != _curPeer)
      {
    	  DbusEventsTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, null);
    	  clientStats.registerDataEventFiltered(e);
      }
    }
  }

  @Override
  public void merge(DbusEventsStatisticsCollector other)
  {
    _totalStats.mergeStats(other._totalStats);
    mergeInternalStats(other);
  }

  //helper methods for merge
  void mergeInternalStats(DbusEventsStatisticsCollector other)
  {
	  Lock otherReadLock = other.acquireReadLock();
	  Lock writeLock = acquireWriteLock(otherReadLock);
	  try
	  {
		  for (Map.Entry<Integer, DbusEventsTotalStats> srcIdEntry: other._perSourceStats.entrySet())
		  {
			  mergePerSource(srcIdEntry.getKey(), srcIdEntry.getValue(), writeLock);
		  }

		  for (Map.Entry<String, DbusEventsTotalStats> peerEntry: other._perPeerStats.entrySet())
		  {
			  mergePerPeer(peerEntry.getKey(), peerEntry.getValue(), writeLock);
		  }
	  }
	  finally
	  {
		  releaseLock(writeLock);
		  releaseLock(otherReadLock);
	  }
  }
  
  @Override
  public void reset()
  {
    _totalStats.reset();
     resetInternalStats();
  }
  
  //helper methods 
  void resetInternalStats()
  {
	  Lock writeLock = acquireWriteLock();
	  try
	  {
		  for (Map.Entry<Integer, DbusEventsTotalStats> srcIdEntry: _perSourceStats.entrySet())
		  {
			  srcIdEntry.getValue().reset();
		  }

		  for (Map.Entry<String, DbusEventsTotalStats> peerEntry: _perPeerStats.entrySet())
		  {
			  peerEntry.getValue().reset();
		  }
	  }
	  finally
	  {
		  releaseLock(writeLock);
	  }
  }
  
  @Override
  public void resetAndMerge(List<DbusEventsStatisticsCollector> objList) 
  {
	  //make a copy ; reset; aggregate; then do atomic copy
	  DbusEventsTotalStats t = _totalStats.clone(true);
	  t.reset();
	  for (DbusEventsStatisticsCollector o: objList)
	  {
		  t.mergeStats(o.getTotalStats());
	  }
	  //update this object's state atomically
	  Lock writeLock = acquireWriteLock();
	  try
	  {	  
		  //atomic clone of t into totalStats; 
		  _totalStats.cloneData(t);
		  resetInternalStats();
		  for (DbusEventsStatisticsCollector o: objList)
		  {
			  mergeInternalStats(o);
		  }
	  }
	  finally
	  {
		  releaseLock(writeLock);
	  }
  }

  @Override
  public boolean isEnabled()
  {
    return _enabled.get();
  }

  @Override
  public void setEnabled(boolean enabled)
  {
    _enabled.set(enabled);
  }

  public String getSanitizedName()
  {
	  return _sanitizedName;
  }

  private DbusEventsTotalStats getOrAddPerSourceCollector(int srcId, Lock writeLock)
  {
    Lock myWriteLock = null;
    if (null == writeLock) myWriteLock = acquireWriteLock();


    try
    {
      if (srcId != _lastUsedSrcIdx) _lastUsedSrcIdx = srcId;

      DbusEventsTotalStats data = _perSourceStats.get(_lastUsedSrcIdx);

      if (null == data)
      {
        data = new AggregatedDbusEventsTotalStats(_id, _perSourceNamePrefix + srcId, true, isThreadSafe(), null);
        _perSourceStats.put(_lastUsedSrcIdx, data);

        if (null != _mbeanServer)
        {
          data.registerAsMbean(_mbeanServer);
        }
      }

      return data;
    }
    finally
    {
      releaseLock(myWriteLock);
    }
  }

  private DbusEventsTotalStats getOrAddPerPeerCollector(String peer, Lock writeLock)
  {
    Lock myWriteLock = null;
    if (null == writeLock) myWriteLock = acquireWriteLock();
    try
    {
      DbusEventsTotalStats peerStats = _perPeerStats.get(peer);
      if (null == peerStats)
      {
        peerStats = new AggregatedDbusEventsTotalStats(_id, _perPeerNamePrefix + peer, true,
                                               isThreadSafe(), null);
        _perPeerStats.put(peer, peerStats);

        if (null != _mbeanServer)
        {
          peerStats.registerAsMbean(_mbeanServer);
        }
      }

      return peerStats;
    }
    finally
    {
      releaseLock(myWriteLock);
    }
  }



  private void mergePerSource(Integer srcId, DbusEventsTotalStats other,
                                             Lock writeLock)
  {
    DbusEventsTotalStats curBean = getOrAddPerSourceCollector(srcId, writeLock);
    curBean.mergeStats(other);
  }

  private void mergePerPeer(String peerName, DbusEventsTotalStats other,
                                             Lock writeLock)
  {
    DbusEventsTotalStats curBean = getOrAddPerPeerCollector(peerName, writeLock);
    curBean.mergeStats(other);
  }

  

  public class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final boolean _enabled;

    public RuntimeConfig(boolean enabled)
    {
      _enabled = enabled;
    }

    /** A flag if events statistics collection is enabled  */
    public boolean isEnabled()
    {
      return _enabled;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      DbusEventsStatisticsCollector.this.setEnabled(this.isEnabled());
    }

    @Override
    public boolean equals(Object other)
    {
      if (null == other || ! (other instanceof RuntimeConfig)) return false;
      return equalsConfig((RuntimeConfig)other);
    }

    @Override
    public int hashCode()
    {
      return _enabled ? 1 : 0;
    }

    @Override
    public boolean equalsConfig(RuntimeConfig otherConfig)
    {
      return isEnabled() == otherConfig.isEnabled();
    }
  }

  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    private boolean _enabled = true;
    private DbusEventsStatisticsCollector _managedInstance = null;

    public RuntimeConfigBuilder()
    {
      super();
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    public void setEnabled(boolean enabled)
    {
      _enabled = enabled;
    }

    @Override
    public RuntimeConfig build() throws InvalidConfigException
    {
      if (null != _managedInstance) return _managedInstance.new RuntimeConfig(_enabled);
      throw new InvalidConfigException("No ContainerStatisticsCollector instance assigned");
    }

    public DbusEventsStatisticsCollector getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(DbusEventsStatisticsCollector managedInstance)
    {
      _managedInstance = managedInstance;
    }

  }

  public static class StaticConfig
  {
    private final RuntimeConfigBuilder _runtime;

    public StaticConfig(RuntimeConfigBuilder runtime)
    {
      _runtime = runtime;
    }

    /** Runtime configuration options */
    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private final RuntimeConfigBuilder _runtime;

    public Config()
    {
      super();
      _runtime = new RuntimeConfigBuilder();
    }

    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      return new StaticConfig(_runtime);
    }

  }

  public String getName()
  {
    return _name;
  }

  public void registerEventError(EventScanStatus writingEventStatus)
  {
    _totalStats.registerEventError(writingEventStatus);
    if (NO_PEER != _curPeer)
    {
      DbusEventsTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, null);
      clientStats.registerEventError(writingEventStatus);
     }
  }

  public void registerCreationTime(long s)
  {
    _totalStats.registerCreationTime(s);
    if (NO_PEER != _curPeer)
    {
      DbusEventsTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, null);
      clientStats.registerCreationTime(s);
    }

  }

  public void registerBufferMetrics(long r, long s,long since, long freeSpace)
  {
    //only global stats make sense
     _totalStats.registerBufferMetrics(r,s,since,freeSpace);
  }

  public void registerTimestampOfFirstEvent(long ts)
  {
    //only global makes sense
    _totalStats.registerTimestampOfFirstEvent(ts);
  }


  public int getOwnerId()
  {
    return _id;
  }


}

