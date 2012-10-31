package com.linkedin.databus.bootstrap.common;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.monitoring.server.mbean.DbusBootstrapHttpStats;
import com.linkedin.databus.bootstrap.monitoring.server.mbean.DbusBootstrapHttpStatsMBean;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.util.ReadWriteSyncedObject;

public class BootstrapHttpStatsCollector extends ReadWriteSyncedObject implements
    BootstrapHttpStatsCollectorMBean
{

  private final static String NO_PEER = "NONE";

  public static final String MODULE = BootstrapHttpStatsCollector.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  private final DbusBootstrapHttpStats _totalStats;
  private final HashMap<String, DbusBootstrapHttpStats> _perClientStats;
  private final MBeanServer _mbeanServer;
  private final ObjectName _collectorObjName;

  private final int _id;
  private final String _name;
  private final String _perPeerPrefix;
  private final String _curPeer;
  private final AtomicBoolean _enabled;

  public BootstrapHttpStatsCollector(int id, String name, boolean enabled, boolean threadSafe,
                                 MBeanServer mbeanServer)
  {
    this(id, name, enabled, threadSafe, NO_PEER, mbeanServer);
  }

  public BootstrapHttpStatsCollector(
                                     int id,
                                     String name,
                                     boolean enabled,
                                     boolean threadSafe,
                                     String peer,
                                     MBeanServer mbeanServer)
  {
    super(threadSafe);
    _mbeanServer = mbeanServer;
    _curPeer = peer;
    _enabled = new AtomicBoolean(enabled);
    _id = id;
    _name = name;
    _perPeerPrefix = _name + ".peer.";

    _totalStats = new DbusBootstrapHttpStats(_id, _name + ".total", enabled, threadSafe, null);
    _perClientStats = new HashMap<String, DbusBootstrapHttpStats>(1000);
    ObjectName jmxName = null;
    try
    {
      Hashtable<String, String> mbeanProps = new Hashtable<String, String>(5);
      mbeanProps.put("name", _name);
      mbeanProps.put("type", BootstrapHttpStatsCollector.class.getSimpleName());
      mbeanProps.put("bootstrap", Integer.toString(id));
      jmxName = new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
    }
    catch (Exception e)
    {
      LOG.error("Error creating JMX object name", e);
    }

    _collectorObjName = jmxName;

    registerAsMBeans();


  }

  private DbusBootstrapHttpStats getOrAddPerPeerCollector(String client, Lock writeLock)
  {
    Lock myWriteLock = null;
    if (null == writeLock) myWriteLock = acquireWriteLock();
    try
    {
      DbusBootstrapHttpStats clientStats = _perClientStats.get(client);
      if (null == clientStats)
      {
        clientStats = new DbusBootstrapHttpStats(_id, _perPeerPrefix + client, true, isThreadSafe(), null);
        _perClientStats.put(client, clientStats);

        if (null != _mbeanServer)
        {
          clientStats.registerAsMbean(_mbeanServer);
        }
      }

      return clientStats;
    }
    finally
    {
      releaseLock(myWriteLock);
    }
  }

  public void merge(BootstrapHttpStatsCollector other)
  {
    _totalStats.mergeStats(other._totalStats);

    Lock otherReadLock = other.acquireReadLock();
    Lock writeLock = acquireWriteLock(otherReadLock);
    try
    {

      for (String peerName: other._perClientStats.keySet())
      {
        DbusBootstrapHttpStats bean = other._perClientStats.get(peerName);
        mergePerPeer(peerName, bean, writeLock);
      }
    }
    finally
    {
      releaseLock(writeLock);
      releaseLock(otherReadLock);
    }
  }

  private void mergePerPeer(String peer, DbusBootstrapHttpStats other, Lock writeLock)
  {
    DbusBootstrapHttpStats curBean = getOrAddPerPeerCollector(peer, writeLock);
    curBean.mergeStats(other);
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
        _totalStats.registerAsMbean(_mbeanServer);
        _mbeanServer.registerMBean(this, _collectorObjName);
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
        _mbeanServer.unregisterMBean(_collectorObjName);
        _totalStats.unregisterMbean(_mbeanServer);

        for (String clientName: _perClientStats.keySet())
        {
          _perClientStats.get(clientName).unregisterMbean(_mbeanServer);
        }

        LOG.info("MBean unregistered " + _collectorObjName);
      }
      catch (Exception e)
      {
        LOG.error("JMX deregistration failed", e);
      }
    }
  }

  @Override
  public void reset()
  {
    _totalStats.reset();

    Lock readLock = acquireReadLock();
    try
    {
      for (String peer: _perClientStats.keySet())
      {
        _perClientStats.get(peer).reset();
      }
    }
    finally
    {
      releaseLock(readLock);
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

  @Override
  public DbusBootstrapHttpStatsMBean   getTotalStats()
  {
    return _totalStats;
  }

  @Override
  public DbusBootstrapHttpStatsMBean getPeerStats(String peer)
  {

    Lock readLock = acquireReadLock();
    try
    {
      DbusBootstrapHttpStats result = _perClientStats.get(peer);
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public void registerBootStrapReq(Checkpoint cp, long latency, long size)
  {
    _totalStats.registerBootStrapReq(cp, latency, size);
    //TODO: client views;
    /*
    Lock writeLock = acquireWriteLock();
    try
    {

      if (NO_PEER != _curPeer)
      {
        DbusBootstrapHttpStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerBootStrapReq(cp,latency,size);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }
    */
  }

  @Override
  public void registerStartSCNReq(long latency)
  {
    //TODO: client views;
    _totalStats.registerStartSCNReq(latency);
  }

  @Override
  public void registerTargetSCNReq(long latency)
  {
    //TODO: client views;
    _totalStats.registerTargetSCNReq(latency);
  }

  @Override
  public void registerErrBootstrap()
  {
    //TODO: client views;
    _totalStats.registerErrBootstrap();
  }

  @Override
  public void registerErrStartSCN()
  {
    //TODO: client views;
    _totalStats.registerErrStartSCN();
  }

  @Override
  public void registerErrTargetSCN()
  {
    //TODO: client views;
    _totalStats.registerErrTargetSCN();
  }

  @Override
  public void registerErrSqlException()
  {
    //TODO: client views;
    _totalStats.registerErrSqlException();
  }

  @Override
  public void registerErrDatabaseTooOld()
  {
    //TODO: client views;
    _totalStats.registerErrDatabaseTooOld();
  }



}
