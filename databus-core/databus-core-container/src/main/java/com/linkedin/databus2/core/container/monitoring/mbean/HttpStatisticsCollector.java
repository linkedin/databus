package com.linkedin.databus2.core.container.monitoring.mbean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.JmxUtil;
import com.linkedin.databus.core.util.ReadWriteSyncedObject;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;

/**
 * Container for all monitoring mbeans
 * @author cbotev
 */
public class HttpStatisticsCollector extends ReadWriteSyncedObject
                                     implements HttpStatisticsCollectorMBean
{
  public static final String MODULE = HttpStatisticsCollector.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final static String NO_PEER = "NONE";

  private final DbusHttpTotalStats _totalStats;
  private final HashMap<Integer, DbusHttpTotalStats> _perSourceStats;
  private final HashMap<String, DbusHttpTotalStats> _perClientStats;
  private final MBeanServer _mbeanServer;
  private final ObjectName _collectorObjName;

  private final int _id;
  private final String _name;
  private final String _perSourceNamePrefix;
  private final String _perPeerPrefix;
  private final String _curPeer;
  private final AtomicBoolean _enabled;

  public HttpStatisticsCollector(int id, String name, boolean enabled, boolean threadSafe,
                                 MBeanServer mbeanServer)
  {
    this(id, name, enabled, threadSafe, NO_PEER, mbeanServer);
  }

  private HttpStatisticsCollector(int relayId, String name, boolean enabled, boolean threadSafe,
                                  String peer, MBeanServer mbeanServer)
  {
    super(threadSafe);

    _mbeanServer = mbeanServer;
    _curPeer = peer;
    _enabled = new AtomicBoolean(enabled);
    _id = relayId;
    _name = name;
    _perSourceNamePrefix = _name + ".source.";
    _perPeerPrefix = _name + ".peer.";

    _totalStats = new DbusHttpTotalStats(_id, _name + ".total", enabled, threadSafe, null);

    _perSourceStats = new HashMap<Integer, DbusHttpTotalStats>(100);
    _perClientStats = new HashMap<String, DbusHttpTotalStats>(1000);

    ObjectName jmxName = null;
    try
    {
      Hashtable<String, String> mbeanProps = new Hashtable<String, String>(5);
      mbeanProps.put("name", _name);
      mbeanProps.put("type", HttpStatisticsCollector.class.getSimpleName());
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

        for (String clientName: _perClientStats.keySet())
        {
          _perClientStats.get(clientName).unregisterMbean(_mbeanServer);
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
  public HttpStatisticsCollector createForClientConnection(String client)
  {
    return new HttpStatisticsCollector(_id, _name, true, false, client, null);
  }

  @Override
  public DbusHttpTotalStats getTotalStats()
  {
    return _totalStats;
  }

  @Override
  public List<Integer> getSources()
  {
    Lock readLock = acquireReadLock();
    try
    {
      List<Integer> result = new ArrayList<Integer>(_perSourceStats.keySet());
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public DbusHttpTotalStats getSourceStats(int srcId)
  {
    DbusHttpTotalStats result = getOrAddPerSourceCollector(srcId, null);
    return result;
  }

  @Override
  public List<String> getPeers()
  {
    Lock readLock = acquireReadLock();
    try
    {
      ArrayList<String> result = new ArrayList<String>(_perClientStats.keySet());
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public DbusHttpTotalStats getPeerStats(String peer)
  {
    Lock readLock = acquireReadLock();
    try
    {
      DbusHttpTotalStats result = _perClientStats.get(peer);
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public void registerRegisterCall(List<RegisterResponseEntry> sources)
  {
    if (!_enabled.get()) return;

    _totalStats.registerRegisterCall(_curPeer);

    for (RegisterResponseEntry respEntry: sources)
    {
      DbusHttpTotalStats perSourceCollector =
        getOrAddPerSourceCollector((int)respEntry.getId(), null);

      perSourceCollector.registerRegisterCall(_curPeer);
    }

    if (NO_PEER != _curPeer)
    {
      DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, null);
      clientStats.registerRegisterCall(_curPeer);
    }
  }

  @Override
  public void registerSourcesCall()
  {
    if (!_enabled.get()) return;

    _totalStats.registerSourcesCall(_curPeer);

    if (NO_PEER != _curPeer)
    {
      DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, null);
      clientStats.registerSourcesCall(_curPeer);
    }
  }

  @Override
  public void registerStreamResponse(long streamCallDuration)
  {
    if (!_enabled.get()) return;

    Lock writeLock = acquireWriteLock();
    try
    {
      _totalStats.registerStreamResponse(streamCallDuration);

      if (NO_PEER != _curPeer)
      {
        DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerStreamResponse(streamCallDuration);
      }

      //NOTE: we don't update per-source stats because for multi-source stream calls the latency
      //is not meaningful on per-source basis
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  @Override
  public void registerStreamRequest(Checkpoint cp, Collection<Integer> sourceIds)
  {
    if (!_enabled.get()) return;

    _totalStats.registerStreamRequest(_curPeer, cp);

    Lock writeLock = acquireWriteLock();
    try
    {
      for (Integer srcId: sourceIds)
      {
        DbusHttpTotalStats curBean = getOrAddPerSourceCollector(srcId, writeLock);
        curBean.registerStreamRequest(_curPeer, cp);
      }

      if (NO_PEER != _curPeer)
      {
        DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerStreamRequest(_curPeer, cp);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void merge(HttpStatisticsCollector other)
  {
    _totalStats.mergeStats(other._totalStats);

    Lock otherReadLock = other.acquireReadLock();
    Lock writeLock = acquireWriteLock(otherReadLock);
    try
    {
      for (Integer id: other._perSourceStats.keySet())
      {
        DbusHttpTotalStats bean = other._perSourceStats.get(id);
        mergePerSource(id, bean, writeLock);
      }

      for (String peerName: other._perClientStats.keySet())
      {
        DbusHttpTotalStats bean = other._perClientStats.get(peerName);
        mergePerPeer(peerName, bean, writeLock);
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

    Lock readLock = acquireReadLock();
    try
    {
      for (Integer sourceId: _perSourceStats.keySet())
      {
        DbusHttpTotalStats sourceStats = _perSourceStats.get(sourceId);
        sourceStats.reset();
      }

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

  private DbusHttpTotalStats getOrAddPerSourceCollector(Integer srcId, Lock writeLock)
  {
    Lock myWriteLock = null;
    if (null == writeLock) myWriteLock = acquireWriteLock();


    try
    {
      DbusHttpTotalStats data = _perSourceStats.get(srcId);
      if (null == data)
      {
        data = new DbusHttpTotalStats(_id, _perSourceNamePrefix + srcId, true, isThreadSafe(), null);
        _perSourceStats.put(srcId, data);

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

  private DbusHttpTotalStats getOrAddPerPeerCollector(String client, Lock writeLock)
  {
    Lock myWriteLock = null;
    if (null == writeLock) myWriteLock = acquireWriteLock();
    try
    {
      DbusHttpTotalStats clientStats = _perClientStats.get(client);
      if (null == clientStats)
      {
        clientStats = new DbusHttpTotalStats(_id, _perPeerPrefix + client, true, isThreadSafe(), null);
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


  private void mergePerSource(Integer srcId, DbusHttpTotalStats other, Lock writeLock)
  {
    DbusHttpTotalStats curBean = getOrAddPerSourceCollector(srcId, writeLock);
    curBean.mergeStats(other);
  }

  private void mergePerPeer(String peer, DbusHttpTotalStats other, Lock writeLock)
  {
    DbusHttpTotalStats curBean = getOrAddPerPeerCollector(peer, writeLock);
    curBean.mergeStats(other);
  }


  public class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final boolean _enabled;

    public RuntimeConfig(boolean enabled)
    {
      _enabled = enabled;
    }

    /** A flag if the statistics collector is enabled and it will update the stats counters  */
    public boolean isEnabled()
    {
      return _enabled;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      HttpStatisticsCollector.this.setEnabled(this.isEnabled());
    }

    @Override
    public boolean equals(Object other)
    {
      if (null == other || !(other instanceof RuntimeConfig)) return false;
      return equalsConfig((RuntimeConfig)other);
    }

    @Override
    public boolean equalsConfig(RuntimeConfig otherConfig)
    {
      if (null == otherConfig) return false;
      return isEnabled() == otherConfig.isEnabled();
    }

    @Override
    public int hashCode()
    {
      return _enabled ? 0 : 1;
    }
  }

  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    private boolean _enabled = true;
    private HttpStatisticsCollector _managedInstance = null;

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

    public HttpStatisticsCollector getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(HttpStatisticsCollector managedInstance)
    {
      _managedInstance = managedInstance;
    }

  }

  public static class StaticConfig
  {
    private final RuntimeConfigBuilder _runtime;
    private final HttpStatisticsCollector _existingStatsCollector;

    public StaticConfig(RuntimeConfigBuilder runtime, HttpStatisticsCollector existingStatsCollector)
    {
      _runtime = runtime;
      _existingStatsCollector = existingStatsCollector;
    }

    /** Runtime configuration */
    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public HttpStatisticsCollector getExistingStatsCollector()
    {
      return _existingStatsCollector;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private final RuntimeConfigBuilder _runtime;
    private HttpStatisticsCollector _existingStatsCollector = null;

    public Config()
    {
      super();
      _runtime = new RuntimeConfigBuilder();
    }

    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public HttpStatisticsCollector getExisting()
    {
      return _existingStatsCollector;
    }

    public void setExisting(HttpStatisticsCollector existingStatsCollector)
    {
      _existingStatsCollector = existingStatsCollector;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      return new StaticConfig(_runtime, _existingStatsCollector);
    }

  }

  @Override
  public String getName()
  {
    return _name;
  }

  @Override
  public void registerInvalidStreamRequest()
  {
    if (!_enabled.get()) return;

    _totalStats.registerInvalidStreamRequest(_curPeer);

    Lock writeLock = acquireWriteLock();
    try
    {

      if (NO_PEER != _curPeer)
      {
        DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerInvalidStreamRequest(_curPeer);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public void registerScnNotFoundStreamResponse()
  {
    if (!_enabled.get()) return;

    _totalStats.registerScnNotFoundStreamResponse(_curPeer);

    Lock writeLock = acquireWriteLock();
    try
    {

      if (NO_PEER != _curPeer)
      {
        DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerScnNotFoundStreamResponse(_curPeer);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public void registerInvalidSourceRequest()
  {
    if (!_enabled.get()) return;

    _totalStats.registerInvalidSourceRequest(_curPeer);

    Lock writeLock = acquireWriteLock();
    try
    {

      if (NO_PEER != _curPeer)
      {
        DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerInvalidSourceRequest(_curPeer);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public void registerInvalidRegisterCall()
  {
    if (!_enabled.get()) return;

    _totalStats.registerInvalidRegisterCall(_curPeer);

    Lock writeLock = acquireWriteLock();
    try
    {

      if (NO_PEER != _curPeer)
      {
        DbusHttpTotalStats clientStats = getOrAddPerPeerCollector(_curPeer, writeLock);
        clientStats.registerInvalidRegisterCall(_curPeer);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }

  }

  @Override
  public void registerMastershipStatus(int i)
  {
    //only global stats make sense;
    _totalStats.registerMastershipStatus(i);
  }

}

