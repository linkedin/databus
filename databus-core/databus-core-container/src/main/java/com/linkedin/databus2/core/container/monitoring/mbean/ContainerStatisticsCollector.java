package com.linkedin.databus2.core.container.monitoring.mbean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.JmxUtil;
import com.linkedin.databus.core.util.ReadWriteSyncedObject;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Container for all monitoring mbeans
 * @author cbotev
 */
public class ContainerStatisticsCollector extends ReadWriteSyncedObject
                                          implements ContainerStatisticsCollectorMBean
{
  public static final String MODULE = ContainerStatisticsCollector.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String NO_PEER = "none";

  private final ContainerTrafficTotalStats _outboundTrafficTotalStatsMBean;
  private final ContainerTrafficTotalStats _inboundTrafficTotalStatsMBean;
  private final HashMap<String, ContainerTrafficTotalStats> _outboundTrafficPerClientStats;
  private final MBeanServer _mbeanServer;
  private final ServerContainer _serverContainer;
  private final ContainerStats _containerStats;

  private final int _containerId;
  private final String _curClient;
  private final String _name;
  private final String _perPeerNamePrefix;
  private final ObjectName _collectorObjName;
  private AtomicBoolean _enabled;

  public ContainerStatisticsCollector(ServerContainer serverContainer, String name, boolean enabled,
                                      boolean threadSafe,
                                      MBeanServer mbeanServer)
  {
    this(serverContainer, name, enabled, threadSafe, NO_PEER, mbeanServer);
  }

  private ContainerStatisticsCollector(ServerContainer serverContainer, String name, boolean enabled,
                                       boolean threadSafe,
                                       String client, MBeanServer mbeanServer)
  {
    super(threadSafe);

    _serverContainer = serverContainer;
    _mbeanServer = mbeanServer;
    _curClient = client;
    _name = name;
    _perPeerNamePrefix = _name + ".peer.";
    _enabled = new AtomicBoolean(enabled);
    _containerId = serverContainer.getContainerStaticConfig().getId();
    _outboundTrafficTotalStatsMBean =
        new ContainerTrafficTotalStats(_containerId, "outbound", enabled, threadSafe, null);
    _inboundTrafficTotalStatsMBean =
        new ContainerTrafficTotalStats(_containerId, "inbound", enabled, threadSafe, null);
    ExecutorService ioService = serverContainer.getIoExecutorService();
    _containerStats = new ContainerStats(_containerId, enabled, threadSafe, null,
                                         (ioService instanceof ThreadPoolExecutor) ? (ThreadPoolExecutor)ioService: null,
                                         serverContainer.getDefaultExecutorService());
    _outboundTrafficPerClientStats = new HashMap<String, ContainerTrafficTotalStats>(1000);

    Hashtable<String, String>  mbeanProps = new Hashtable<String, String>(5);
    mbeanProps.put("type", ContainerStatisticsCollector.class.getSimpleName());
    mbeanProps.put("name", _name);
    mbeanProps.put("container", Integer.toString(_containerId));

    ObjectName jmxName = null;
    try
    {
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
        _outboundTrafficTotalStatsMBean.registerAsMbean(_mbeanServer);
        _inboundTrafficTotalStatsMBean.registerAsMbean(_mbeanServer);
        _containerStats.registerAsMbean(_mbeanServer);
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
        _outboundTrafficTotalStatsMBean.unregisterMbean(_mbeanServer);
        _inboundTrafficTotalStatsMBean.unregisterMbean(_mbeanServer);
        _containerStats.unregisterMbean(_mbeanServer);

        for (String clientName: _outboundTrafficPerClientStats.keySet())
        {
          _outboundTrafficPerClientStats.get(clientName).unregisterMbean(_mbeanServer);
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
  public ContainerStatisticsCollector createForClientConnection(String client)
  {
    return new ContainerStatisticsCollector(_serverContainer, client, true, false, client, null);
  }

  @Override
  public ContainerTrafficTotalStatsMBean getOutboundTrafficTotalStats()
  {
    return _outboundTrafficTotalStatsMBean;
  }

  @Override
  public ContainerTrafficTotalStatsMBean getInboundTrafficTotalStats()
  {
    return _inboundTrafficTotalStatsMBean;
  }

  @Override
  public List<String> getOutboundClients()
  {
    Lock readLock = acquireReadLock();
    try
    {
      ArrayList<String> result = new ArrayList<String>(_outboundTrafficPerClientStats.keySet());
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public ContainerTrafficTotalStats getOutboundClientStats(String client)
  {
    Lock readLock = acquireReadLock();
    try
    {
      ContainerTrafficTotalStats result = _outboundTrafficPerClientStats.get(client);
      return result;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  public ContainerStats getContainerStats()
  {
    return _containerStats;
  }

  /**
   * Registers a closing of an HTTP connection from a client
   */
  public void registerOutboundConnectionClose()
  {
    if (!_enabled.get()) return;

    _outboundTrafficTotalStatsMBean.registerConnectionClose();

    if (NO_PEER != _curClient)
    {
      ContainerTrafficTotalStats clientStats =
          getOrAddPerClientOutboundCollector(_curClient, null);
      clientStats.registerConnectionClose();
    }
  }

  /**
   * Registers a closing of an HTTP connection originating from the container
   */
  public void registerInboundConnectionClose()
  {
    if (!_enabled.get()) return;

    _inboundTrafficTotalStatsMBean.registerConnectionClose();

    /* FIXME add inbound client handling
    if (NO_PEER != _curClient)
    {
      OutboundTrafficPerClientContainerStats clientStats =
          getOrAddPerClientOutboundCollector(_curClient, null);
      clientStats.registerConnectionClose(connTimeMs);
    }
    */
  }

  public void registerInboundConnectionError(Throwable error)
  {
    if (!_enabled.get()) return;

    _inboundTrafficTotalStatsMBean.registerConnectionError(error);

    //TODO add per-client stats collection (DDSDBUS-103)
  }

  public void registerOutboundConnectionError(Throwable error)
  {
    if (!_enabled.get()) return;

    _outboundTrafficTotalStatsMBean.registerConnectionError(error);

    //TODO add per-client stats collection (DDSDBUS-103)
  }

  /**
   * Registers a new HTTP connection from a client
   */
  public void registerOutboundConnectionOpen()
  {
    if (!_enabled.get()) return;

    _outboundTrafficTotalStatsMBean.registerConnectionOpen(_curClient);

    Lock writeLock = acquireWriteLock();
    try
    {
      if (NO_PEER != _curClient)
      {
        ContainerTrafficTotalStats clientStats = getOrAddPerClientOutboundCollector(_curClient, writeLock);
        clientStats.registerConnectionOpen(_curClient);
      }
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  /**
   * Registers a new HTTP connection originating from the container
   */
  public void registerInboundConnectionOpen()
  {
    if (!_enabled.get()) return;

    _inboundTrafficTotalStatsMBean.registerConnectionOpen(_curClient);

    Lock writeLock = acquireWriteLock();
    try
    {
      /* FIXME add per-client handling
      if (NO_PEER != _curClient)
      {
        OutboundTrafficPerClientContainerStats clientStats =
            getOrAddPerClientOutboundCollector(_curClient, writeLock);
        clientStats.registerConnectionOpen();
      }
      */
    }
    finally
    {
      releaseLock(writeLock);
    }
  }

  public void registerContainerError(Throwable error)
  {
    if (!_enabled.get()) return;

    _containerStats.registerError(error);
  }

  public void registerInboundNetworkError(Throwable error)
  {
    if (!_enabled.get()) return;

    _inboundTrafficTotalStatsMBean.registerConnectionError(error);
  }

  public void registerOutboundNetworkError(Throwable error)
  {
    if (!_enabled.get()) return;

    _outboundTrafficTotalStatsMBean.registerConnectionError(error);
  }

  /**
   * Registers the data about a response size for a connection originating from a remote client
   * @param bytesSent       the number of bytes sent
   */
  public void addOutboundResponseSize(int bytesSent)
  {
    if (!_enabled.get()) return;

    _outboundTrafficTotalStatsMBean.addResponseSize(bytesSent);

    if (NO_PEER != _curClient)
    {
      ContainerTrafficTotalStats clientStats = getOrAddPerClientOutboundCollector(_curClient, null);
      clientStats.addResponseSize(bytesSent);
    }
  }

  /**
   * Registers the data about a response size for a connection originating from the container
   * @param bytesSent       the number of bytes sent
   */
  public void addInboundResponseSize(int bytesSent)
  {
    if (!_enabled.get()) return;

    _inboundTrafficTotalStatsMBean.addResponseSize(bytesSent);

    /* FIXME add per-client handling
    if (NO_PEER != _curClient)
    {
      OutboundTrafficPerClientContainerStats clientStats =
          getOrAddPerClientOutboundCollector(_curClient, null);
      clientStats.addResponseSize(bytesSent);
    }
    */
  }

  public void merge(ContainerStatisticsCollector other)
  {
    _outboundTrafficTotalStatsMBean.mergeStats(other._outboundTrafficTotalStatsMBean);
    _inboundTrafficTotalStatsMBean.mergeStats(other._inboundTrafficTotalStatsMBean);
    _containerStats.mergeStats(other._containerStats);

    Lock otherReadLock = other.acquireReadLock();
    Lock writeLock = acquireWriteLock(otherReadLock);
    try
    {

      for (String clientName: other._outboundTrafficPerClientStats.keySet())
      {
        ContainerTrafficTotalStats bean = other._outboundTrafficPerClientStats.get(clientName);
        mergeOutboundTrafficPerClient(clientName, bean, writeLock);
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
    _outboundTrafficTotalStatsMBean.reset();
    _inboundTrafficTotalStatsMBean.reset();
    _containerStats.reset();

    Lock readLock = acquireReadLock();
    try
    {

      for (String client: _outboundTrafficPerClientStats.keySet())
      {
        _outboundTrafficPerClientStats.get(client).reset();
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

  private ContainerTrafficTotalStats getOrAddPerClientOutboundCollector(
      String client, Lock writeLock)
  {
    Lock myWriteLock = null;
    if (null == writeLock) myWriteLock = acquireWriteLock();
    try
    {
      ContainerTrafficTotalStats clientStats = _outboundTrafficPerClientStats.get(client);
      if (null == clientStats)
      {
        clientStats = new ContainerTrafficTotalStats(_containerId, _perPeerNamePrefix + client,
                                                     true, isThreadSafe(), null);
        _outboundTrafficPerClientStats.put(client, clientStats);

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


  private void mergeOutboundTrafficPerClient(String client, ContainerTrafficTotalStats other,
                                             Lock writeLock)
  {
    ContainerTrafficTotalStats curBean = getOrAddPerClientOutboundCollector(client, writeLock);
    curBean.mergeStats(other);
  }

  public class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final boolean _enabled;

    public RuntimeConfig(boolean enabled)
    {
      _enabled = enabled;
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      ContainerStatisticsCollector.this.setEnabled(this.isEnabled());
    }

    @Override
    public boolean equals(Object other)
    {
      if (null == other || ! (other instanceof RuntimeConfig)) return false;
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
      return (_enabled ? 0xFFFFFFFF : 0);
    }
  }

  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    private boolean _enabled = true;
    private ContainerStatisticsCollector _managedInstance = null;

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

    public ContainerStatisticsCollector getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(ContainerStatisticsCollector managedInstance)
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

    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private RuntimeConfigBuilder _runtime;

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

}

