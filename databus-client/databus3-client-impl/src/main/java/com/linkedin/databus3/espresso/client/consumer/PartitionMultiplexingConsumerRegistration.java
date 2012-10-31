package com.linkedin.databus3.espresso.client.consumer;

import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DatabusV3MultiPartitionRegistration;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DeregisterResult;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.FlushRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RegistrationState;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.StartResult;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.client.pub.monitoring.events.ConsumerCallbackStatsEvent;
import com.linkedin.databus.core.CompoundDatabusComponentStatus;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus3.espresso.client.DatabusHttpV3ClientImpl;
import com.linkedin.databus3.espresso.client.DatabusV3ConsumerRegistration;
import com.linkedin.databus3.espresso.client.DeregisterResultImpl;
import com.linkedin.databus3.espresso.client.StartResultImpl;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumer.PartitionConsumer;

public class PartitionMultiplexingConsumerRegistration
       extends DatabusV3ConsumerRegistration
       implements DatabusV3MultiPartitionRegistration
{
  private final PartitionMultiplexingConsumer _parent;
  private final DbusKeyCompositeFilterConfig _filterConfig;
  private final Map<PhysicalPartition, RegistrationId> _partionRegIds;
  private final Map<PhysicalPartition, DatabusV3Registration> _partionRegs =
      new HashMap<PhysicalPartition, DatabusV3Registration>();
  private final Map<PhysicalPartition, DatabusV3Registration> _partionRegsRO =
      Collections.unmodifiableMap(_partionRegs);
  private final StatusImpl _status;
  /** Aggregator for relay callback statistics across all partitions */
  private StatsCollectors<ConsumerCallbackStats> _relayCallbackStatsMerger;
  /** Aggregator for bootstrap callback statistics across all partitions */
  private StatsCollectors<ConsumerCallbackStats> _bootstrapCallbackStatsMerger;
  /** Aggregator for relay event statistics across all partitions */
  private StatsCollectors<DbusEventsStatisticsCollector> _relayEventStatsMerger;
  /** Aggregator for bootstrap event statistics across all partitions */
  private StatsCollectors<DbusEventsStatisticsCollector> _bootstrapEventStatsMerger;

  protected class StatusImpl extends CompoundDatabusComponentStatus
  {

    public StatusImpl(String componentName, BackoffTimerStaticConfig errorRetriesConf)
    {
      super(componentName, errorRetriesConf, enumerateComponentStatuses());
    }

    public StatusImpl(String componentName,
                      Status status,
                      String detailedMessage,
                      BackoffTimer errorRetriesCounter)
    {
      super(componentName, status, detailedMessage, errorRetriesCounter, enumerateComponentStatuses());
    }

    public StatusImpl(String componentName,
                      Status status,
                      String detailedMessage,
                      BackoffTimerStaticConfig errorRetriesConf)
    {
      super(componentName, status, detailedMessage, errorRetriesConf, enumerateComponentStatuses());
    }

    public StatusImpl(String componentName, Status status, String detailedMessage)
    {
      super(componentName, status, detailedMessage, enumerateComponentStatuses());
    }

    public StatusImpl(String componentName)
    {
      super(componentName, enumerateComponentStatuses());
    }

  }

  public PartitionMultiplexingConsumerRegistration(DatabusHttpV3ClientImpl client,
                                                   String dbName,
                                                   DbusKeyCompositeFilterConfig filterConfig,
                                                   PartitionMultiplexingConsumer parent,
                                                   RegistrationId parentRegId,
                                                   Logger log)
        throws DatabusClientException
  {
    super(client, dbName, parent, parentRegId, parent.getAllSubs(), filterConfig,
          client.getCheckpointPersistenceProvider(), client.getComponentStatus(),
          log);
    assert null != parentRegId;

    _parent = parent;
    if (null != getLogger()) _parent.setLog(getLogger());
    _filterConfig = filterConfig;
    _partionRegIds = new HashMap<PhysicalPartition, RegistrationId>(_parent.getConsumers().size());
    registerPartitionConsumers();
    _status = new StatusImpl("registration_" + parentRegId);
  }

  void registerPartitionConsumers() throws DatabusClientException
  {
    for (Map.Entry<PhysicalPartition, PartitionConsumer> entry: _parent.getConsumers().entrySet())
    {
      RegistrationId consRegId = getPartitionRegistrationId(getRid(), entry.getKey());
      List<DatabusSubscription> consSubList = _parent.getPartitionSubs(entry.getKey());
      DatabusSubscription[] consSubs = new DatabusSubscription[consSubList.size()];
      consSubList.toArray(consSubs);
      DatabusV3Registration reg =
          getClient().registerDatabusListener(entry.getValue(), consRegId, _filterConfig, consSubs);
      _partionRegs.put(entry.getKey(), reg);
      if (null != reg.getLogger()) entry.getValue().setLog(reg.getLogger());
      if (null != _client && !_client.getClientStaticConfig().isEnablePerConnectionStats())
      {
        _relayEventStatsMerger.addStatsCollector(reg.getId().getId(),
                                                 (DbusEventsStatisticsCollector)reg.getRelayEventStats());
        _bootstrapEventStatsMerger.addStatsCollector(reg.getId().getId(),
                                                 (DbusEventsStatisticsCollector)reg.getBootstrapEventStats());
        _relayCallbackStatsMerger.addStatsCollector(reg.getId().getId(),
                                                 (ConsumerCallbackStats)reg.getRelayCallbackStats());
        _bootstrapCallbackStatsMerger.addStatsCollector(reg.getId().getId(),
                                                 (ConsumerCallbackStats)reg.getBootstrapCallbackStats());
      }
    }
  }

  List<DatabusComponentStatus> enumerateComponentStatuses()
  {
    ArrayList<DatabusComponentStatus> statuses =
        new ArrayList<DatabusComponentStatus>(_partionRegs.size());
    for (Map.Entry<PhysicalPartition, DatabusV3Registration> entry: _partionRegs.entrySet())
    {
      statuses.add(entry.getValue().getStatus());
    }
    return statuses;
  }

  public static RegistrationId getPartitionRegistrationId(RegistrationId parentRegId, PhysicalPartition pp)
  {
    return new RegistrationId(parentRegId.getId() + "_" + pp.getName() + "_" + pp.getId());
  }

  @Override
  public StartResult start()
  {
    _lock.lock();
    try
    {
      if (!getState().equals(RegistrationState.CREATED))
        return StartResultImpl.createFailedStartResult(getRid(),
                                                       "invalid registration state: " + getState(),
                                                       null);
      _log.info("starting");

      boolean success = true;
      List<StartResult> failedResults = new ArrayList<StartResult>(_partionRegIds.size());
      for (Map.Entry<PhysicalPartition, DatabusV3Registration> entry: _partionRegs.entrySet())
      {

        StartResult res = null;
        try
        {
            res = entry.getValue().start();
        }
        catch (RuntimeException e)
        {
          PhysicalPartitionConsumptionException ppce =
              new PhysicalPartitionConsumptionException(entry.getKey(), e);
          res = StartResultImpl.createFailedStartResult(entry.getValue().getId(), ppce.getMessage(),
                                                        ppce);
        }
        if (!res.getSuccess())
        {
          success = false;
          failedResults.add(res);
        }
        else
        {
          _relayCallbackStatsMerger.addStatsCollector(
              entry.getValue().getId().getId(),
              (ConsumerCallbackStats)entry.getValue().getRelayCallbackStats());
          _bootstrapCallbackStatsMerger.addStatsCollector(
               entry.getValue().getId().getId(),
               (ConsumerCallbackStats)entry.getValue().getBootstrapCallbackStats());
          _relayEventStatsMerger.addStatsCollector(
               entry.getValue().getId().getId(),
               (DbusEventsStatisticsCollector)entry.getValue().getRelayEventStats());
          _bootstrapEventStatsMerger.addStatsCollector(
               entry.getValue().getId().getId(),
               (DbusEventsStatisticsCollector)entry.getValue().getBootstrapEventStats());
        }
      }

      if (success)
      {
        _log.info("start succeeded");
        setState(RegistrationState.STARTED);
        return StartResultImpl.createSuccessStartResult();
      }
      else
      {
        _log.info("start failed: " + failedResults);
        return StartResultImpl.createFailedStartResult(getRid(), failedResults.toString(), null);
      }
    }
    finally
    {
      _lock.unlock();
    }
  }

  @Override
  public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request) throws InterruptedException
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult,
                                      FlushRequest flushRequest) throws InterruptedException
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest,
                                      FlushRequest flushRequest) throws InterruptedException
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public DeregisterResult deregister()
  {
    _lock.lock();
    try
    {
      if (!getState().equals(RegistrationState.CREATED) && !getState().equals(RegistrationState.STARTED))
        return DeregisterResultImpl.createFailedDeregisterResult(
            getRid(),  "unable to deregister in state: " + getState(),  null);

      _log.info("deregistering");

      boolean success = true;
      boolean connShutdown = true;
      List<DeregisterResult> failedResults = new ArrayList<DeregisterResult>(_partionRegIds.size());
      for (Map.Entry<PhysicalPartition, DatabusV3Registration> entry: _partionRegs.entrySet())
      {

        DeregisterResult res = null;
        try
        {
            res = entry.getValue().deregister();
        }
        catch (RuntimeException e)
        {
          PhysicalPartitionConsumptionException ppce =
              new PhysicalPartitionConsumptionException(entry.getKey(), e);
          res = DeregisterResultImpl.createFailedDeregisterResult(entry.getValue().getId(),
                                                                  ppce.getMessage(),
                                                                  ppce);
        }
        if (!res.isSuccess())
        {
          success = false;
          failedResults.add(res);
        }
        connShutdown = connShutdown && res.isConnectionShutdown();
      }

      if (success)
      {
        _log.info("deregister succeeded");
        setState(RegistrationState.DEREGISTERED);
        return DeregisterResultImpl.createSuccessDeregisterResult(connShutdown);
      }
      else
      {
        _log.info("deregister failed: " + failedResults);
        return DeregisterResultImpl.createFailedDeregisterResult(getRid(),
                                                                 failedResults.toString(),
                                                                 null);
      }
    }
    finally
    {
      _lock.unlock();
    }
  }

  @Override
  public void addSubscriptions(DatabusSubscription... subs) throws DatabusClientException
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public void removeSubscriptions(DatabusSubscription... subs) throws DatabusClientException
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public void addDatabusConsumers(DatabusV3Consumer[] consumers)
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public void removeDatabusConsumers(DatabusV3Consumer[] consumers)
  {
    throw new RuntimeException("not supported yet");
  }

  @Override
  public DatabusComponentStatus getStatus()
  {
    _lock.lock();
    try
    {
      return _status;
    }
    finally
    {
      _lock.unlock();
    }
  }

  @Override
  public CheckpointPersistenceProvider getCheckpoint()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<PhysicalPartition, DatabusV3Registration> getPartionRegs()
  {
    return _partionRegsRO;
  }

  @Override
  public DbusEventsStatisticsCollectorMBean getRelayEventStats()
  {
    return _relayEventStatsMerger.getStatsCollector();
  }

  @Override
  public DbusEventsStatisticsCollectorMBean getBootstrapEventStats()
  {
    return _bootstrapEventStatsMerger.getStatsCollector();
  }

  @Override
  public ConsumerCallbackStatsMBean getRelayCallbackStats()
  {
    return _relayCallbackStatsMerger.getStatsCollector();
  }

  @Override
  public ConsumerCallbackStatsMBean getBootstrapCallbackStats()
  {
    return _relayCallbackStatsMerger.getStatsCollector();
  }

  public PartitionMultiplexingConsumer getParent()
  {
    return _parent;
  }

  @Override
  protected void initializeStatsCollectors()
  {
    //some safety against null pointers coming from unit tests
    MBeanServer mbeanServer =
        null == _client || _client.getClientStaticConfig().isEnablePerConnectionStats() ?
        _client.getMbeanServer() : null;
    int ownerId = null == _client ? -1 : _client.getContainerStaticConfig().getId();
    String regId = null != _rid ? _rid.getId() : "unknownReg";

    ConsumerCallbackStats relayConsumerStats =
        new ConsumerCallbackStats(ownerId, regId + ".callback.relay",
                                  regId, true, false, new ConsumerCallbackStatsEvent());
    ConsumerCallbackStats bootstrapConsumerStats =
        new ConsumerCallbackStats(ownerId, regId + ".callback.bootstrap",
                                  regId, true, false, new ConsumerCallbackStatsEvent());
    _relayCallbackStatsMerger = new StatsCollectors<ConsumerCallbackStats>(relayConsumerStats);
    _bootstrapCallbackStatsMerger = new StatsCollectors<ConsumerCallbackStats>(bootstrapConsumerStats);
    _relayEventStatsMerger = new StatsCollectors<DbusEventsStatisticsCollector>(
        new AggregatedDbusEventsStatisticsCollector(ownerId, regId + ".inbound", true, false, mbeanServer));
    _bootstrapEventStatsMerger = new StatsCollectors<DbusEventsStatisticsCollector>(
        new AggregatedDbusEventsStatisticsCollector(ownerId, regId + ".inbound.bs", true, false, mbeanServer));
    if (null != _client)
    {
      _client.getBootstrapEventsStats().addStatsCollector(
          regId, _bootstrapEventStatsMerger.getStatsCollector() );
      _client.getInBoundStatsCollectors().addStatsCollector(
          regId, _relayEventStatsMerger.getStatsCollector());
      _client.getRelayConsumerStatsCollectors().addStatsCollector(
          regId, _relayCallbackStatsMerger.getStatsCollector());
      _client.getBootstrapConsumerStatsCollectors().addStatsCollector(
           regId, _bootstrapCallbackStatsMerger.getStatsCollector());
      _client.getGlobalStatsMerger().registerStatsCollector(_relayEventStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_bootstrapEventStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_relayCallbackStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_bootstrapCallbackStatsMerger);
    }
  }

}
