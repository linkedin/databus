package com.linkedin.databus3.espresso.client;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusInvalidRegistrationException;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DeregisterResult;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.FlushRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RegistrationState;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult.SummaryCode;
import com.linkedin.databus.client.pub.StartResult;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.client.pub.monitoring.events.ConsumerCallbackStatsEvent;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.Role;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 *
 * @author pganti
 *
 * A class that describes the registration of a consumer or a group of consumers
 * to a list of sources
 *
 * @param <C>
 */
public class DatabusV3ConsumerRegistration extends DatabusV2ConsumerRegistration
										   implements DatabusV3Registration
{

	protected final RegistrationId _rid;
	private final CheckpointPersistenceProvider _checkpointPersistenceProvider;
	private final String _dbName;
	protected final DatabusHttpV3ClientImpl _client;
	private RegistrationState _state;
	private List<DatabusV2ConsumerRegistration> _regInfo = null;
	protected final Logger _log;
    private DbusEventsStatisticsCollector _inboundEventsStatsCollector;
	private DbusEventsStatisticsCollector _bootstrapEventsStatsCollector;
    private ConsumerCallbackStats _relayConsumerStats;
    private ConsumerCallbackStats _bootstrapConsumerStats;

  private final Status _status; //status before the connection is initialized
	DatabusSourcesConnection _conn;

	protected Lock _lock = null;

	public class Status extends DatabusComponentStatus
	{

      public Status()
      {
        super(getStatusName());
      }

      @Override
      public void start()
      {
        DatabusV3ConsumerRegistration.this.start();
        super.start();
      }

      @Override
      public void shutdown()
      {
        deregister();
        super.shutdown();
      }

      @Override
      public void pause()
      {
        throw new RuntimeException("not implemented");
        //super.pause();
      }

      @Override
      public void resume()
      {
        throw new RuntimeException("not implemented");
        //super.resume();
      }

      @Override
      public void suspendOnError(Throwable error)
      {
        throw new RuntimeException("not implemented");
        //super.suspendOnError(error);
      }

      void setStarted()
      {
        super.start();
      }

	}

    /**
     * Registration for a single consumer to a list of sources
     */
    public DatabusV3ConsumerRegistration(DatabusHttpV3ClientImpl client,
            String dbName,
            DatabusCombinedConsumer consumer,
            RegistrationId rid,
            List<DatabusSubscription> sources,
            DbusKeyCompositeFilterConfig filterConfig,
            CheckpointPersistenceProvider checkpointPersistenceProvider,
            DatabusComponentStatus clientStatus)
    {
      this(client, dbName, consumer, rid, sources, filterConfig, checkpointPersistenceProvider,
           clientStatus, null);
    }

	/**
	 * Registration for a group of consumers to a list of sources
	 */
	public DatabusV3ConsumerRegistration(DatabusHttpV3ClientImpl client,
			String dbName,
			DatabusCombinedConsumer[] consumers,
			RegistrationId rid,
			List<DatabusSubscription> sources,
			DbusKeyCompositeFilterConfig filterConfig,
			CheckpointPersistenceProvider checkpointPersistenceProvider,
			DatabusComponentStatus clientStatus)
	{
	  this(client, dbName, Arrays.asList(consumers), rid, sources, filterConfig,
	       checkpointPersistenceProvider, clientStatus, null);
	}

    /**
     * Registration for a group of consumers to a list of sources
     */
    public DatabusV3ConsumerRegistration(DatabusHttpV3ClientImpl client,
            String dbName,
            DatabusCombinedConsumer[] consumers,
            RegistrationId rid,
            List<DatabusSubscription> sources,
            DbusKeyCompositeFilterConfig filterConfig,
            CheckpointPersistenceProvider checkpointPersistenceProvider,
            DatabusComponentStatus clientStatus,
            Logger log)
    {
      this(client, dbName, Arrays.asList(consumers), rid, sources, filterConfig,
           checkpointPersistenceProvider, clientStatus, null);
    }

    /**
     * Registration for a group of consumers to a list of sources
     */
    public DatabusV3ConsumerRegistration(DatabusHttpV3ClientImpl client,
                                         String dbName, List<DatabusCombinedConsumer> consumers,
                                         RegistrationId rid,
                                         List<DatabusSubscription> sources,
                                         DbusKeyCompositeFilterConfig filterConfig,
                                         CheckpointPersistenceProvider checkpointPersistenceProvider,
                                         DatabusComponentStatus clientStatus)
    {
      this(client, dbName, consumers, rid, sources, filterConfig, checkpointPersistenceProvider,
           clientStatus, null);
    }

    /**
     * Registration for a single consumer to a list of sources
     */
    public DatabusV3ConsumerRegistration(DatabusHttpV3ClientImpl client,
            String dbName,
            DatabusCombinedConsumer consumer,
            RegistrationId rid,
            List<DatabusSubscription> sources,
            DbusKeyCompositeFilterConfig filterConfig,
            CheckpointPersistenceProvider checkpointPersistenceProvider,
            DatabusComponentStatus clientStatus,
            Logger log)
    {
        super(consumer, null, sources, filterConfig);

        _log = null != log ? log : Logger.getLogger(getClass().getName() +
                                                    (null  == rid ? "" : "." + rid.getId()));

        _lock = new ReentrantLock();
        _client = client;
        _dbName = dbName;
        _rid = rid;
        _checkpointPersistenceProvider = checkpointPersistenceProvider;
        _state = RegistrationState.CREATED;
        _status = new Status();
        initializeStatsCollectors();
        verifySubscriptionRoles(sources);
    }

	protected void initializeStatsCollectors()
    {
	  //some safety against null pointers coming from unit tests
	  MBeanServer mbeanServer =
	      null == _client || _client.getClientStaticConfig().isEnablePerConnectionStats() ?
	      _client.getMbeanServer() : null;
	  int ownerId = null == _client ? -1 : _client.getContainerStaticConfig().getId();
	  String regId = null != _rid ? _rid.getId() : "unknownReg";

	  _inboundEventsStatsCollector =
	      new DbusEventsStatisticsCollector(ownerId,
	                                        regId + ".inbound",
	                                        true,
	                                        false,
	                                        mbeanServer);
	  _bootstrapEventsStatsCollector =
	      new DbusEventsStatisticsCollector(ownerId,
	                                        regId + ".inbound.bs",
	                                        true,
	                                        false,
	                                        mbeanServer);
	  _relayConsumerStats =
	      new ConsumerCallbackStats(ownerId, regId + ".callback.relay",
	                                regId, true, false, new ConsumerCallbackStatsEvent());
      _bootstrapConsumerStats =
          new ConsumerCallbackStats(ownerId, regId + ".callback.bootstrap",
                                    regId, true, false, new ConsumerCallbackStatsEvent());
	  if (null != _client && _client.getClientStaticConfig().isEnablePerConnectionStats())
	  {
        _client.getBootstrapEventsStats().addStatsCollector(regId, _bootstrapEventsStatsCollector );
        _client.getInBoundStatsCollectors().addStatsCollector(regId, _inboundEventsStatsCollector);
        _client.getRelayConsumerStatsCollectors().addStatsCollector(regId, _relayConsumerStats);
        _client.getBootstrapConsumerStatsCollectors().addStatsCollector(regId, _bootstrapConsumerStats);
	  }
    }

  /**
	 * Registration for a group of consumers to a list of sources
	 */
	public DatabusV3ConsumerRegistration(DatabusHttpV3ClientImpl client,
			String dbName, List<DatabusCombinedConsumer> consumers,
			RegistrationId rid,
			List<DatabusSubscription> sources,
			DbusKeyCompositeFilterConfig filterConfig,
			CheckpointPersistenceProvider checkpointPersistenceProvider,
			DatabusComponentStatus clientStatus,
			Logger log)
	{
		super(null, consumers, sources, filterConfig);

        _log = null != log ? log : Logger.getLogger(getClass().getName() +
                                                    (null  == rid ? "" : "." + rid.getId()));

		_lock = new ReentrantLock();
		_client = client;
		_dbName = dbName;
		_rid = rid;
		_checkpointPersistenceProvider = checkpointPersistenceProvider;
		_state = RegistrationState.CREATED;
        _status = new Status();
        initializeStatsCollectors();
		verifySubscriptionRoles(sources);
	}

	@Override
	public StartResult start()
	{
		_lock.lock();
		try
		{
			_log.info(_rid.getId() + ": start called");
			StartResult sr = StartResultImpl.createSuccessStartResult();
			if ( _state != RegistrationState.CREATED)
			{
				String errorMessage = "Registration is not in CREATED state but in " + _state.toString();
				sr = StartResultImpl.createFailedStartResult(getId(), errorMessage, null);
				return sr;
			}

		    _log.info("Consumer Registration List is :" + _regInfo);

		    try
		    {
		      _conn = _client.initializeRelayConnection(_rid, _regInfo, getSubscriptions());
		      _client.updateRegistrationConnectionMap(_rid, _conn);
		      if ( ! _client.hasClientStarted())
		      {
		        _log.info("Databus Client service has not been started yet. Starting !!");
		        _client.start();
		      }
		      _state = RegistrationState.STARTED;
    		  sr = StartResultImpl.createSuccessStartResult();
  		    } catch (DatabusClientException e)
  		    {
  		    	String errorMessage = "Error initializing a client";
  		    	sr = StartResultImpl.createFailedStartResult(_rid, errorMessage, e);
  		    	_log.error(errorMessage);
  		    }

		    //if (sr.getSuccess()) _status.setStarted();
		    //else _status.suspendOnError(new DatabusClientException(sr.getErrorMessage(), sr.getException()));

			return sr;
		} finally
		{
			_lock.unlock();
		}
	}

	public void addDatabusConsumerRegInfo(List<DatabusV2ConsumerRegistration> regInfo)
	{
		_regInfo = regInfo;
		return;
	}

	@Override
	public RegistrationState getState()
	{
		if ( null != _rid )
		{
    		_log.debug(_rid.getId() + ": getState called");
		}
		return _state;
	}

	@Override
	public RegistrationId getId()
	{
		if ( null != _rid )
		{
    		_log.debug(_rid.getId() + ": getId called");
		}
		return _rid;
	}

	@Override
	public void addSubscriptions(DatabusSubscription ... subs)
	throws DatabusClientException
	{
		_lock.lock();
		try
		{
			if ( null != _rid )
			{
				_log.info(_rid.getId() + ": addSubscriptions called");
			}
			if ( _state == RegistrationState.DEREGISTERED)
			{
				String errorMessage = "Registration is in DEREGISTERED state, during which subscriptions cannot be added ";
				_log.error(errorMessage);
				return;
			}

			if ( null == subs)
			{
				_log.error("List of subscriptions to add is null");
				return;
			}

			for (DatabusSubscription sub : subs)
			{
				if (! getSubscriptions().contains(sub))
				{
					_log.info("Adding subscription " + sub.toJsonString());
					getSubscriptions().add(sub);
				}
				else
				{
					_log.info("Subscription " + sub.toJsonString() + " already exists. Ignoring");
				}
			}

			/**
			 * TODO :
			 * 1. Checkpoint should be recomputed and the persistent checkpoint should be changed
			 * 2. Should check to see if a new connection needs to be made to be able to connect to a client
			 */
			return;
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public void removeSubscriptions(DatabusSubscription ... subs)
	throws DatabusClientException
	{
		_lock.lock();
		try
		{
			if ( null != _rid )
			{
			    _log.info(_rid.getId() + ": removeSubscriptions called");
			}
			if ( _state == RegistrationState.DEREGISTERED)
			{
				String errorMessage = "Registration is in DEREGISTERED state, during which subscriptions cannot be removed ";
				_log.error(errorMessage);
				return;
			}

			if ( null == subs)
			{
				_log.error("List of subscriptions to remove is null");
				return;
			}

			for (DatabusSubscription sub : subs)
			{
				if (getSubscriptions().contains(sub))
				{
					_log.info("Removing subscription " + sub.toJsonString());
					getSubscriptions().remove(sub);
				}
				else
				{
					_log.info("Subscription " + sub.toJsonString() + " doesn't exist. Ignoring");
				}
			}

			/**
			 * TODO :
			 * 1. Checkpoint should be recomputed and the persistent checkpoint should be changed
			 * 2. Should check to see if an existing connection needs to a client should be terminated
			 */

			return;
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public void addDatabusConsumers(DatabusV3Consumer[] consumers)
	{
		_lock.lock();
		try
		{
			if (null != _rid)
			{
	    		_log.info(_rid.getId() + ": addDatabusConsumers called");
			}
			if ( _state == RegistrationState.DEREGISTERED)
			{
				String errorMessage = "Registration is in DEREGISTERED state, during which consumers cannot be added ";
				_log.error(errorMessage);
				return;
			}

			if ( null == consumers)
			{
				_log.error("List of consumers is null");
				return;
			}

			List<DatabusV3Consumer> lConsumers = Arrays.asList(consumers);
			for (DatabusV3Consumer cons : lConsumers)
			{
				if (! lConsumers.contains(cons))
				{
					_consumers.add(cons);
				}
				else
				{
					_log.info("Consumer already exists. Ignoring");
				}
			}
			return;
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public void removeDatabusConsumers(DatabusV3Consumer[] consumers)
	{
		_lock.lock();
		try
		{
			_log.info(_rid.getId() + ": removeDatabusConsumers called");
			if ( _state == RegistrationState.DEREGISTERED)
			{
				String errorMessage = "Registration is in DEREGISTERED state, during which consumers cannot be removed ";
				_log.error(errorMessage);
				return;
			}

			if ( null == consumers)
			{
				_log.error("List of consumers is null");
				return;
			}

			List<DatabusV3Consumer> lConsumers = Arrays.asList(consumers);
			for (DatabusV3Consumer cons : lConsumers)
			{
				if (lConsumers.contains(cons))
				{
					_consumers.remove(cons);
				}
				else
				{
					_log.info("Consumer does not exist. Ignoring");
				}
			}
			return;
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public DatabusComponentStatus getStatus()
	{
		if ( null != _rid )
		{
			_log.debug(_rid.getId() + ": getStatus called");
		}
		return null != _conn ? _conn.getConnectionStatus() : _status;
	}

	/**
	 * When invoked in DEREGISTERED state, null is returned because this object can be created with null persistence provider
	 */
	@Override
	public CheckpointPersistenceProvider getCheckpoint()
	{
		if ( null != _rid )
		{
			_log.debug(_rid.getId() + ": getCheckpoint called");
		}
		if ( _state == RegistrationState.DEREGISTERED)
		{
			String errorMessage = "Registration is in DEREGISTERED state, during which CheckpointPersistenceProvider cannot be obtained";
			_log.error(errorMessage);
			return null;
		}

		return _checkpointPersistenceProvider;
	}

	@Override
	public String toString() {
		return "DatabusV3ConsumerRegistration [_rid=" + _rid
				+ ", _checkpointPersistenceProvider="
				+ _checkpointPersistenceProvider + ", _clientStatus="
				+ _status + ", _dbName=" + _dbName + ", _consumers="
				+ _consumers + ", _sources=" + _sources + ", _rng=" + _rng
				+ ", _filterConfig=" + _filterConfig + "]";
	}

	@Override
	public String getDBName()
	{
		if (null != _rid)
		{
		     _log.debug(_rid.getId() + ": getDBName called");
		}
		return _dbName;
	}

	@Override
	public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
	throws InterruptedException
	{
	    _lock.lock();
	    try
	    {
			if ( null != _rid)
			{
	     		_log.info(_rid.getId() + ": fetchMaxSCN called");
			}
			if (null == request)
			{
				RelayFindMaxScnResultImpl rfms = new RelayFindMaxScnResultImpl();
				RelayFindMaxSCNResult.SummaryCode sc = RelayFindMaxSCNResult.SummaryCode.FAIL;
				rfms.setResultSummary(sc);
				return rfms;
			}
			return _client.fetchMaxSCN(this, request);
	    } finally
	    {
	    	_lock.unlock();
	    }
	}

	@Override
	public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult,
			FlushRequest flushRequest)
	throws InterruptedException
	{
		_lock.lock();
		try
		{
			if ( null != _rid )
			{
				_log.info(_rid.getId() + ": flush called with RelayFindMaxSCNResult");
			}
			if ( _state != RegistrationState.STARTED)
			{
				String errorMessage = "Registration is not in STARTED state. Current state = " + _state.toString();
				_log.error(errorMessage);
				RelayFlushMaxSCNResult rfm = new RelayFlushMaxSCNResultImpl(SummaryCode.FAIL);
				return rfm;
			}
			else if ( null == fetchSCNResult || null == flushRequest)
			{
				String errorMessage = "Input parameters cannot be null";
				_log.error(errorMessage);
				RelayFlushMaxSCNResult rfm = new RelayFlushMaxSCNResultImpl(SummaryCode.FAIL);
				return rfm;
			}

			return _client.flush(fetchSCNResult, this, flushRequest.getFlushTimeoutMillis());
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest request, FlushRequest flushRequest)
	throws InterruptedException
	{
		_lock.lock();
		try
		{
			if ( null != _rid )
			{
				_log.info(_rid.getId() + ": flush called with FetchMaxSCNRequest");
			}
			if ( _state != RegistrationState.STARTED)
			{
				String errorMessage = "Registration is not in STARTED state. Current state = " + _state.toString();
				_log.error(errorMessage);
				RelayFlushMaxSCNResult rfm = new RelayFlushMaxSCNResultImpl(SummaryCode.FAIL);
				return rfm;
			}
			else if ( null == request || null == flushRequest)
			{
				String errorMessage = "Input parameters cannot be null";
				_log.error(errorMessage);
				RelayFlushMaxSCNResult rfm = new RelayFlushMaxSCNResultImpl(SummaryCode.FAIL);
				return rfm;
			}

			return _client.flush(this, flushRequest.getFlushTimeoutMillis(), request);
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public DeregisterResult deregister()
	{
		_lock.lock();
		try
		{
			if ( null != _rid )
			{
				_log.info(_rid.getId() + ": deregister called");
			}
			if ( _state != RegistrationState.CREATED && _state != RegistrationState.STARTED)
			{
				String errorMessage = "Registration is neither in CREATED nor in STARTED state. Current state = " + _state.toString();
				_log.error(errorMessage);
				DeregisterResult dr = DeregisterResultImpl.createFailedDeregisterResult(_rid, errorMessage, new Exception(errorMessage));
				return dr;
			}
	        _state = RegistrationState.DEREGISTERED;
			return _client.deregister(this);
		} finally
		{
			_lock.unlock();
		}
	}

	@Override
	public boolean equals(Object other)
	{
	  if(other instanceof DatabusV3ConsumerRegistration)
	  {
	    return getId().equals(((DatabusV3ConsumerRegistration)other).getId());
	  }
	  return false;
	}

	@Override
	public int hashCode()
	{
		return getId().hashCode();
	}

	/**
	 * Verify roles for all Subscriptions to be consistent.
	 *
	 *  Allowed Configs:
	 *  1. All subscriptions must have the same roles
	 *
	 */
	private void verifySubscriptionRoles(List<DatabusSubscription> sources)
	{
		//TODO: As part of implementing DDSDBUS-527, we need to revisit this logic

		if ( null == sources)
			return;

		boolean first = true;
		Role role = null;
		for ( DatabusSubscription ds : sources)
		{
			if ( null == ds)
				continue;

			if (first)
			{
				first = false;
				role = ds.getPhysicalSource().getRole();
			} else {
				if ( ! role.equals(ds.getPhysicalSource().getRole()))
					throw new DatabusInvalidRegistrationException(
							"Role (" + role +") conflicts with role (" + ds.getPhysicalSource().getRole() + ") for the registration :" + this);
			}
		}
	}

	protected DatabusHttpV3ClientImpl getClient()
  {
    return _client;
  }

  protected void setState(RegistrationState state)
  {
    _state = state;
  }

  protected RegistrationId getRid()
  {
    return _rid;
  }

  public String getStatusName()
  {
    return "Status_" + _rid.getId();
  }

  @Override
  public Logger getLogger()
  {
    return _log;
  }

  @Override
  public DbusEventsStatisticsCollectorMBean getRelayEventStats()
  {
    return _inboundEventsStatsCollector;
  }

  @Override
  public DbusEventsStatisticsCollectorMBean getBootstrapEventStats()
  {
    return _bootstrapEventsStatsCollector;
  }

  @Override
  public ConsumerCallbackStatsMBean getRelayCallbackStats()
  {
    return _relayConsumerStats;
  }

  @Override
  public ConsumerCallbackStatsMBean getBootstrapCallbackStats()
  {
    return _bootstrapConsumerStats;
  }
}
