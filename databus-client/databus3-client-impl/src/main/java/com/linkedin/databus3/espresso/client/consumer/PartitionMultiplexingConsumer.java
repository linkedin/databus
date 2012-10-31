package com.linkedin.databus3.espresso.client.consumer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.consumer.DelegatingDatabusCombinedConsumer;
import com.linkedin.databus.client.data_model.MultiPartitionSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.NamedThreadFactory;

public class PartitionMultiplexingConsumer extends DelegatingDatabusCombinedConsumer
                                           implements DatabusV3Consumer
{
  /** Used for scheduling */
  private final ScheduledExecutorService _timeoutScheduler;

  final RegistrationId _regId;

  /** The ticket period in milliseconds*/
  final long _ticketExpiryPeriodMs;

  /**
   * A flag if EOW should release the lock or we should wait for the onCheckpoint callback.
   * In general, every onEndDataEventSequence is followed by a onCheckpoint so it is good to hold
   * the lock until then so that the checkpoint can be saved atomically with the window. OTOH,
   * if there is no checkpoint persistence provider, onCheckpoint will never be called. So this
   * behavior has to be configurable.
   * */
  final boolean _eowReleasesLock;

  /** All subscriptions across all partitions */
  final List<DatabusSubscription> _allSubs;

  /** The per-partition consumers */
  final Map<PhysicalPartition, List<DatabusSubscription>> _subs =
      new HashMap<PhysicalPartition, List<DatabusSubscription>>();
  final Map<PhysicalPartition, PartitionConsumer> _consumers =
      new HashMap<PhysicalPartition, PartitionConsumer>();
  final Map<PhysicalPartition, PartitionConsumer> _consumersRO =
      Collections.unmodifiableMap(_consumers);

  /** A flag to ensure that only one startConsumption call is made */
  boolean _startConsumptionCalled = false;

  /** Partitions for which stopConsumption callbacks have been received so far. Used to ensure
   * that only the last one is fired. */
  final Set<PartitionConsumer> _stopConsumptionCalls;

  /** A flag if bootstrap is enabled */
  final boolean _bootstrapEnabled;

  /** Multi-partition SCN to track progress in each partition */
  final MultiPartitionSCN _curScn;

  /** The latest assigned ticket */
  PartitionTicket _currentTicket;

  /** A lock to control the access to the tickets */
  Lock _ticketLock = new ReentrantLock(true);

  /** A condition when the current ticket is done: expired or released */
  Condition _ticketDone = _ticketLock.newCondition();

  /** the number of physical partitions tracked */
  int _partitionNum;

  /** Expect the checkpoint after an EOW */
  boolean _expectEOWCheckpoint;

  public PartitionMultiplexingConsumer(RegistrationId regId,
                                       DatabusCombinedConsumer delegate, Logger log,
                                       ScheduledExecutorService timeoutScheduler,
                                       long maxPartitionTimeMs, boolean bootstrapEnabled,
                                       boolean eowReleasesLock,
                                       DatabusSubscription... subs)
                                       throws DatabusClientException
  {
    this(regId, delegate, log, timeoutScheduler, maxPartitionTimeMs, bootstrapEnabled,
         eowReleasesLock, Arrays.asList(subs));
  }

  public PartitionMultiplexingConsumer(RegistrationId regId,
                                       DatabusCombinedConsumer delegate, Logger log,
                                       ScheduledExecutorService timeoutScheduler,
                                       long maxPartitionTimeMs, boolean bootstrapEnabled,
                                       boolean eowReleasesLock,
                                       List<DatabusSubscription> subs)
                                       throws DatabusClientException
  {
    super(delegate, log);
    assert null != regId;

    _regId = regId;
    _timeoutScheduler = (null != timeoutScheduler) ?
        timeoutScheduler :
        new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ppMuxTimeoutHandler", true));
    _ticketExpiryPeriodMs = maxPartitionTimeMs;
    _stopConsumptionCalls = new HashSet<PartitionMultiplexingConsumer.PartitionConsumer>();
    _bootstrapEnabled = bootstrapEnabled;
    _partitionNum = 0;
    _eowReleasesLock = eowReleasesLock;
    _allSubs = subs;
    processSubs(subs);
    _curScn = new MultiPartitionSCN(_partitionNum);
  }

  private void processSubs(Collection<DatabusSubscription> subs) throws DatabusClientException
  {
    if (null == subs) return;
    String dbName = null;
    for (DatabusSubscription sub: subs)
    {
      if (sub.getPhysicalPartition().isWildcard())
        throw new DatabusClientException("internal error: unexpected partition wildcard");
      if (null == dbName)
      {
        dbName = sub.getPhysicalPartition().getName();
      }
      else if (! dbName.equals(sub.getPhysicalPartition().getName()))
        throw new DatabusClientException("subscriptions across databases are not supported");
      createPartitionConsumer(sub);
    }
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    assertTicket();

    if (_startConsumptionCalled) return ConsumerCallbackResult.SUCCESS;
    try
    {
      return super.onStartConsumption();
    }
    finally
    {
      if (_log.isDebugEnabled()) _log.debug("ignoring startConsumption for " + _currentTicket.getOwner());
      _startConsumptionCalled = true;
    }
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    assertTicket();

    _stopConsumptionCalls.add(_currentTicket.getOwner());
    if (_stopConsumptionCalls.size() == _subs.size())
    {
      return super.onStopConsumption();
    }
    else
    {
      if (_log.isDebugEnabled())
      {
        _log.debug("ignoring stopConsumption for " + _currentTicket.getOwner());
        _log.debug("stopConsumption() callbacks:" + _stopConsumptionCalls.size());
      }
      return ConsumerCallbackResult.SUCCESS;
    }
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    assertTicket();
    assert startScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)startScn).getSequence());

    return super.onStartDataEventSequence(_curScn);
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    assertTicket();
    assert endScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)endScn).getSequence());

    return super.onEndDataEventSequence(_curScn);
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    assertTicket();
    assert rollbackScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)rollbackScn).getSequence());

    return super.onRollback(_curScn);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    assertTicket();
    assert checkpointScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)checkpointScn).getSequence());

    return super.onCheckpoint(_curScn);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    assertTicket();
    assert startScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)startScn).getSequence());

    return super.onStartBootstrapSequence(_curScn);
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    assertTicket();
    assert endScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)endScn).getSequence());

    return super.onEndBootstrapSequence(_curScn);
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN rollbackScn)
  {
    assertTicket();
    assert rollbackScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)rollbackScn).getSequence());

    return super.onBootstrapRollback(_curScn);
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
  {
    assertTicket();
    assert checkpointScn instanceof SingleSourceSCN;

    int pIdx = _currentTicket.getOwner().getPartition().getId();
    _curScn.partitionSeq(pIdx, ((SingleSourceSCN)checkpointScn).getSequence());
    return super.onBootstrapCheckpoint(_curScn);
  }

  @Override
  public boolean canBootstrap()
  {
    return _bootstrapEnabled;
  }

  public RegistrationId getRegId()
  {
    return _regId;
  }

  List<DatabusSubscription> getAllSubs()
  {
    return _allSubs;
  }

  /** Attempts to acquire a ticket for the specified partition consumer
   * @throws InterruptedException */
  PartitionTicket acquireTicket(PartitionConsumer owner) throws InterruptedException
  {
    _ticketLock.lockInterruptibly();

    try
    {
      if (null != _currentTicket && _currentTicket.isValid() && _currentTicket.getOwner() == owner)
      {
        _currentTicket.renew();
        return _currentTicket;
      }

      while (null != _currentTicket && _currentTicket.isValid())
      {
        _ticketDone.await();
      }

      _currentTicket = new PartitionTicket(owner);
      return _currentTicket;
    }
    finally
    {
      _ticketLock.unlock();
    }
  }

  PartitionConsumer createPartitionConsumer(DatabusSubscription sub)
  {
    PhysicalPartition pp = sub.getPhysicalPartition();
    PartitionConsumer cons = _consumers.get(pp);
    List<DatabusSubscription> subs = _subs.get(pp);
    if (null == subs)
    {
      cons = new PartitionConsumer(pp);
      subs = new ArrayList<DatabusSubscription>(1);
      _subs.put(pp, subs);
      _consumers.put(pp, cons);
      ++_partitionNum;
    }

    subs.add(sub);

    return cons;
  }

  public PartitionConsumer getPartitionConsumer(DatabusSubscription sub)
  {
    return _consumers.get(sub.getPhysicalPartition());
  }

  public PartitionConsumer getPartitionConsumer(PhysicalPartition pp)
  {
    return _consumers.get(pp);
  }

  public List<DatabusSubscription> getPartitionSubs(PhysicalPartition pp)
  {
    return _subs.get(pp);
  }

  void assertTicket()
  {
    assert null != _currentTicket;
    try
    {
      assert _currentTicket.isValid();
    }
    catch (InterruptedException e)
    {
      throw new AssertionError("isValid timeout");
    }
  }

  public Map<PhysicalPartition, PartitionConsumer> getConsumers()
  {
    return _consumersRO;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (null != _log && _log.isDebugEnabled()) _log.info("onDataEvent: scn=" + _curScn);
    return super.onDataEvent(e, eventDecoder);
  }

  /**
   * Ticket that needs to be acquired by a PartitionConsumer that allows it to have callbacks
   * to the multiplexing consumer. The ticket expires after some period of time and it is not
   * possible to make any more callbacks until a new ticket is acquired.
   *
   * @author cbotev
   */
  class PartitionTicket implements Runnable, Closeable
  {

    /** The owner of the ticket (mostly for debugging) */
    final PartitionConsumer _owner;

    /** A timestamp when the ticket was acquired  (mostly for debugging) */
    final long _acquisitionTs;

    /** The future of the timeout task*/
    volatile ScheduledFuture<?> _timeoutFuture;

    /** Number of renews (for debugging purposes) */
    AtomicInteger _renewCnt;

    /** A flag if the ticket has expired */
    boolean _expired;

    public PartitionTicket(PartitionConsumer owner)
    {
      _owner = owner;
      _acquisitionTs = System.currentTimeMillis();
      _renewCnt = new AtomicInteger(-1);
      _expired = false;
      renew();
      if (_log.isDebugEnabled()) _log.debug("created ticket for " + owner);
    }

    @Override
    /** Implements the ticket timeout task*/
    public void run()
    {
      _ticketLock.lock();
      try
      {
        _expired = true;
        _ticketDone.signal(); //wake one guy waiting for the lock
      }
      finally
      {
        _ticketLock.unlock();
      }
    }

    @Override
    /** Releases the ticket */
    public void close()
    {
      _ticketLock.lock();
      try
      {
        expireTimeoutTask();
        _expired = true;
        _ticketDone.signal(); //wake one guy waiting for the lock
      }
      finally
      {
        _ticketLock.unlock();
      }
      if (_log.isDebugEnabled()) _log.debug("closed ticket for " + _owner);
    }

    public boolean isValid() throws InterruptedException
    {
      if (!_ticketLock.tryLock(_ticketExpiryPeriodMs, TimeUnit.MILLISECONDS)) return false;
      try
      {
        return (!_expired && null != _timeoutFuture && this == _currentTicket &&
            _timeoutFuture.getDelay(TimeUnit.MILLISECONDS) > 0);
      }
      finally
      {
        _ticketLock.unlock();
      }
    }

    public boolean validate() throws InterruptedException
    {
      if (!_ticketLock.tryLock(_ticketExpiryPeriodMs, TimeUnit.MILLISECONDS)) return false;
      try
      {
        if (! isValid())
        {
          return false;
        }
        return renew();
      }
      finally
      {
        _ticketLock.unlock();
      }
    }

    /** Renews the ticket.
     * @return true iff the renew succeeded
     */
    boolean renew()
    {
      if (!expireTimeoutTask()) return false;

      _timeoutFuture = _timeoutScheduler.schedule(this, _ticketExpiryPeriodMs, TimeUnit.MILLISECONDS);
      _renewCnt.incrementAndGet();

      return true;
    }

    /**
     * Expire any scheduled timeout tasks. This must be called only from someone holding
     * {@link PartitionMultiplexingConsumer#_ticketLock}
     * @return true if the expire succeeded
     */
    private boolean expireTimeoutTask()
    {
      if (null != _timeoutFuture && !_timeoutFuture.isDone())
      {
        _timeoutFuture.cancel(true);
        if (!_timeoutFuture.isCancelled())
        {
          _log.warn("failed to cancel timeout task for " + _owner);
          return false;
        }
      }

      if (_log.isDebugEnabled()) _log.debug("canceled timeout task for " + _owner);
      return true;
    }

    public PartitionConsumer getOwner()
    {
      return _owner;
    }

    public int getRenewCnt()
    {
      return _renewCnt.get();
    }

    /** Number of milliseconds since acquisitions */
    public long getHoldPeriodMs()
    {
      return System.currentTimeMillis() - _acquisitionTs;
    }

    /** Obtain the number of milliseconds left till a timeout */
    public long getTimeoutRemainingMs()
    {
      return (null == _timeoutFuture) ? -1 : _timeoutFuture.getDelay(TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString()
    {
      return "{\"owner\":" + _owner + ", \"expired\":\"" + _expired + "\", \"renewCnt\":" +
              _renewCnt + ", \"timeoutRemainingMs\":" + getTimeoutRemainingMs()
              + "\"holdPeriodMs\":" + getHoldPeriodMs() + "}";
    }
  }

  class PartitionConsumer extends DelegatingDatabusCombinedConsumer implements DatabusV3Consumer
  {
    private final PhysicalPartition _sub;
    private PartitionTicket _ticket;

    public PartitionConsumer(PhysicalPartition pp)
    {
      super(PartitionMultiplexingConsumer.this,
            Logger.getLogger(PartitionMultiplexingConsumer.this._log.getName() + "_" +
                             pp.getName() + "_" +
                             pp.getId()));
      _sub = pp;
    }

    public PhysicalPartition getPartition()
    {
      return _sub;
    }

    @Override
    public String toString()
    {
      return "{\"sub\":" + _sub.toJsonString() + "}";
    }

    @Override
    public ConsumerCallbackResult onStartConsumption()
    {
      if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
      try
      {
        return super.onStartConsumption();
      }
      finally
      {
        releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStopConsumption()
    {
      if (null == _ticket)
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
      }
      else
      {
        if (!ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      }

      try
      {
        return super.onStopConsumption();
      }
      finally
      {
        releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
    {
      if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
      return super.onStartDataEventSequence(startScn);
    }

    @Override
    public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
    {
      boolean releaseLock = _eowReleasesLock;
      try
      {
        if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
        ConsumerCallbackResult res = super.onEndDataEventSequence(endScn);
        if (ConsumerCallbackResult.isFailure(res)) releaseLock = true;

        return res;
      }
      catch (RuntimeException e)
      {
        releaseLock = true;
        throw e;
      }
      finally
      {
        if (releaseLock) releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onRollback(SCN rollbackScn)
    {
      try
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
        return super.onRollback(rollbackScn);
      }
      finally
      {
        releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
    {
      if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      return super.onStartSource(source, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
    {
      if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      return super.onEndSource(source, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
      if (null != _log && _log.isDebugEnabled()) _log.debug("onDataEvent: scn=" + e.sequence());
      if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      return super.onDataEvent(e, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
    {
      boolean releaseTicket = ! _eowReleasesLock;
      if (null == _ticket)
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
        releaseTicket = true;
      }
      else
      {
        if (!ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      }

      try
      {
        return super.onCheckpoint(checkpointScn);
      }
      finally
      {
        if (releaseTicket) releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onError(Throwable err)
    {
      boolean releaseTicket = false;
      if (null == _ticket)
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
        releaseTicket = true;
      }
      else
      {
        if (!ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      }

      try
      {
        PhysicalPartitionConsumptionException newErr =
            new PhysicalPartitionConsumptionException(getPartition(), err);
        return super.onError(newErr);
      }
      finally
      {
        if (releaseTicket) releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStartBootstrap()
    {
      if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
      try
      {
        return super.onStartBootstrap();
      }
      finally
      {
        releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStopBootstrap()
    {
      if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
      try
      {
        return super.onStopBootstrap();
      }
      finally
      {
        releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
    {
      if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
      return super.onStartBootstrapSequence(startScn);
    }

    @Override
    public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
    {
      boolean releaseLock = _eowReleasesLock;
      try
      {
        if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
        ConsumerCallbackResult res = super.onEndBootstrapSequence(endScn);
        if (ConsumerCallbackResult.isFailure(res)) releaseLock = true;

        return res;
      }
      catch (RuntimeException e)
      {
        releaseLock = true;
        throw e;
      }
      finally
      {
        if (releaseLock) releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
    {
      if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      return super.onStartBootstrapSource(name, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
    {
      if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      return super.onEndBootstrapSource(name, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                   DbusEventDecoder eventDecoder)
    {
      if (! ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      return super.onBootstrapEvent(e, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
    {
      try
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
        return super.onBootstrapRollback(batchCheckpointScn);
      }
      finally
      {
        releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
    {
      boolean releaseTicket = false;
      if (null == _ticket)
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
        releaseTicket = true;
      }
      else
      {
        if (!ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      }

      try
      {
        return super.onBootstrapCheckpoint(batchCheckpointScn);
      }
      finally
      {
        if (releaseTicket) releaseMyTicket();
      }
    }

    @Override
    public ConsumerCallbackResult onBootstrapError(Throwable err)
    {
      boolean releaseTicket =  ! _eowReleasesLock;
      if (null == _ticket)
      {
        if (! acquireTicketForMe()) return ConsumerCallbackResult.ERROR;
        releaseTicket = true;
      }
      else
      {
        if (!ensureMyTicket()) return ConsumerCallbackResult.ERROR;
      }

      try
      {
        PhysicalPartitionConsumptionException newErr =
            new PhysicalPartitionConsumptionException(getPartition(), err);
        return super.onBootstrapError(newErr);
      }
      finally
      {
        if (releaseTicket) releaseMyTicket();
      }
    }

    boolean acquireTicketForMe()
    {

      try
      {
        if (_ticket != null && _ticket.validate())
        {
          _log.warn("ticket should have been closed; reusing " + _ticket);
        }
        else
        {
          _ticket = acquireTicket(this);
        }
        return true;
      }
      catch (InterruptedException e)
      {
        _log.error("failed to acquire ticket");
        return false;
      }
    }

    boolean ensureMyTicket()
    {
      if (null == _ticket)
      {
        _log.error("missing ticket");
        return false;
      }
      try
      {
        if (! _ticket.validate())
        {
          _log.error("timeout processing window");
          return false;
        }
      }
      catch (InterruptedException e)
      {
        _log.info("ticket acquisition interrupted");
        return false;
      }

      return true;
    }

    void releaseMyTicket()
    {
      if (null != _ticket) _ticket.close();
      _ticket = null;
    }

    @Override
    public boolean canBootstrap()
    {
      return _bootstrapEnabled;
    }
  }

  public MultiPartitionSCN getCurScn()
  {
    return _curScn;
  }

}
