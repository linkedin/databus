package com.linkedin.databus.client.consumer;
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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.IdNamePair;

/**
 * Implements callbacks to multiple databus consumers in parallel. It also enforces
 * configurable time budget.
 */
public class MultiConsumerCallback implements DatabusStreamConsumer
{
  public final String MODULE = MultiConsumerCallback.class.getName();
  public final Logger LOG = Logger.getLogger(MODULE);

  private static final long MILLI_NANOS = 1000000L;

  private final List<DatabusV2ConsumerRegistration> _registrations;
  private final ExecutorService _executorService;
  private final List<ConsumerCallable<ConsumerCallbackResult>> _currentBatch;
  private final ConsumerCallbackFactory<DatabusCombinedConsumer> _callbackFactory;
  private final long _timeBudgetNanos;
  private Map<Long, IdNamePair> _sourceMap;
  private long _runCallsCounter;
  private PriorityQueue<TimestampedFuture<ConsumerCallbackResult>> _submittedCalls;
  private final Lock _lock = new ReentrantLock();
  private final LoggingConsumer _loggingConsumer;

  //local stats accumulator
  private final ConsumerCallbackStats _consumerStats;


  // used only by tests
  public MultiConsumerCallback(List<DatabusV2ConsumerRegistration> registrations,
          ExecutorService executorService,
          long timeBudgetMs,
          ConsumerCallbackFactory<DatabusCombinedConsumer> callbackFactory
         )
  {
    this(registrations, executorService, timeBudgetMs, callbackFactory, null, null);
  }

  public MultiConsumerCallback(List<DatabusV2ConsumerRegistration> registrations,
                               ExecutorService executorService,
                               long timeBudgetMs,
                               ConsumerCallbackFactory<DatabusCombinedConsumer> callbackFactory,
                               ConsumerCallbackStats consumerStats,
                               LoggingConsumer loggingConsumer)
  {
    super();
    _registrations = registrations;
    _executorService = executorService;
    _timeBudgetNanos = timeBudgetMs * MILLI_NANOS;
    _currentBatch = new ArrayList<ConsumerCallable<ConsumerCallbackResult>>(2048);
    _callbackFactory = callbackFactory;
    _runCallsCounter = 0;
    _submittedCalls =
        new PriorityQueue<MultiConsumerCallback.TimestampedFuture<ConsumerCallbackResult>>(100,
            new TimestampedFutureComparator<ConsumerCallbackResult>());
    _consumerStats = consumerStats;
    // TODO:  Should we sniff registrations list and make sure no LoggingConsumers in there?
    //        Sole non-test caller is DatabusSourcesConnection, so as long as LoggingConsumer
    //        is not available to end-users, no need...
    _loggingConsumer = loggingConsumer;  // may be null in unit tests
  }

  private ConsumerCallbackResult submitBatch(long curNanos, boolean barrierBefore,
                                             boolean barrierAfter)
  {
    ++ _runCallsCounter;
    ConsumerCallbackResult retValue = ConsumerCallbackResult.SUCCESS;
    if (0 >= curNanos) curNanos = System.nanoTime();
    try
    {
      if (barrierBefore) retValue = flushCallQueue(curNanos);

      if (ConsumerCallbackResult.isSuccess(retValue))
      {
        String batchName = _currentBatch.size() > 0 ? _currentBatch.get(0).getClass().getSimpleName()
                                                    : "";
        for (ConsumerCallable<ConsumerCallbackResult> call: _currentBatch)
        {
          Future<ConsumerCallbackResult> future = _executorService.submit(call);
          _submittedCalls.add(new TimestampedFuture<ConsumerCallbackResult>(call, future,
              batchName, ++_runCallsCounter));

        }
      }
      _currentBatch.clear();

      if (ConsumerCallbackResult.isSuccess(retValue))
      {
        ConsumerCallbackResult retValue2 = barrierAfter ? flushCallQueue(curNanos)
                                                        : cleanUpCallQueue(curNanos);
        retValue = ConsumerCallbackResult.max(retValue, retValue2);
      }
    }
    catch (RuntimeException e)
    {
      LOG.error("internal callback error: " + e.getMessage(), e);
      retValue = ConsumerCallbackResult.ERROR;
    }
    return retValue;
  }

  private ConsumerCallbackResult cleanUpCallQueue(long curNanos)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    TimestampedFuture<ConsumerCallbackResult> top = null;
    if (0 > curNanos) curNanos = System.nanoTime();
    long timeoutNanos = _timeBudgetNanos > 0 ? curNanos - _timeBudgetNanos : 0;

    //remove completed or expired calls at the head of the queue
    while ((top = _submittedCalls.peek()) != null &&
           (timeoutNanos >= top.getTimestamp() || top.getFuture().isDone()))
    {
      ConsumerCallbackResult callRes = null;
      if (top.getFuture().isDone())
      {
        callRes = getCallResult(top.getFuture(), top.getCallType(), -1);
      }
      else
      {
        //timeout
        callRes = ConsumerCallbackResult.ERROR;
        top.getFuture().cancel(true);
        LOG.error("callback timeout: " + top.getCallType() + "; runtime, ms : " +
                  ((top.getTimestamp() - curNanos) / 1000000.0)
                  + "; try increasing client.connectionDefaults.consumerTimeBudgetMs");
      }

      result = ConsumerCallbackResult.max(result, callRes);
      if (ConsumerCallbackResult.isFailure(result))
      {
        LOG.error("error detected; cancelling all " + _submittedCalls.size() + " outstanding callbacks ");
        cancelCalls();
      }
      //remove the call
      dequeueTopFuture(result);
    }

    return result;
  }


  private void dequeueTopFuture(ConsumerCallbackResult result)
  {
    _lock.lock();
    try
    {
      TimestampedFuture<ConsumerCallbackResult> future = _submittedCalls.poll();

      if (null == future) return;
      ConsumerCallable<ConsumerCallbackResult> callable = future.getCallable();
      callable.endCall(result);
    }
    finally
    {
      _lock.unlock();
    }
  }

  private ConsumerCallbackResult getCallResult(Future<ConsumerCallbackResult> future,
                                               String callType,
                                               long timeoutNanos)
  {
    try
    {
      if (timeoutNanos == 0)
      {
        LOG.error("Exhausted time budget of " + _timeBudgetNanos/MILLI_NANOS + "ms. Skipping remaining callbacks of type "
                      + callType);
    	  throw new TimeoutException("No time remaining in a timeout budget of " + (_timeBudgetNanos/MILLI_NANOS) + " ms");
        // Exception caught below
      }

      ConsumerCallbackResult result =  timeoutNanos < 0 ? future.get()
                                                         : future.get(timeoutNanos, TimeUnit.NANOSECONDS);
      if (result == null)
      {
        result = ConsumerCallbackResult.ERROR;
        LOG.error("Client application callback (" + callType + ") returned null");
      }
      else if (!ConsumerCallbackResult.isSuccess(result))
      {
        LOG.error("Client application callback (" + callType + ") returned error:" + result);
      }
      return result;
    }
    catch (ExecutionException ee)
    {
      // Consumer threw an exception while fielding the callback.
      LOG.error("Uncaught exception in client application callback (" + callType + "): " + ee.getCause().getCause(), ee.getCause());
    }
    catch (InterruptedException ee)
    {
      LOG.warn("Client application callback (" + callType + ") interrupted");
    }
    catch (TimeoutException te)
    {
      LOG.error("Client application timed out handling callback: " + callType +
                "; Try increasing client.connectionDefaults.consumerTimeBudgetMs " +
                " or client.connectionDefaults.bstConsumerTimeBudgetMs");
    }

    return ConsumerCallbackResult.ERROR;
  }

  /** Cancel all outstanding calls */
  private void cancelCalls()
  {
      //cancel all remaining outstanding calls
      TimestampedFuture<ConsumerCallbackResult> top = null;
      while ( (top = _submittedCalls.poll()) != null)
      {
        try
        {
          if (! top.getFuture().isDone()) top.getFuture().cancel(true);
          top.getCallable().endCall(ConsumerCallbackResult.ERROR);
        }
        catch (RuntimeException e)
        {
          LOG.error("unable to cancel call: " + top.getCallType() + ": " + e.getMessage(), e);
        }
      }
  }

  /** Acts as a barrier for all outstanding calls in the call queue */
  public ConsumerCallbackResult flushCallQueue(long curTime)
  {
    ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;
    if (0 >= curTime) curTime = System.nanoTime();

    TimestampedFuture<ConsumerCallbackResult> top = null;
    while (_submittedCalls.size() > 0)
    {
      top = _submittedCalls.peek();
      ConsumerCallbackResult topResult = null;
      Future<ConsumerCallbackResult> topFuture = top.getFuture();

      if (!topFuture.isDone())
      {
        if (0 >= curTime) curTime = System.nanoTime();
        long calcTimeout =  getEstimatedTimeout(_timeBudgetNanos, curTime,top);
        long timeoutNanos = _timeBudgetNanos > 0 ? ( calcTimeout > 0 ? calcTimeout : 0) : -1;
        topResult = getCallResult(topFuture, top.getCallType(), timeoutNanos);
        curTime = -1;
      }

      if (topFuture.isDone() && null == topResult) topResult = getCallResult(topFuture, top.getCallType(), -1);
      if (null == topResult) topResult = ConsumerCallbackResult.ERROR;

      result = ConsumerCallbackResult.max(result, topResult);
      if (ConsumerCallbackResult.isFailure(result))
      {
        LOG.error("error detected; cancelling all " + _submittedCalls.size() + " outstanding callbacks");
        cancelCalls();
      }

      if (topFuture.isDone() && result != ConsumerCallbackResult.ERROR)
      {
    	  boolean debugEnabled = LOG.isDebugEnabled();
    	  if (top.getCallType().equals("StartSourceCallable"))
    	  {
        	  long runTime = top.getCallable().getNanoRunTime() / MILLI_NANOS;
        	  if (debugEnabled)
        	  {
        		  StartSourceCallable tf = (StartSourceCallable) top.getCallable();
        		  LOG.debug("StartSourceCallable time taken for source " + tf.getSource() + " = " + runTime);
        	  }
    	  }
    	  else if (top.getCallType().equals("StartDataEventSequenceCallable"))
    	  {
        	  long runTime = top.getCallable().getNanoRunTime() / MILLI_NANOS;
        	  if (debugEnabled)
        	  {
        		  StartDataEventSequenceCallable tf = (StartDataEventSequenceCallable) top.getCallable();
        		  LOG.debug("StartDataEventSequenceCallable time taken for source " + tf.getSCN() + " = " + runTime);
        	  }
    	  }
      }

      dequeueTopFuture(result);
    }

    return result;
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> checkpointCallable =
            _callbackFactory.createCheckpointCallable(curNanos, checkpointScn, consumer,_consumerStats);
        _currentBatch.add(checkpointCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> checkpointCallable =
          _callbackFactory.createCheckpointCallable(curNanos, checkpointScn, _loggingConsumer, null);
      _currentBatch.add(checkpointCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onCheckpoint = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, true, true);
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    boolean debugEnabled = LOG.isDebugEnabled();

    long curNanos = System.nanoTime();
    if (null == _sourceMap)
    {
      LOG.error("No sources map specified");
      if (_consumerStats != null) _consumerStats.registerSrcErrors();
      return ConsumerCallbackResult.ERROR;
    }

    long srcid = e.srcId();
    short lPartitionId = e.logicalPartitionId();
    IdNamePair eventSource = _sourceMap.get(srcid);
    if (null == eventSource)
    {
      LOG.error("Unknown source");
      if (_consumerStats != null) _consumerStats.registerSrcErrors();
      return ConsumerCallbackResult.ERROR;
    }

    for (DatabusV2ConsumerRegistration reg: _registrations)
    {
      DatabusSubscription eventSourceName = DatabusSubscription.createSubscription(eventSource, lPartitionId);
      if (debugEnabled) LOG.debug("event source=" + eventSource + " lpart=" + lPartitionId);
      if (reg.checkSourceSubscription(eventSourceName))
      {

        if (debugEnabled) LOG.debug("consumer matches:" + reg.getConsumer());
        ConsumerCallable<ConsumerCallbackResult> dataEventCallable =
            _callbackFactory.createDataEventCallable(curNanos, e, eventDecoder, reg.getConsumer(),_consumerStats);
        _currentBatch.add(dataEventCallable);
        if (_consumerStats != null) _consumerStats.registerDataEventReceived(e);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> dataEventCallable =
          _callbackFactory.createDataEventCallable(curNanos, e, eventDecoder, _loggingConsumer, null);
      _currentBatch.add(dataEventCallable);
    }
    if (debugEnabled)
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onDataEvent = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }

    return submitBatch(curNanos, false, false);
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> endWindowCallable =
            _callbackFactory.createEndDataEventSequenceCallable(curNanos, endScn, consumer,_consumerStats);
        _currentBatch.add(endWindowCallable);
        if (_consumerStats != null) _consumerStats.registerSystemEventReceived();
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> endWindowCallable =
          _callbackFactory.createEndDataEventSequenceCallable(curNanos, endScn, _loggingConsumer, null);
      _currentBatch.add(endWindowCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onEndDataEventSequence = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, true, true);
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> endSourceCallable =
            _callbackFactory.createEndSourceCallable(curNanos, source, sourceSchema, consumer,_consumerStats);
        _currentBatch.add(endSourceCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> endSourceCallable =
          _callbackFactory.createEndSourceCallable(curNanos, source, sourceSchema, _loggingConsumer, null);
      _currentBatch.add(endSourceCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onEndSource = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, true, true);
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> rollbackCallable =
            _callbackFactory.createRollbackCallable(curNanos, startScn, consumer,_consumerStats);
        _currentBatch.add(rollbackCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> rollbackCallable =
          _callbackFactory.createRollbackCallable(curNanos, startScn, _loggingConsumer, null);
      _currentBatch.add(rollbackCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onRollback = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, true, true);
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> startWindowCallable =
          _callbackFactory.createStartDataEventSequenceCallable(curNanos, startScn, consumer,_consumerStats);
        _currentBatch.add(startWindowCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> startWindowCallable =
          _callbackFactory.createStartDataEventSequenceCallable(curNanos, startScn, _loggingConsumer, null);
      _currentBatch.add(startWindowCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onStartDataEventSequence = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }

    return submitBatch(curNanos, true, true);
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> startSourceCallable =
          _callbackFactory.createStartSourceCallable(curNanos, source, sourceSchema, consumer,_consumerStats);
        _currentBatch.add(startSourceCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> startSourceCallable =
          _callbackFactory.createStartSourceCallable(curNanos, source, sourceSchema, _loggingConsumer, null);
      _currentBatch.add(startSourceCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onStartSource = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }

    return submitBatch(curNanos, false, true);
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> startConsumptionCallable =
            _callbackFactory.createStartConsumptionCallable(curNanos, consumer,_consumerStats);
        _currentBatch.add(startConsumptionCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> startConsumptionCallable =
          _callbackFactory.createStartConsumptionCallable(curNanos, _loggingConsumer, null);
      _currentBatch.add(startConsumptionCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onStartConsumption = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, false, true);
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> endConsumptionCallable =
          _callbackFactory.createEndConsumptionCallable(curNanos, consumer,_consumerStats);
        _currentBatch.add(endConsumptionCallable);
        if (_consumerStats != null) _consumerStats.registerEventsReceived(1);

      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> endConsumptionCallable =
          _callbackFactory.createEndConsumptionCallable(curNanos, _loggingConsumer, null);
      _currentBatch.add(endConsumptionCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onStopConsumption = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, true, true);
  }

  public void setSourceMap(Map<Long, IdNamePair> sourceMap)
  {
    _sourceMap = sourceMap;
  }

  public Map<Long, IdNamePair> getSourceMap()
  {
    return _sourceMap;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    long curNanos = System.nanoTime();
    for (DatabusV2ConsumerRegistration consumerReg: _registrations)
    {
      for (DatabusCombinedConsumer consumer: consumerReg.getConsumers())
      {
        ConsumerCallable<ConsumerCallbackResult> onErrorCallable =
          _callbackFactory.createOnErrorCallable(curNanos, err, consumer,_consumerStats);
        _currentBatch.add(onErrorCallable);
        if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      }
    }
    if (_loggingConsumer != null)
    {
      ConsumerCallable<ConsumerCallbackResult> onErrorCallable =
          _callbackFactory.createOnErrorCallable(curNanos, err, _loggingConsumer, null);
      _currentBatch.add(onErrorCallable);
    }
    if (LOG.isDebugEnabled())
    {
    	long endNanos = System.nanoTime();
    	LOG.debug("Time spend in databus clientlib by onError = " + (endNanos - curNanos) / MILLI_NANOS + "ms");
    }
    return submitBatch(curNanos, true, true);
  }

  public ConsumerCallbackStats getStats()
  {
      return _consumerStats;
  }

  static class TimestampedFuture<T>
  {
    private final Future<T> _future;
    private final String _callType;
    private final long _callNum;
    private final ConsumerCallable<T> _callable;

    public TimestampedFuture(ConsumerCallable<T> callable, Future<T> future, String callType,
                             long callNum)
    {
      super();
      _future = future;
      _callType = callType;
      _callNum = callNum;
      _callable = callable;
    }

    public Future<T> getFuture()
    {
      return _future;
    }
    public long getTimestamp()
    {
      return _callable.getCreationTime();
    }

    public long getCallNum()
    {
      return _callNum;
    }

    public String getCallType()
    {
      return _callType;
    }

    public ConsumerCallable<T> getCallable()
    {
      return _callable;
    }
  }

  static class TimestampedFutureComparator<T> implements Comparator<TimestampedFuture<T>>, Serializable
  {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(TimestampedFuture<T> o1, TimestampedFuture<T> o2)
    {
      return (int)(o1.getTimestamp() - o2.getTimestamp());
    }
  }

  public void removeRegistration(DatabusV2ConsumerRegistration reg)
  {
    _registrations.remove(reg);
  }

  protected long getEstimatedTimeout(long timeBudget,
		  							 long curTime,
		  							 TimestampedFuture<ConsumerCallbackResult> top )
  {
	  return (timeBudget - (curTime - top.getTimestamp()));
  }
}
