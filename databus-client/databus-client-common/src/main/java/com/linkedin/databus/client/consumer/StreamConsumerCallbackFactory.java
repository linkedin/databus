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


import org.apache.avro.Schema;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalWritable;

/**
 * A factory for stream-consumer callbacks.
 */
public class StreamConsumerCallbackFactory implements ConsumerCallbackFactory<DatabusCombinedConsumer>
{
  protected final ConsumerCallbackStats _consumerStats;
  protected final UnifiedClientStats _unifiedClientStats;

  public StreamConsumerCallbackFactory(ConsumerCallbackStats consumerStats,
                                       UnifiedClientStats unifiedClientStats)
  {
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createCheckpointCallable(long currentNanos,
                                                                           SCN scn,
                                                                           DatabusCombinedConsumer consumer,
                                                                           boolean updateStats)
  {
    if (updateStats) {
      return new CheckpointCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new CheckpointCallable(currentNanos, scn, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createDataEventCallable(long currentNanos,
                                                                          DbusEvent e,
                                                                          DbusEventDecoder eventDecoder,
                                                                          DatabusCombinedConsumer consumer,
                                                                          boolean updateStats)
  {
    if (updateStats) {
      return new OnDataEventCallable(currentNanos, e, eventDecoder, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new OnDataEventCallable(currentNanos, e, eventDecoder, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createEndConsumptionCallable(long currentNanos,
                                                                               DatabusCombinedConsumer consumer,
                                                                               boolean updateStats)
  {
    if (updateStats) {
      return new StopConsumptionCallable(currentNanos, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StopConsumptionCallable(currentNanos, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createEndDataEventSequenceCallable(long currentNanos,
                                                                                     SCN scn,
                                                                                     DatabusCombinedConsumer consumer,
                                                                                     boolean updateStats)
  {
    if (updateStats) {
      return new EndDataEventSequenceCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new EndDataEventSequenceCallable(currentNanos, scn, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createEndSourceCallable(long currentNanos,
                                                                          String source,
                                                                          Schema sourceSchema,
                                                                          DatabusCombinedConsumer consumer,
                                                                          boolean updateStats)
  {
    if (updateStats) {
      return new EndSourceCallable(currentNanos, source, sourceSchema, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new EndSourceCallable(currentNanos, source, sourceSchema, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createRollbackCallable(long currentNanos,
                                                                         SCN scn,
                                                                         DatabusCombinedConsumer consumer,
                                                                         boolean updateStats)
  {
    if (updateStats) {
      return new RollbackCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new RollbackCallable(currentNanos, scn, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createStartConsumptionCallable(long currentNanos,
                                                                                 DatabusCombinedConsumer consumer,
                                                                                 boolean updateStats)
  {
    if (updateStats) {
      return new StartConsumptionCallable(currentNanos, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StartConsumptionCallable(currentNanos, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createStartDataEventSequenceCallable(long currentNanos,
                                                                                       SCN scn,
                                                                                       DatabusCombinedConsumer consumer,
                                                                                       boolean updateStats)
  {
    if (updateStats) {
      return new StartDataEventSequenceCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StartDataEventSequenceCallable(currentNanos, scn, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createStartSourceCallable(long currentNanos,
                                                                            String source,
                                                                            Schema sourceSchema,
                                                                            DatabusCombinedConsumer consumer,
                                                                            boolean updateStats)
  {
    if (updateStats) {
      return new StartSourceCallable(currentNanos, source, sourceSchema, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StartSourceCallable(currentNanos, source, sourceSchema, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createOnErrorCallable(long currentNanos,
                                                                        Throwable err,
                                                                        DatabusCombinedConsumer consumer,
                                                                        boolean updateStats)
  {
    if (updateStats) {
      return new OnErrorCallable(currentNanos, err, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new OnErrorCallable(currentNanos, err, consumer, null, null);
    }
  }
}

class OnErrorCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final Throwable _err;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public OnErrorCallable(long currentNanos,
                         Throwable err,
                         DatabusCombinedConsumer consumer,
                         ConsumerCallbackStats consumerStats,
                         UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _err = err;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onError(_err);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class CheckpointCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public CheckpointCallable(long currentNanos,
                            SCN scn,
                            DatabusCombinedConsumer consumer,
                            ConsumerCallbackStats consumerStats,
                            UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
     ConsumerCallbackResult res =  _consumer.onCheckpoint(_scn);
     return ConsumerCallbackResult.isFailure(res) ? ConsumerCallbackResult.SKIP_CHECKPOINT : res;
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class OnDataEventCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DbusEvent _event;
  private final DbusEventDecoder _eventDecoder;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public OnDataEventCallable(long currentNanos,
                             DbusEvent e,
                             DbusEventDecoder eventDecoder,
                             DatabusCombinedConsumer consumer)
  {
    this(currentNanos, e, eventDecoder, consumer, null, null);
  }

  public OnDataEventCallable(long currentNanos,
                             DbusEvent e,
                             DbusEventDecoder eventDecoder,
                             DatabusCombinedConsumer consumer,
                             ConsumerCallbackStats consumerStats,
                             UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    if (!(e instanceof DbusEventInternalWritable)) {
      throw new UnsupportedClassVersionError("Cannot support cloning on non-DbusEvent");
    }
    _event = ((DbusEventInternalWritable)e).clone(null);
    _eventDecoder = eventDecoder;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onDataEvent(_event, _eventDecoder);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerDataErrorsProcessed();
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerDataEventsProcessed(1, totalTime, _event);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class StopConsumptionCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StopConsumptionCallable(long currentNanos,
                                 DatabusCombinedConsumer consumer,
                                 ConsumerCallbackStats consumerStats,
                                 UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStopConsumption();
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class EndDataEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public EndDataEventSequenceCallable(long currentNanos,
                                      SCN scn,
                                      DatabusCombinedConsumer consumer,
                                      ConsumerCallbackStats consumerStats,
                                      UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onEndDataEventSequence(_scn);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerSysErrorsProcessed();
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerSystemEventProcessed(totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class EndSourceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final String _source;
  private final Schema _sourceSchema;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public EndSourceCallable(long currentNanos,
                           String source,
                           Schema sourceSchema,
                           DatabusCombinedConsumer consumer,
                           ConsumerCallbackStats consumerStats,
                           UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _source = source;
    _sourceSchema = sourceSchema;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onEndSource(_source, _sourceSchema);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class RollbackCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;


  public RollbackCallable(long currentNanos,
                          SCN scn,
                          DatabusCombinedConsumer consumer,
                          ConsumerCallbackStats consumerStats,
                          UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onRollback(_scn);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class StartConsumptionCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StartConsumptionCallable(long currentNanos,
                                  DatabusCombinedConsumer consumer,
                                  ConsumerCallbackStats consumerStats,
                                  UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartConsumption();
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class StartDataEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StartDataEventSequenceCallable(long currentNanos,
                                        SCN scn,
                                        DatabusCombinedConsumer consumer,
                                        ConsumerCallbackStats consumerStats,
                                        UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartDataEventSequence(_scn);
  }

  public SCN getSCN()
  {
    return _scn;
  }
  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}

class StartSourceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final String _source;
  private final Schema _sourceSchema;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StartSourceCallable(long currentNanos,
                             String source,
                             Schema sourceSchema,
                             DatabusCombinedConsumer consumer,
                             ConsumerCallbackStats consumerStats,
                             UnifiedClientStats unifiedClientStats)
  {
    super(currentNanos);
    _source = source;
    _sourceSchema = sourceSchema;
    _consumer = consumer;
    _consumerStats = consumerStats;
    _unifiedClientStats = unifiedClientStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartSource(_source, _sourceSchema);
  }

  public String getSource()
  {
    return _source;
  }

  public Schema getSourceSchema()
  {
    return _sourceSchema;
  }

  public DatabusCombinedConsumer getConsumer()
  {
    return _consumer;
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
    if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
    {
      if (_consumerStats != null) _consumerStats.registerErrorEventsProcessed(1);
      if (_unifiedClientStats != null) _unifiedClientStats.registerCallbackError();
    }
    else
    {
      long nanoRunTime = getNanoRunTime();
      if (_consumerStats != null)
      {
        long totalTime = (nanoRunTime + getNanoTimeInQueue()) / DbusConstants.NUM_NSECS_IN_MSEC;
        _consumerStats.registerEventsProcessed(1, totalTime);
      }
      if (_unifiedClientStats != null)
      {
        _unifiedClientStats.registerCallbacksProcessed(nanoRunTime);
      }
    }
  }
}
