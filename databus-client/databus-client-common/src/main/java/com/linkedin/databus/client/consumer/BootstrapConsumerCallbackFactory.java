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
 * A factory for bootstrap-consumer callbacks.
 */
public class BootstrapConsumerCallbackFactory implements ConsumerCallbackFactory<DatabusCombinedConsumer>
{
  protected final ConsumerCallbackStats _consumerStats;
  protected final UnifiedClientStats _unifiedClientStats;

  public BootstrapConsumerCallbackFactory(ConsumerCallbackStats consumerStats,
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
      return new BootstrapCheckpointCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new BootstrapCheckpointCallable(currentNanos, scn, consumer, null, null);
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
      return new BootstrapDataEventCallable(currentNanos, e, eventDecoder, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new BootstrapDataEventCallable(currentNanos, e, eventDecoder, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createEndConsumptionCallable(long currentNanos,
                                                                               DatabusCombinedConsumer consumer,
                                                                               boolean updateStats)
  {
    if (updateStats) {
      return new StopBootstrapCallable(currentNanos, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StopBootstrapCallable(currentNanos, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createEndDataEventSequenceCallable(long currentNanos,
                                                                                     SCN scn,
                                                                                     DatabusCombinedConsumer consumer,
                                                                                     boolean updateStats)
  {
    if (updateStats) {
      return new EndBootstrapEventSequenceCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new EndBootstrapEventSequenceCallable(currentNanos, scn, consumer, null, null);
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
      return new EndBootstrapSourceCallable(currentNanos, source, sourceSchema, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new EndBootstrapSourceCallable(currentNanos, source, sourceSchema, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createRollbackCallable(long currentNanos,
                                                                         SCN scn,
                                                                         DatabusCombinedConsumer consumer,
                                                                         boolean updateStats)
  {
    if (updateStats) {
      return new BootstrapRollbackCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new BootstrapRollbackCallable(currentNanos, scn, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createStartConsumptionCallable(long currentNanos,
                                                                                 DatabusCombinedConsumer consumer,
                                                                                 boolean updateStats)
  {
    if (updateStats) {
      return new StartBootstrapCallable(currentNanos, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StartBootstrapCallable(currentNanos, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createStartDataEventSequenceCallable(long currentNanos,
                                                                                       SCN scn,
                                                                                       DatabusCombinedConsumer consumer,
                                                                                       boolean updateStats)
  {
    if (updateStats) {
      return new StartBootstrapEventSequenceCallable(currentNanos, scn, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StartBootstrapEventSequenceCallable(currentNanos, scn, consumer, null, null);
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
      return new StartBootstrapSourceCallable(currentNanos, source, sourceSchema, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new StartBootstrapSourceCallable(currentNanos, source, sourceSchema, consumer, null, null);
    }
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult> createOnErrorCallable(long currentNanos,
                                                                        Throwable err,
                                                                        DatabusCombinedConsumer consumer,
                                                                        boolean updateStats)
  {
    if (updateStats) {
      return new OnBootstrapErrorCallable(currentNanos, err, consumer, _consumerStats, _unifiedClientStats);
    } else {
      return new OnBootstrapErrorCallable(currentNanos, err, consumer, null, null);
    }
  }

}

class OnBootstrapErrorCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final Throwable _err;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public OnBootstrapErrorCallable(long currentNanos, Throwable err,
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
    return _consumer.onBootstrapError(_err);
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

class BootstrapCheckpointCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public BootstrapCheckpointCallable(long currentNanos,
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
     ConsumerCallbackResult res = _consumer.onBootstrapCheckpoint(_scn);
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

class BootstrapDataEventCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DbusEvent _event;
  private final DbusEventDecoder _eventDecoder;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public BootstrapDataEventCallable(long currentNanos,
                                    DbusEvent e,
                                    DbusEventDecoder eventDecoder,
                                    DatabusCombinedConsumer consumer)
  {
    this(currentNanos, e, eventDecoder, consumer, null, null);
  }

  public BootstrapDataEventCallable(long currentNanos,
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
    return _consumer.onBootstrapEvent(_event, _eventDecoder);
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

class StopBootstrapCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StopBootstrapCallable(long currentNanos,
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
    return _consumer.onStopBootstrap();
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

class EndBootstrapEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public EndBootstrapEventSequenceCallable(long currentNanos, SCN scn,
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
    return _consumer.onEndBootstrapSequence(_scn);
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

class EndBootstrapSourceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final String _source;
  private final Schema _sourceSchema;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public EndBootstrapSourceCallable(long currentNanos,
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
    return _consumer.onEndBootstrapSource(_source, _sourceSchema);
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

class BootstrapRollbackCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public BootstrapRollbackCallable(long currentNanos, SCN scn,
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
    return _consumer.onBootstrapRollback(_scn);
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

class StartBootstrapCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StartBootstrapCallable(long currentNanos,
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
    return _consumer.onStartBootstrap();
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

class StartBootstrapEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StartBootstrapEventSequenceCallable(long currentNanos, SCN scn,
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
    return _consumer.onStartBootstrapSequence(_scn);
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

class StartBootstrapSourceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final String _source;
  private final Schema _sourceSchema;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;
  private final UnifiedClientStats _unifiedClientStats;

  public StartBootstrapSourceCallable(long currentNanos,
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
    return _consumer.onStartBootstrapSource(_source, _sourceSchema);
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
