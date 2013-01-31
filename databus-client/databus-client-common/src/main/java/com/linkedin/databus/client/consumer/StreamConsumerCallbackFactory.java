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
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalWritable;

/**
 * A factory for stream consumer callbacks.
 * @author cbotev
 *
 */
public class StreamConsumerCallbackFactory implements ConsumerCallbackFactory<DatabusCombinedConsumer>
{

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createCheckpointCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new CheckpointCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
                                 DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new OnDataEventCallable(currentNanos, e, eventDecoder, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createEndConsumptionCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StopConsumptionCallable(currentNanos, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createEndDataEventSequenceCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new EndDataEventSequenceCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createEndSourceCallable(long currentNanos, String source, Schema sourceSchema,
                                 DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new EndSourceCallable(currentNanos, source, sourceSchema, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createRollbackCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new RollbackCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createStartConsumptionCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StartConsumptionCallable(currentNanos, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createStartDataEventSequenceCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StartDataEventSequenceCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createStartSourceCallable(long currentNanos, String source, Schema sourceSchema,
                                   DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StartSourceCallable(currentNanos, source, sourceSchema, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createOnErrorCallable(long currentNanos, Throwable err, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new OnErrorCallable(currentNanos, err, consumer,stats);
  }
}

class OnErrorCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final Throwable _err;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public OnErrorCallable(long currentNanos, Throwable err, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _err = err;
    _consumer = consumer;
    _consumerStats = stats;
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
		  if (null != _consumerStats) _consumerStats.registerErrorEventsProcessed(1);
	  }
	  else
	  {
		  long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
		  if (null != _consumerStats) _consumerStats.registerEventsProcessed(1, totalTime);
	  }
  }
}

class CheckpointCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public CheckpointCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onCheckpoint(_scn);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			 _consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1,totalTime);
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

  public OnDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
          DatabusCombinedConsumer consumer)
  {
	  this(currentNanos,e,eventDecoder,consumer,null);
  }

  public OnDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
                             DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    if (!(e instanceof DbusEventInternalWritable)) {
      throw new UnsupportedClassVersionError("Cannot support cloning on non-Dbusevent");
    }
    _event = ((DbusEventInternalWritable)e).clone(null);
    _eventDecoder = eventDecoder;
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onDataEvent(_event, _eventDecoder);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerDataErrorsProcessed();
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerDataEventsProcessed(1, totalTime,_event);
 		 }
 	 }
  }

}

class StopConsumptionCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public StopConsumptionCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStopConsumption();
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1, totalTime);
 		 }
 	 }
  }

}

class EndDataEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public EndDataEventSequenceCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onEndDataEventSequence(_scn);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerSysErrorsProcessed();
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerSystemEventProcessed(totalTime);
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

  public EndSourceCallable(long currentNanos, String source, Schema sourceSchema,
                           DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _source = source;
    _sourceSchema = sourceSchema;
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onEndSource(_source, _sourceSchema);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1, totalTime);
 		 }
 	 }
  }

}

class RollbackCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public RollbackCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onRollback(_scn);
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1, totalTime);
 		 }
 	 }
  }

}

class StartConsumptionCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public StartConsumptionCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _consumer = consumer;
    _consumerStats = stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartConsumption();
  }

  @Override
  protected void doEndCall(ConsumerCallbackResult result)
  {
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1, totalTime);
 		 }
 	 }
  }

}

class StartDataEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public StartDataEventSequenceCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats = stats;
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
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1, totalTime);
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

  public StartSourceCallable(long currentNanos, String source, Schema sourceSchema,
                             DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _source = source;
    _sourceSchema = sourceSchema;
    _consumer = consumer;
    _consumerStats = stats;
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
 	 if (_consumerStats != null)
 	 {
 		 if (result==ConsumerCallbackResult.ERROR || result==ConsumerCallbackResult.ERROR_FATAL)
 		 {
 			_consumerStats.registerErrorEventsProcessed(1);
 		 }
 		 else
 		 {
 		 	 long totalTime = (getNanoRunTime() + getNanoTimeInQueue())/1000000;
 			 _consumerStats.registerEventsProcessed(1, totalTime);
 		 }
 	 }
  }


}
