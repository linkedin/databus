package com.linkedin.databus.client.consumer;

import com.linkedin.databus.core.DbusEventInternalWritable;
import org.apache.avro.Schema;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.core.DbusEvent;

/**
 * A factory for bootstrap consumer callbacks.
 * @author cbotev
 *
 */
public class BootstrapConsumerCallbackFactory implements ConsumerCallbackFactory<DatabusCombinedConsumer>
{

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createCheckpointCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new BootstrapCheckpointCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
                                 DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new BootstrapDataEventCallable(currentNanos, e, eventDecoder, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createEndConsumptionCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StopBootstrapCallable(currentNanos, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createEndDataEventSequenceCallable(long currentNanos, SCN scn,
                                            DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new EndBootstrapEventSequenceCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createEndSourceCallable(long currentNanos, String source, Schema sourceSchema,
                                 DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new EndBootstrapSourceCallable(currentNanos, source, sourceSchema, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createRollbackCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new BootstrapRollbackCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createStartConsumptionCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StartBootstrapCallable(currentNanos, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createStartDataEventSequenceCallable(long currentNanos, SCN scn,
                                              DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StartBootstrapEventSequenceCallable(currentNanos, scn, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
         createStartSourceCallable(long currentNanos, String source, Schema sourceSchema,
                                   DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new StartBootstrapSourceCallable(currentNanos, source, sourceSchema, consumer,stats);
  }

  @Override
  public ConsumerCallable<ConsumerCallbackResult>
          createOnErrorCallable(long currentNanos, Throwable err, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    return new OnBootstrapErrorCallable(currentNanos, err, consumer,stats);
  }

}

class OnBootstrapErrorCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final Throwable _err;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public OnBootstrapErrorCallable(long currentNanos, Throwable err,
                                  DatabusCombinedConsumer consumer,
                                  ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _err = err;
    _consumer = consumer;
    _consumerStats=stats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onBootstrapError(_err);
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

class BootstrapCheckpointCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public BootstrapCheckpointCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onBootstrapCheckpoint(_scn);
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

class BootstrapDataEventCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DbusEvent _event;
  private final DbusEventDecoder _eventDecoder;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;

  public BootstrapDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
          DatabusCombinedConsumer consumer)
  {
	  this(currentNanos,e,eventDecoder,consumer,null);
  }

  public BootstrapDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
                                    DatabusCombinedConsumer consumer,ConsumerCallbackStats consumerStats)
  {
    super(currentNanos);
    if (!(e instanceof DbusEventInternalWritable)) {
      throw new UnsupportedClassVersionError("Cannot support cloning on non-Dbusevent");
    }
    _event = ((DbusEventInternalWritable)e).clone(null);
    _eventDecoder = eventDecoder;
    _consumer = consumer;
    _consumerStats = consumerStats;
  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onBootstrapEvent(_event, _eventDecoder);
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
 			 _consumerStats.registerDataEventsProcessed(1, totalTime,_event);
 		 }
 	 }
  }

}

class StopBootstrapCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public StopBootstrapCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStopBootstrap();
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

class EndBootstrapEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public EndBootstrapEventSequenceCallable(long currentNanos, SCN scn,
                                           DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onEndBootstrapSequence(_scn);
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

class EndBootstrapSourceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final String _source;
  private final Schema _sourceSchema;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public EndBootstrapSourceCallable(long currentNanos, String source, Schema sourceSchema,
                                    DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _source = source;
    _sourceSchema = sourceSchema;
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onEndBootstrapSource(_source, _sourceSchema);
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

class BootstrapRollbackCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public BootstrapRollbackCallable(long currentNanos, SCN scn, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onBootstrapRollback(_scn);
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

class StartBootstrapCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public StartBootstrapCallable(long currentNanos, DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartBootstrap();
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

class StartBootstrapEventSequenceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final SCN _scn;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public StartBootstrapEventSequenceCallable(long currentNanos, SCN scn,
                                             DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _scn = scn;
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartBootstrapSequence(_scn);
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

class StartBootstrapSourceCallable extends ConsumerCallable<ConsumerCallbackResult>
{
  private final String _source;
  private final Schema _sourceSchema;
  private final DatabusCombinedConsumer _consumer;
  private final ConsumerCallbackStats _consumerStats;


  public StartBootstrapSourceCallable(long currentNanos, String source, Schema sourceSchema,
                                      DatabusCombinedConsumer consumer,ConsumerCallbackStats stats)
  {
    super(currentNanos);
    _source = source;
    _sourceSchema = sourceSchema;
    _consumer = consumer;
    _consumerStats=stats;

  }

  @Override
  protected ConsumerCallbackResult doCall() throws Exception
  {
    return _consumer.onStartBootstrapSource(_source, _sourceSchema);
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
