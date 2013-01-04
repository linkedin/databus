package com.linkedin.databus.client.consumer;

import org.apache.avro.Schema;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.core.DbusEvent;

public interface ConsumerCallbackFactory<C>
{
  ConsumerCallable<ConsumerCallbackResult> createStartConsumptionCallable(long currentNanos, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createStartDataEventSequenceCallable(long currentNanos, SCN scn, C consumer, ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createStartSourceCallable(long currentNanos, String source, Schema sourceSchema, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createDataEventCallable(long currentNanos, DbusEvent e, DbusEventDecoder eventDecoder,
                                            C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createEndSourceCallable(long currentNanos, String source, Schema sourceSchema, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createEndDataEventSequenceCallable(long currentNanos, SCN scn, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createCheckpointCallable(long currentNanos, SCN scn, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createRollbackCallable(long currentNanos, SCN scn, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createEndConsumptionCallable(long currentNanos, C consumer,ConsumerCallbackStats stats);
  ConsumerCallable<ConsumerCallbackResult> createOnErrorCallable(long currentNanos, Throwable err, C consumer,ConsumerCallbackStats stats);
}
