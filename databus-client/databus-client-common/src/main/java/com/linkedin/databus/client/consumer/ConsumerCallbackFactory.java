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
