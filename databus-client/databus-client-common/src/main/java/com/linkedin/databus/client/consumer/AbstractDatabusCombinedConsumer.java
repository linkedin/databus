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
import com.linkedin.databus.core.DbusEvent;

/** A class that provides default NO-OP implementations for the @{link {@link DatabusCombinedConsumer}
 * interface */
public abstract class AbstractDatabusCombinedConsumer implements DatabusCombinedConsumer
{
  private final ConsumerCallbackResult _defaultStreamAnswer;
  private final ConsumerCallbackResult _defaultBootstrapAnswer;

  protected AbstractDatabusCombinedConsumer(ConsumerCallbackResult defaultStreamAnswer,
                                            ConsumerCallbackResult defaultBootstrapAnswer)
  {
    _defaultStreamAnswer = defaultStreamAnswer;
    _defaultBootstrapAnswer = defaultBootstrapAnswer;
  }

  protected AbstractDatabusCombinedConsumer()
  {
    this(ConsumerCallbackResult.SUCCESS, ConsumerCallbackResult.SUCCESS);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return _defaultStreamAnswer;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return _defaultStreamAnswer;
  }


  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    return _defaultBootstrapAnswer;
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    return _defaultBootstrapAnswer;
  }
    
  @Override
  public boolean canBootstrap() 
  {
    return true;
  }

}
