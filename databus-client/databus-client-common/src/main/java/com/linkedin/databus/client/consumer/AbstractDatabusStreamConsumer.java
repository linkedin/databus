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
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;

/**
 * Provides default implementations for all {@link DatabusStreamConsumer} methods. Real
 * implementations can override only the methods they care about
 * @author cbotev
 *
 */
public abstract class AbstractDatabusStreamConsumer implements DatabusStreamConsumer
{
  private final ConsumerCallbackResult _defaultAnswer;

  protected AbstractDatabusStreamConsumer(ConsumerCallbackResult defaultAnswer)
  {
    _defaultAnswer = defaultAnswer;
  }

  protected AbstractDatabusStreamConsumer()
  {
    this(ConsumerCallbackResult.SUCCESS);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return _defaultAnswer;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return _defaultAnswer;
  }
}
