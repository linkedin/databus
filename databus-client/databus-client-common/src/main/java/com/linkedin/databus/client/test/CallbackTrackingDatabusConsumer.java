package com.linkedin.databus.client.test;
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


import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.DelegatingDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;

public class CallbackTrackingDatabusConsumer extends DelegatingDatabusCombinedConsumer
{
  final List<String> _callbacks = new ArrayList<String>();
  String _lastCallback;

  public CallbackTrackingDatabusConsumer(DatabusCombinedConsumer delegate, Logger log)
  {

    super(delegate, log);
  }

  public List<String> getCallbacks()
  {
    return _callbacks;
  }

  public String getLastCallback()
  {
    return _lastCallback;
  }

  public void resetCallbacks()
  {
    _callbacks.clear();
    _lastCallback = null;
  }

  void registerCallback(String cb)
  {
    _lastCallback = cb;
    _callbacks.add(cb);
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    registerCallback("onStartConsumption()");
    return super.onStartConsumption();
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    registerCallback("onStopConsumption()");
    return super.onStopConsumption();
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    registerCallback("onStartDataEventSequence(" + startScn + ")");
    return super.onStartDataEventSequence(startScn);
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    registerCallback("onEndDataEventSequence(" + endScn + ")");
    return super.onEndDataEventSequence(endScn);
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    registerCallback("onRollback(" + rollbackScn + ")");
    return super.onRollback(rollbackScn);
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    registerCallback("onStartSource(" + source + ")");
    return super.onStartSource(source, sourceSchema);
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    registerCallback("onEndSource(" + source + ")");
    return super.onEndSource(source, sourceSchema);
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    registerCallback("onDataEvent(" + e + ")");
    return super.onDataEvent(e, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    registerCallback("onCheckpoint(" + checkpointScn + ")");
    return super.onCheckpoint(checkpointScn);
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    registerCallback("onError(" + err.getClass().getName() +
                     ":" + err.getMessage() +
                     ")");
    return super.onError(err);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    registerCallback("onStartBootstrap()");
    return super.onStartBootstrap();
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    registerCallback("onStopBootstrap()");
    return super.onStopBootstrap();
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    registerCallback("onStartBootstrapSequence(" + startScn + ")");
    return super.onStartBootstrapSequence(startScn);
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    registerCallback("onEndBootstrapSequence(" + endScn + ")");
    return super.onEndBootstrapSequence(endScn);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String source, Schema sourceSchema)
  {
    registerCallback("onStartBootstrapSource(" + source + ")");
    return super.onStartBootstrapSource(source, sourceSchema);
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String source, Schema sourceSchema)
  {
    registerCallback("onEndBootstrapSource(" + source + ")");
    return super.onEndBootstrapSource(source, sourceSchema);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                 DbusEventDecoder eventDecoder)
  {
    registerCallback("onBootstrapEvent(" + e + ")");
    return super.onBootstrapEvent(e, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN rollbackScn)
  {
    registerCallback("onBootstrapRollback(" + rollbackScn + ")");
    return super.onBootstrapRollback(rollbackScn);
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
  {
    registerCallback("onBootstrapCheckpoint(" + checkpointScn + ")");
    return super.onBootstrapCheckpoint(checkpointScn);
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    registerCallback("onBootstrapError("  + err.getClass().getName() +
                     ":" + err.getMessage() + ")");
    return super.onBootstrapError(err);
  }
}
