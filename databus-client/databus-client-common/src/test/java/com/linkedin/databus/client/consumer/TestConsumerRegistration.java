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

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;

public class TestConsumerRegistration
{

  @Test
  public void testSingleConsumer() throws Exception
  {
    DatabusCombinedConsumer logConsumer = new LoggingConsumer();
    List<String> sources = new ArrayList<String>();
    ConsumerRegistration consumerReg = new ConsumerRegistration(logConsumer, sources, null);

    Assert.assertEquals(logConsumer, consumerReg.getConsumer());
    return;
  }

  @Test
  public void testMultipleConsumers() throws Exception
  {
    DatabusCombinedConsumer logConsumer1 = new LoggingConsumer();
    DatabusCombinedConsumer logConsumer2 = new LoggingConsumer();
    List<DatabusCombinedConsumer> lcs = new ArrayList<DatabusCombinedConsumer>();
    lcs.add(logConsumer1);
    lcs.add(logConsumer2);

    List<String> sources = new ArrayList<String>();
    ConsumerRegistration consumerReg = new ConsumerRegistration(lcs, sources, null);

    DatabusStreamConsumer cons = consumerReg.getConsumer();
    boolean condition = logConsumer1.equals(cons) || logConsumer2.equals(cons);
    Assert.assertEquals(condition, true);
    return;
  }

}
