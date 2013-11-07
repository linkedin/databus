package com.linkedin.databus2.core.container.request;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.container.netty.ServerContainer;

public class TestDatabusRequest
{
  private static final ObjectMapper _objectMapper = new ObjectMapper();
  private final String reqName = "RequestUnderTest";
  private final HttpMethod method = HttpMethod.GET;
  private final String prop1 = "a";
  private final String prop2 = "b";
  private final String propval1 = "aval";
  private final String propval2 = "bval";
  private final byte[] ipAddr = {0x01, 0x02, 0x03, 0x04};
  final int port = 666;

  private DatabusRequest makeRequest() throws Exception
  {
    SocketAddress sockAddr = new InetSocketAddress(InetAddress.getByAddress(ipAddr), port);
    Properties props = new Properties();
    props.setProperty(prop1, propval1);
    props.setProperty(prop2, propval2);
    ServerContainer.ExecutorConfig defaultConfig = new ServerContainer.ExecutorConfig(3, 2, 100, false, 5);
    ServerContainer.ExecutorConfig ioConfig = new ServerContainer.ExecutorConfig(5, 4, 50, true, 6);
    ServerContainer.RuntimeConfig runtimeConfig = new ServerContainer.RuntimeConfig(200, defaultConfig, ioConfig);

    return new DatabusRequest(reqName, method, sockAddr, props, runtimeConfig);
  }

  @Test
  public void testToString() throws Exception
  {
    DatabusRequest req = makeRequest();

    // Coverage of toString().
    String jsonStr = req.toString();
    Map<String, String> jsonMap = _objectMapper.readValue(jsonStr.getBytes(), new TypeReference<Map<String, String>>() {});
    Assert.assertEquals(reqName, jsonMap.get("name"));
    Assert.assertEquals(method.toString(), jsonMap.get("method"));
    Assert.assertEquals(propval1, jsonMap.get(prop1));
    Assert.assertEquals(propval2, jsonMap.get(prop2));
    Assert.assertEquals(4, jsonMap.size());

    // Coverage for null processor.
    Assert.assertEquals(req, req.call());

    // Coverqage for setCursorPartition
    PhysicalPartition pp = new PhysicalPartition(15, "SomePartition");
    req.setCursorPartition(pp);
    Assert.assertEquals(pp, req.getCursorPartition());

    Assert.assertFalse(req.cancel(true));
    Assert.assertFalse(req.cancel(false));
    Assert.assertEquals(req, req.get());
    Assert.assertEquals(req, req.get(89L, TimeUnit.HOURS));
    Assert.assertFalse(req.isCancelled());

    InetSocketAddress remoteAddress = (InetSocketAddress)req.getRemoteAddress();
    Assert.assertEquals(port, remoteAddress.getPort());
    Assert.assertEquals(InetAddress.getByAddress(ipAddr), remoteAddress.getAddress());
  }

  @Test
  public void testParamsNegative() throws Exception
  {
    DatabusRequest req = makeRequest();
    // Non-existent parameter
    boolean caughtException = false;
    int x = -1;
    try
    {
      x = req.getRequiredIntParam("NonExistentParam");
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    // Existing parameter, but non-numeric.
    caughtException = false;
    try
    {
      x = req.getRequiredIntParam(prop1);
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    final int defaultInt = 23;
    x = req.getOptionalIntParam("NonExistingParam", defaultInt);
    Assert.assertEquals(defaultInt, x);

    x = -1;
    caughtException = false;
    try
    {
      x = req.getOptionalIntParam(prop1, defaultInt);
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }
    Assert.assertEquals(-1, x);
    Assert.assertTrue(caughtException);

    long y = -1;
    caughtException = false;
    try
    {
      y = req.getRequiredLongParam("NonExistentParam");
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }
    Assert.assertEquals(-1, y);
    Assert.assertTrue(caughtException);

    y = -1;
    caughtException = false;
    try
    {
      y = req.getRequiredLongParam(prop1);
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }
    Assert.assertEquals(-1, y);
    Assert.assertTrue(caughtException);

    y = -1;
    final long defaultLong = 78;
    y = req.getOptionalLongParam("NoParam", defaultLong);
    Assert.assertEquals(defaultLong, y);

    y = -1;
    caughtException = false;
    try
    {
      y = req.getOptionalLongParam(prop1, defaultLong);
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
    Assert.assertEquals(-1, y);

    caughtException = false;
    try
    {
      req.getRequiredStringParam("NoParam");
    }
    catch(InvalidRequestParamValueException e)
    {
      caughtException = true;
    }

    Assert.assertTrue(caughtException);
  }
}
