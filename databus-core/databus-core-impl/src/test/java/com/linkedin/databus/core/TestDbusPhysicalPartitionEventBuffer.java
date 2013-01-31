package com.linkedin.databus.core;
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


import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;


/*
 * Physical partition configuration can have a event buffer configuration, this class checks if the event buffer configuration is
 * being honored per physical partition
 */
public class TestDbusPhysicalPartitionEventBuffer  {

  PhysicalSourceConfig PhysicalSourcesConfigWithEBuffer;
  PhysicalSourceStaticConfig PhysicalSourcesStaticConfigWithEBuffer;

  PhysicalSourceConfig PhysicalSourcesConfigWithoutEBuffer;
  PhysicalSourceStaticConfig PhysicalSourcesStaticConfigWithoutEbuffer;

  DbusEventBuffer.StaticConfig GlobalDbusEbufferStaticConfig;

  private final static int eventBufferMaxSize = 141234991;
  private final static int scnIndexSize = 192412342;
  private final static int eventBufferReadBufferSize = 92931;
  private final static int globaleventBufferMaxSize = 123456;


  @BeforeClass
  public void setUp() throws InvalidConfigException {

    LogicalSourceConfig loConfig = new LogicalSourceConfig();
    loConfig.setId((short)0);
    loConfig.setName("testlogicalName");
    loConfig.setPartition((short) 0);
    loConfig.setUri("testuri");
    loConfig.setPartitionFunction("constant:1");

    //Physical sources config without event buffer
    PhysicalSourceConfig config = new PhysicalSourceConfig();
    config.setName("testname");
    config.setUri("testuri");
    config.setSource(0,loConfig);
    PhysicalSourcesStaticConfigWithoutEbuffer = config.build();
    PhysicalSourcesConfigWithoutEBuffer = config;

    //Physical sources config with event buffer
    PhysicalSourceConfig config2 = new PhysicalSourceConfig();
    config2.setName("testname");
    config2.setUri("testuri");
    config2.setSource(0,loConfig);
    config2.setName("testname");
    config2.setUri("testuri");
    config2.getDbusEventBuffer().setMaxSize(eventBufferMaxSize);
    config2.getDbusEventBuffer().setScnIndexSize(scnIndexSize);
    config2.getDbusEventBuffer().setReadBufferSize(eventBufferReadBufferSize);
    PhysicalSourcesStaticConfigWithEBuffer = config2.build();
    PhysicalSourcesConfigWithEBuffer = config2;

    //Global event buffer config
    DbusEventBuffer.Config ebufConfig = new DbusEventBuffer.Config();
    ebufConfig.setMaxSize(globaleventBufferMaxSize);
    GlobalDbusEbufferStaticConfig = ebufConfig.build();
  }

  @Test
  public void testBufferSize()
  {
    Assert.assertEquals(true, PhysicalSourcesStaticConfigWithEBuffer.isDbusEventBufferSet());
    Assert.assertEquals(PhysicalSourcesStaticConfigWithEBuffer.getDbusEventBuffer().getMaxSize(), eventBufferMaxSize);
  }

  @Test
  public void addNewBufferTestPhysicalSourceConfig() throws InvalidConfigException
  {
    PhysicalSourceStaticConfig[] physicalSourcesConfig = new PhysicalSourceStaticConfig[1];
    physicalSourcesConfig[0] = PhysicalSourcesStaticConfigWithEBuffer;
    DbusEventBufferMult multBuf = new DbusEventBufferMult(physicalSourcesConfig,GlobalDbusEbufferStaticConfig);

    //Verify if it addNewBuffer has honored the physicalSourcesConfig event buffer
    DbusEventBuffer buffer = multBuf.getOneBuffer(PhysicalSourcesStaticConfigWithEBuffer.getPhysicalPartition());
    Assert.assertEquals(PhysicalSourcesStaticConfigWithEBuffer.isDbusEventBufferSet(),true);
    Assert.assertEquals(PhysicalSourcesStaticConfigWithEBuffer.getDbusEventBuffer().getMaxSize(), eventBufferMaxSize);
    Assert.assertEquals(buffer.getAllocatedSize(), eventBufferMaxSize);
  }

  @Test
  public void addNewBufferTestGlobalEventBufferConfig() throws InvalidConfigException
  {
    PhysicalSourceStaticConfig[] physicalSourcesConfig = new PhysicalSourceStaticConfig[1];
    physicalSourcesConfig[0] = PhysicalSourcesStaticConfigWithoutEbuffer;
    DbusEventBufferMult multBuf = new DbusEventBufferMult(physicalSourcesConfig,GlobalDbusEbufferStaticConfig);

    //Verify if it addNewBuffer has honored the globaleventbuffer config
    DbusEventBuffer buffer = multBuf.getOneBuffer(PhysicalSourcesStaticConfigWithoutEbuffer.getPhysicalPartition());
    Assert.assertEquals(PhysicalSourcesStaticConfigWithoutEbuffer.isDbusEventBufferSet(),false);
    Assert.assertEquals(buffer.getAllocatedSize(), globaleventBufferMaxSize);
  }

  /*
   *  The toString method of the physical sources config has been overriden to avoid calling getDbusEventBuffer, this
   *  method will verify if the serialization still works
   */
  @Test
  public void serializePhysicalSourceConfigWithNullEBuffer() throws JSONException
  {
      JSONObject jsonObject = new JSONObject(PhysicalSourcesConfigWithoutEBuffer.toString());
      //System.out.println(PhysicalSourcesConfigWithoutEBuffer.toString());
      Assert.assertEquals(jsonObject.get("dbusEventBuffer"), JSONObject.NULL);
  }

  @Test
  public void serializePhysicalSourceConfig() throws JSONException
  {
    JSONObject jsonObject = new JSONObject(PhysicalSourcesConfigWithEBuffer.toString());
    Assert.assertNotEquals(jsonObject.get("dbusEventBuffer"), null);
  }


  @Test
  public void deserializePhysicalSourceConfigWithEbuffer() throws JSONException, JsonParseException, JsonMappingException, IOException
  {
    JSONObject jsonObject = new JSONObject(PhysicalSourcesConfigWithEBuffer.toString());
    Assert.assertNotEquals(jsonObject.get("dbusEventBuffer"), JSONObject.NULL);
    ObjectMapper mapper = new ObjectMapper();
    PhysicalSourceConfig config = mapper.readValue(jsonObject.toString(), PhysicalSourceConfig.class);
    //System.out.println(config.toString());
    Assert.assertEquals(config.isDbusEventBufferSet(), true);
  }

  @Test
  public void deserializePhysicalSourceConfigWithoutEbuffer() throws JSONException, JsonParseException, JsonMappingException, IOException
  {
    JSONObject jsonObject = new JSONObject(PhysicalSourcesConfigWithoutEBuffer.toString());
    Assert.assertEquals(jsonObject.get("dbusEventBuffer"), JSONObject.NULL);
    ObjectMapper mapper = new ObjectMapper();
    PhysicalSourceConfig config = mapper.readValue(jsonObject.toString(), PhysicalSourceConfig.class);
    Assert.assertEquals(config.isDbusEventBufferSet(), false);
  }
}
