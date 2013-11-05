package com.linkedin.databus.container.netty;
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


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.container.request.GenerateDataEventsRequestProcessor;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.test.netty.SimpleHttpResponseHandler;
import com.linkedin.databus.core.test.netty.SimpleTestHttpClient;
import com.linkedin.databus.core.test.netty.SimpleTestHttpClient.TimeoutPolicy;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.netty.HttpServerPipelineFactory;
import com.linkedin.databus2.core.container.request.EchoRequestProcessor;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig.RegistryType;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSetBackedRegistryService;

@Test(singleThreaded=true)
public class TestRelayCommandsLocal
{

  static
  {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  public static final String MODULE = TestRelayCommandsLocal.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private HttpRelay _relay;
  private final HttpRelay.Config _staticConfigBuilder;
  private final HttpRelay.StaticConfig _staticConfig;
  private VersionedSchemaSetBackedRegistryService _schemaRegistry;
  private DbusEventBufferMult _eventBuffer;
  private LocalAddress _serverAddress;
  private Channel _serverChannel;
  private ServerBootstrap _bootstrap;
  private ExecutorService _executor = Executors.newCachedThreadPool();

  public TestRelayCommandsLocal() throws IOException, InvalidConfigException, DatabusException
  {
    _staticConfigBuilder = new HttpRelay.Config();

    //ConfigLoader<HttpRelay.StaticConfig> configLoader =
        new ConfigLoader<HttpRelay.StaticConfig>("databus.relay.", _staticConfigBuilder);

    //configLoader.loadConfig(System.getProperties());
    _staticConfigBuilder.getContainer().setExistingMbeanServer(null);

    _staticConfigBuilder.getSchemaRegistry().getFileSystem().setRefreshPeriodMs(0);

    DbusEventBuffer.Config eventBufferConfig = _staticConfigBuilder.getEventBuffer();
    eventBufferConfig.setAllocationPolicy("HEAP_MEMORY");
    eventBufferConfig.setMaxSize(15000);
    eventBufferConfig.setAverageEventSize(10000);
    eventBufferConfig.setScnIndexSize(10000);

    _staticConfigBuilder.setSourceName("100", "commonsource1");
    _staticConfigBuilder.setSourceName("101", "commonsource1");
    _staticConfigBuilder.setSourceName("1001", "src11");
    _staticConfigBuilder.setSourceName("2001", "src21");
    _staticConfigBuilder.setSourceName("2002", "src22");
    _staticConfigBuilder.setSourceName("3001", "test3.source1");
    _staticConfigBuilder.setSourceName("3002", "test3.source2");
    _staticConfigBuilder.setSourceName("4001", "test4.source1");
    _staticConfigBuilder.setSourceName("4002", "test4.source2");
    _staticConfigBuilder.setSourceName("5001", "test5.source1");
    _staticConfigBuilder.setSourceName("5002", "test5.source2");

    _schemaRegistry = new VersionedSchemaSetBackedRegistryService();

    String schema31Str = "{\"name\":\"source1_v1\",\"namespace\":\"test3\",\"type\":\"record\",\"fields\":[{\"type\":\"int\",\"name\":\"intField\"}]}";
    VersionedSchema vschema31 = new VersionedSchema("test3.source1", (short)1, Schema.parse(schema31Str), null);
    String schema32Str = "{\"name\":\"source2_v1\",\"namespace\":\"test3\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
    VersionedSchema vschema32 = new VersionedSchema("test3.source2", (short)1, Schema.parse(schema32Str), null);
    _schemaRegistry.registerSchema(vschema31);
    _schemaRegistry.registerSchema(vschema32);

    String schema41Str = "{\"name\":\"source1_v1\",\"namespace\":\"test4\",\"type\":\"record\",\"fields\":[{\"type\":\"int\",\"name\":\"intField\"}]}";
    VersionedSchema vschema41 = new VersionedSchema("test4.source1", (short)1, Schema.parse(schema41Str), null);
    String schema42Str = "{\"name\":\"source2_v1\",\"namespace\":\"test4\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
    VersionedSchema vschema42 = new VersionedSchema("test4.source2", (short)1, Schema.parse(schema42Str), null);
    _schemaRegistry.registerSchema(vschema41);
    _schemaRegistry.registerSchema(vschema42);

    String schema51Str = "{\"name\":\"source1_v1\",\"namespace\":\"test5\",\"type\":\"record\",\"fields\":[{\"type\":\"int\",\"name\":\"intField\"}]}";
    VersionedSchema vschema51 = new VersionedSchema("test5.source1", (short)1, Schema.parse(schema51Str), null);
    //String schema52Str = "{\"name\":\"source2_v1\",\"namespace\":\"test5\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
    VersionedSchema vschema52 = new VersionedSchema("test5.source2", (short)1, Schema.parse(schema51Str), null);
    _schemaRegistry.registerSchema(vschema51);
    _schemaRegistry.registerSchema(vschema52);

    // See https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Espresso+Metadata+Schema for
    // the real definition.
    String metadataSchemaStr = "{\"name\":\"metadata\",\"namespace\":\"test_namespace\",\"type\":\"record\",\"fields\":[{\"name\":\"randomStrField\",\"type\":\"string\"}]}";
    // TODO (DDSDBUS-2093/2096):  update this according to real schema-registry metadata API.
    // (Right now the "metadata" string here is used as a key for lookups within the relay's
    // schema registry, but that could conflict with an actual source schema named "metadata".)
    VersionedSchema metaSchema = new VersionedSchema("metadata", (short)1, Schema.parse(metadataSchemaStr), null);
    _schemaRegistry.registerSchema(metaSchema);

    _staticConfigBuilder.getSchemaRegistry().setType(RegistryType.EXISTING.toString());
    _staticConfigBuilder.getSchemaRegistry().useExistingService(_schemaRegistry);
    _staticConfig = _staticConfigBuilder.build();
  }

  @BeforeMethod
  public void setUp() throws Exception
  {
    _relay = new HttpRelay(_staticConfig, null);  // creates an event factory for us

    _eventBuffer = _relay.getEventBuffer();

    DatabusEventProducer randomEventProducer = new DatabusEventRandomProducer(
        _eventBuffer, 10, 1, 10, _staticConfig.getSourceIds());

    RequestProcessorRegistry processorRegistry = _relay.getProcessorRegistry();
    processorRegistry.register(EchoRequestProcessor.COMMAND_NAME, new EchoRequestProcessor(null));
    processorRegistry.register(GenerateDataEventsRequestProcessor.COMMAND_NAME,
                               new GenerateDataEventsRequestProcessor(null, _relay,
                                                                      randomEventProducer));

    // Configure the server.
    _bootstrap = new ServerBootstrap(new DefaultLocalServerChannelFactory());
    // Set up the event pipeline factory.
    _bootstrap.setPipelineFactory(new HttpServerPipelineFactory(_relay));
    _serverAddress = new LocalAddress(10);
    _serverChannel = _bootstrap.bind(_serverAddress);
  }

  @AfterMethod
  public void tearDown()
  {
    _relay.doShutdown();

    if (null != _staticConfig)
    {
      _relay.getDefaultExecutorService().shutdownNow();
    }
    if (null != _bootstrap)
    {
      _serverChannel.close();
      _bootstrap.releaseExternalResources();
    }
  }

  @Test
  public void testSources1Command() throws Exception
  {
    LOG.debug("\n\nstarting testSources1Command()\n");


    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/sources");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    List<IdNamePair> res = objMapper.readValue(in, new TypeReference<List<IdNamePair>>(){});
    assertNotNull("no result", res);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/sources response:" + new String(respHandler.getReceivedBytes()));
    }

    HashSet<IdNamePair> origSet = new HashSet<IdNamePair>(_staticConfig.getSourceIds());
    HashSet<IdNamePair> resSet = new HashSet<IdNamePair>(res);
    Assert.assertEquals(origSet, resSet);
  }

  @Test
  public void testSourcesTrackingCommand() throws Exception
  {
    LOG.debug("\n\nstarting testSourcesTrackingCommand()\n");


    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/sources");
    httpRequest.setHeader(DatabusHttpHeaders.DBUS_CLIENT_HOST_HDR, "localhost");
    httpRequest.setHeader(DatabusHttpHeaders.DBUS_CLIENT_SERVICE_HDR, "unittestclient");
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response",
               respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    List<IdNamePair> res = objMapper.readValue(in, new TypeReference<List<IdNamePair>>(){});
    assertNotNull("no result", res);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/sources response:" + new String(respHandler.getReceivedBytes()));
    }

    HashSet<IdNamePair> origSet = new HashSet<IdNamePair>(_staticConfig.getSourceIds());
    HashSet<IdNamePair> resSet = new HashSet<IdNamePair>(res);
    Assert.assertEquals(origSet, resSet);
  }

  private void prepareTestSources2Command()
  {
  }

  private void doTestSources2Command() throws Exception
  {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/sources");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    List<IdNamePair> res = objMapper.readValue(in, new TypeReference<List<IdNamePair>>(){});
    assertNotNull("no result", res);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/sources response:" + new String(respHandler.getReceivedBytes()));
    }

    HashSet<IdNamePair> origSet = new HashSet<IdNamePair>(_staticConfig.getSourceIds());
    HashSet<IdNamePair> resSet = new HashSet<IdNamePair>(res);
    Assert.assertEquals(origSet, resSet);
  }

  @Test
  public void testSources2Command() throws Exception
  {
    LOG.debug("\n\nstarting testSources2Command()\n");

    prepareTestSources2Command();
    doTestSources2Command();
  }

  @Test
  public void testSources1CommandThreaded() throws Exception
  {
    LOG.debug("\n\nstarting testSources1CommandThreaded()\n");

    Future<?> future = _executor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        boolean ok = true;
        try
        {
          testSources1Command();
        }
        catch (Exception e)
        {
          ok = false;
        }
        assertTrue("unexpected /sources exception", ok);
      }
    });

    try
    {
    future.get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    assertTrue("/sources failed to complete", future.isDone());
  }

  @Test
  public void testSources2CommandThreaded() throws Exception
  {
    LOG.debug("\n\nstarting testSources2CommandThreaded()\n");

    prepareTestSources2Command();

    Runnable runTestSources2Command = new Runnable()
        {
          @Override
          public void run()
          {
            LOG.info("Start testSource2Command thread");
            boolean ok = true;
            try
            {
              doTestSources2Command();
            }
            catch (Exception e)
            {
              ok = false;
            }
            assertTrue("unexpected /sources exception", ok);
            LOG.info("Done testSource2Command thread");
          }
        };

    Future<?> future1 = _executor.submit(runTestSources2Command);
    Future<?> future2 = _executor.submit(runTestSources2Command);
    Future<?> future3 = _executor.submit(runTestSources2Command);

    try
    {
      future1.get(2, TimeUnit.SECONDS);
      future2.get(2, TimeUnit.SECONDS);
      future3.get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    assertTrue("expected /sources call 1 to complete", future1.isDone());
    assertTrue("expected /sources call 2 to complete", future1.isDone());
    assertTrue("expected /sources call 3 to complete", future1.isDone());
  }

  @Test
  public void testRegisterCommandOneSource() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterCommandOneSource()\n");

    // /register?sources=2
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/register?sources=3002");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    byte[] respBytes = respHandler.getReceivedBytes();
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response:" + new String(respBytes));
    }
    ByteArrayInputStream in = new ByteArrayInputStream(respBytes);
    ObjectMapper objMapper = new ObjectMapper();
    List<RegisterResponseEntry> res =
        objMapper.readValue(in, new TypeReference<List<RegisterResponseEntry>>(){});
    assertNotNull("no result", res);
    assertEquals("expected one source", 1, res.size());
    assertEquals("expected correct source id", 3002, res.get(0).getId());
    Schema resSchema = Schema.parse(res.get(0).getSchema());
    assertEquals("expected correct source schema", "test3.source2_v1", resSchema.getFullName());
  }

  private void prepareTestRegisterCommandTwoSources() throws Exception
  {
  }

  private void doTestRegisterCommandTwoSources() throws Exception
  {
    ObjectMapper objMapper = new ObjectMapper();

    // /register?sources=1,2
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.GET, "/register?sources=4001,4002");
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    List<RegisterResponseEntry> res =
        objMapper.readValue(in, new TypeReference<List<RegisterResponseEntry>>(){});
    assertNotNull("no result", res);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response:" + new String(respHandler.getReceivedBytes()));
    }
    assertEquals("expected two sources", 2, res.size());
    assertEquals("expected correct source id", 4001, res.get(0).getId());
    Schema resSchema = Schema.parse(res.get(0).getSchema());
    assertEquals("expected correct source schema", "test4.source1_v1", resSchema.getFullName());
    assertEquals("expected correct source id", 4002, res.get(1).getId());
    resSchema = Schema.parse(res.get(1).getSchema());
    assertEquals("expected correct source schema", "test4.source2_v1", resSchema.getFullName());
  }


  @Test
  public void testRegisterCommandTwoSources() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterCommandTwoSources()\n");

    prepareTestRegisterCommandTwoSources();
    doTestRegisterCommandTwoSources();
  }

  @Test
  public void testRegisterCommandThreeSources() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterCommandThreeSources()\n");

    // /register?sources=1,2,3
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.GET, "/register?sources=5001,5002,5003");
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);
    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response:" + new String(respHandler.getReceivedBytes()));
    }

    HttpResponse respObj = respHandler.getResponse();
    // Note that v3 client code doesn't currently (May 2013) support "sources" param for "/register".
    assertNotNull("exception class header not present",
                  respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    assertEquals("exception class name mismatch",
                 InvalidRequestParamValueException.class.getName(),
                 respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
  }

  @Test
  public void testRegisterCommandOneSourceThreaded() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterCommandOneSourceThreaded()\n");

    Future<?> future = _executor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        boolean ok = true;
        try
        {
          testRegisterCommandOneSource();
        }
        catch (Exception e)
        {
          ok = false;
        }
        assertTrue("unexpected /register exception", ok);
      }
    });

    try
    {
    future.get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    assertTrue("/register failed to complete", future.isDone());
  }

  @Test
  public void testRegisterCommandTwoSourcesThreaded() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterCommandTwoSourcesThreaded()\n");

    Runnable runRegisterCommandTwoSources = new Runnable()
        {
          @Override
          public void run()
          {
            boolean ok = true;
            try
            {
              doTestRegisterCommandTwoSources();
            }
            catch (Exception e)
            {
              ok = false;
            }
            assertTrue("unexpected /register exception", ok);
          }
        };

    Future<?>[] future = new Future<?>[4];

    for (int i = 0; i < 4; ++i) future[i] = _executor.submit(runRegisterCommandTwoSources);

    try
    {
      for (int i = 0; i < 4; ++i) future[i].get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie)
    {}
    for (int i = 0; i < 4; ++i)
    {
      assertTrue("/register thread " + (i+1) + " failed to complete", future[i].isDone());
    }
  }

  @Test
  public void testRegisterV4CommandHappyPath() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterV4CommandHappyPath()\n");

    // /register?protocolVersion=4
    HttpRequest httpRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                               HttpMethod.GET,
                               "/register?sources=4002&" + DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=4");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    HttpResponse respObj = respHandler.getResponse();
    assertNull("/register v4 returned unexpected error: " +
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER),
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    String registerResponseProtocolVersionStr =
        respObj.getHeader(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR);
    assertNotNull("/register protocol-version response header not present", registerResponseProtocolVersionStr);
    assertEquals("client-relay protocol response version mismatch", "4", registerResponseProtocolVersionStr);

    byte[] respBytes = respHandler.getReceivedBytes();
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response: " + new String(respBytes));
    }
    ByteArrayInputStream in = new ByteArrayInputStream(respBytes);
    ObjectMapper objMapper = new ObjectMapper();
    Map<String, List<Object>> resMap =
        objMapper.readValue(in, new TypeReference<Map<String, List<Object>>>(){});
    assertNotNull("no result", resMap);

    List<Object> sourceSchemasObjectsList = resMap.get(RegisterResponseEntry.SOURCE_SCHEMAS_KEY);
    assertNotNull("missing required sourceSchemas key in response", sourceSchemasObjectsList);
    assertEquals("expected one source schema", 1, sourceSchemasObjectsList.size());

    // ObjectMapper encodes plain Object as LinkedHashMap with (apparently) String keys and
    // either String or Integer values (currently).  We must construct RegisterResponseEntry
    // manually.
    assertTrue("sourceSchemas deserialization error: 'Object' type = " +
               sourceSchemasObjectsList.get(0).getClass().getName() + ", not LinkedHashMap",
               sourceSchemasObjectsList.get(0) instanceof LinkedHashMap);
    @SuppressWarnings("unchecked") // just obj
    LinkedHashMap<String, Object> obj = (LinkedHashMap<String, Object>)sourceSchemasObjectsList.get(0);
    assertTrue("sourceSchemas deserialization error: missing \"id\" key", obj.containsKey("id"));
    assertTrue("sourceSchemas deserialization error: missing \"version\" key", obj.containsKey("version"));
    assertTrue("sourceSchemas deserialization error: missing \"schema\" key", obj.containsKey("schema"));

    assertTrue("obj.get(\"id\") type = " + obj.get("id").getClass().getName() + ", not Integer",
               obj.get("id") instanceof Integer);
    assertTrue("obj.get(\"version\") type = " + obj.get("version").getClass().getName() + ", not Integer",
               obj.get("version") instanceof Integer);
    RegisterResponseEntry rre = new RegisterResponseEntry((Integer)obj.get("id"),
                                                          ((Integer)obj.get("version")).shortValue(),
                                                          (String)obj.get("schema"));
    assertEquals("unexpected source id", 4002, rre.getId());
    Schema resSchema = Schema.parse(rre.getSchema());
    assertEquals("unexpected source-schema name for source id 4002", "test4.source2_v1", resSchema.getFullName());

    // There's no guarantee of a metadataSchemas entry in general, but we pre-stuffed our
    // VersionedSchemaSetBackedRegistryService with one in the test's constructor, so we
    // expect the relay to hand it back to us.  Or else.
/* disabled for now since simplistic relay implementation has been disabled; reenable/update/modify as part of DDSDBUS-2093/2096 (TODO)
    List<Object> metadataSchemasObjectsList = resMap.get(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY);
    assertNotNull("missing expected metadataSchemas key in response", metadataSchemasObjectsList);
    assertEquals("expected one metadata schema", 1, metadataSchemasObjectsList.size());

    // As above, we must construct RegisterResponseMetadataEntry manually.
    assertTrue("metadataSchemas deserialization error: 'Object' type = " +
               metadataSchemasObjectsList.get(0).getClass().getName() + ", not LinkedHashMap",
               metadataSchemasObjectsList.get(0) instanceof LinkedHashMap);
    @SuppressWarnings("unchecked") // just obj2
    LinkedHashMap<String, Object> obj2 = (LinkedHashMap<String, Object>)metadataSchemasObjectsList.get(0);
    assertTrue("metadataSchemas deserialization error: missing \"version\" key", obj2.containsKey("version"));
    assertTrue("metadataSchemas deserialization error: missing \"schema\" key", obj2.containsKey("schema"));

    assertTrue("obj2.get(\"version\") type = " + obj2.get("version").getClass().getName() + ", not Integer",
               obj2.get("version") instanceof Integer);
    RegisterResponseMetadataEntry rrme = new RegisterResponseMetadataEntry(((Integer)obj2.get("version")).shortValue(),
                                                                          (String)obj2.get("schema"));
    assertEquals("unexpected metadata version", 1, rrme.getVersion());
    resSchema = Schema.parse(rrme.getSchema());
    assertEquals("unexpected metadata schema name", "test_namespace.metadata", resSchema.getFullName());
 */

    LOG.debug("\n\ndone with testRegisterV4CommandHappyPath()\n");
  }

  @Test
  public void testRegisterV4CommandLessThanHappyPaths() throws Exception
  {
    LOG.debug("\n\nstarting testRegisterV4CommandLessThanHappyPaths()\n");

    // protocolVersion < 2
    HttpRequest httpRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                               HttpMethod.GET,
                               "/register?sources=4002&" + DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=1");
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);
    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    HttpResponse respObj = respHandler.getResponse();
    assertNotNull("/register failed to return expected error",
                  respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    LOG.debug("DATABUS_ERROR_CLASS_HEADER = " + respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));

    // protocolVersion > 4
    httpRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                               HttpMethod.GET,
                               "/register?sources=4002&" + DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=5");
    respHandler = httpClient.sendRequest(_serverAddress, httpRequest);
    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    respObj = respHandler.getResponse();
    assertNotNull("/register failed to return expected error",
                  respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    LOG.debug("DATABUS_ERROR_CLASS_HEADER = " + respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));

    // protocolVersion == 2:  this is a happy path, but explicitly specifying the version is
    // unusual in this case (default = version 2 or 3, which are identical for /register), so
    // check for expected response
    httpRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                               HttpMethod.GET,
                               "/register?sources=4002&" + DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=2");
    respHandler = httpClient.sendRequest(_serverAddress, httpRequest);
    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    respObj = respHandler.getResponse();
    assertNull("/register v2 returned unexpected error: " +
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER),
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    LOG.debug("DATABUS_ERROR_CLASS_HEADER = " + respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    String registerResponseProtocolVersionStr =
        respObj.getHeader(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR);
    assertNotNull("/register protocol-version response header not present", registerResponseProtocolVersionStr);
    assertEquals("client-relay protocol response version mismatch", "2", registerResponseProtocolVersionStr);
    byte[] respBytes = respHandler.getReceivedBytes();
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response: " + new String(respBytes));
    }
    ByteArrayInputStream in = new ByteArrayInputStream(respBytes);
    ObjectMapper objMapper = new ObjectMapper();
    List<RegisterResponseEntry> sourceSchemasList = null;
    try
    {
      sourceSchemasList = objMapper.readValue(in, new TypeReference<List<RegisterResponseEntry>>(){});
    }
    catch (JsonMappingException jmex)
    {
      Assert.fail("ObjectMapper failed unexpectedly");
    }
    assertNotNull("missing source schemas in response", sourceSchemasList);
    assertEquals("expected one source schema", 1, sourceSchemasList.size());
    RegisterResponseEntry rre = sourceSchemasList.get(0);
    assertEquals("unexpected source id", 4002, rre.getId());
    Schema resSchema = Schema.parse(rre.getSchema());
    assertEquals("unexpected source-schema name for source id 4002", "test4.source2_v1", resSchema.getFullName());

    // protocolVersion == 3:  as with v2 above; just do a quick sanity check
    httpRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                               HttpMethod.GET,
                               "/register?sources=4002&" + DatabusHttpHeaders.PROTOCOL_VERSION_PARAM + "=3");
    respHandler = httpClient.sendRequest(_serverAddress, httpRequest);
    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    respObj = respHandler.getResponse();
    assertNull("/register v3 returned unexpected error: " +
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER),
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    LOG.debug("DATABUS_ERROR_CLASS_HEADER = " + respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    registerResponseProtocolVersionStr = respObj.getHeader(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR);
    assertNotNull("/register protocol-version response header not present", registerResponseProtocolVersionStr);
    assertEquals("client-relay protocol response version mismatch", "3", registerResponseProtocolVersionStr);
  }

  @Test
  public void testNoDataStreamCommand() throws Exception
  {
    LOG.debug("\n\nstarting testNoDataStreamCommand()\n");

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/stream?sources=100&size=1000&output=json&checkPoint={\"windowScn\":-1,\"windowOffset\":-1}");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    assertEquals("expected to get empty response", 0, respHandler.getReceivedBytes().length);

    HttpResponse respObj = respHandler.getResponse();
    assertNull("/stream returned unexpected error", respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    assertNull("/stream returned unexpected error with cause",
               respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_CLASS_HEADER));
  }

  @Test
  public void testNoDataStreamCommandThreaded() throws Exception
  {
    LOG.debug("\n\nstarting testNoDataStreamCommandThreaded()\n");

    int numThreads = 10;
    Runnable runRegisterCommandTwoSources = new Runnable()
        {
          @Override
          public void run()
          {
            LOG.info("Starting thread for /stream");
            boolean ok = true;
            try
            {
              doTestRegisterCommandTwoSources();
            }
            catch (Exception e)
            {
              ok = false;
            }
            assertTrue("unexpected /register exception", ok);
            LOG.info("Stopping thread for /stream");
          }
        };

    Future<?>[] future = new Future<?>[numThreads];

    for (int i = 0; i < numThreads; ++i) future[i] = _executor.submit(runRegisterCommandTwoSources);

    try
    {
      for (int i = 0; i < numThreads; ++i) future[i].get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    for (int i = 0; i < numThreads; ++i)
    {
      assertTrue("/register thread " + (i+1) + " failed to complete", future[i].isDone());
    }
  }

  private void prepareTestOneDataStreamCommand() throws Exception
  {
    //generate an event
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/genDataEvents/start?src_ids=100&fromScn=10&eventsPerSec=1&duration=10");
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    String respString = new String(respHandler.getReceivedBytes());
    LOG.debug("Response string:" + respString);

    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    Map<String,String> genRes = objMapper.readValue(in, new TypeReference<Map<String,String>>(){});
    HttpResponse respObj = respHandler.getResponse();
    assertNull("/genDataEvents returned unexpected error", respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    assertEquals("event-generation failed to start", "true", genRes.get("genDataEventsStarted"));

    try {Thread.sleep(2000);} catch (InterruptedException ie) {}
  }

  private void doTestOneDataStreamCommand() throws Exception
  {
    //try to read it
    Checkpoint cp = Checkpoint.createFlexibleCheckpoint();

    String streamRequest = "/stream?sources=100&size=100000&output=json&checkPoint=" + cp.toString();

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, streamRequest);

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    HttpResponse respObj = respHandler.getResponse();
    assertNull("/stream returned unexpected error", respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));

    if (LOG.isDebugEnabled())
    {
      LOG.debug("/stream response:" + new String(respHandler.getReceivedBytes()));
    }

    ObjectMapper objMapper = new ObjectMapper();
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    objMapper.readValue(in, new TypeReference<Map<String,String>>(){});
  }

  /** Validates the version checks in the stream calls. */
  private void doTestOneDataClientVerStreamCommand(int ver, boolean expectFail) throws Exception
  {
    //try to read it
    Checkpoint cp = Checkpoint.createFlexibleCheckpoint();

    String maxev = "&" + DatabusHttpHeaders.MAX_EVENT_VERSION + "=" + ver;
    // protocol version 2 (versions >= 3 use "subs=")
    String streamRequest = "/stream?sources=100&size=100000&output=json&checkPoint=" + cp.toString() + maxev;

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, streamRequest);

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("failed to get a response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    HttpResponse respObj = respHandler.getResponse();
    if (expectFail)
    {
      assertNotNull("/stream failed to return expected error",
                    respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    }
    else
    {
      assertNull("/stream returned unexpected error",
                 respObj.getHeader(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER));
    }

    if (LOG.isDebugEnabled())
    {
      LOG.debug("/stream response:" + new String(respHandler.getReceivedBytes()));
    }

    ObjectMapper objMapper = new ObjectMapper();
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    objMapper.readValue(in, new TypeReference<Map<String,String>>(){});
  }

  @Test
  public void testOneDataStreamCommand() throws Exception
  {
    LOG.debug("\n\nstarting testOneDataStreamCommand()\n");

    prepareTestOneDataStreamCommand();
    doTestOneDataStreamCommand();
  }

  @Test
  // test relay handling of max event version
  public void testOneDataClientVerStreamCommand() throws Exception
  {
    LOG.debug("\n\nstarting testOneDataClientVerStreamCommand()\n");

    prepareTestOneDataStreamCommand();
    doTestOneDataClientVerStreamCommand(0, false);

    // version "1" doesn't exist (V1 == 0), and the code strictly enforces it now:
    doTestOneDataClientVerStreamCommand(1, true);

    doTestOneDataClientVerStreamCommand(2, false);

    // version of the client is smaller than the version of the events
    doTestOneDataClientVerStreamCommand(-1, true);
  }

  @Test
  public void testOneDataStreamCommandThreaded() throws Exception
  {
    LOG.debug("\n\nstarting testOneDataStreamCommandThreaded()\n");

    prepareTestOneDataStreamCommand();

    int numThreads = 10;
    Runnable runOneDataStreamCommand = new Runnable()
        {
          @Override
          public void run()
          {
            LOG.info("Starting thread for /stream");
            boolean ok = true;
            try
            {
              doTestOneDataStreamCommand();
            }
            catch (Exception e)
            {
              ok = false;
            }
            assertTrue("unexpected /stream exception", ok);
            LOG.info("Stopping thread for /stream");
          }
        };

    Future<?>[] future = new Future<?>[numThreads];

    for (int i = 0; i < numThreads; ++i) future[i] = _executor.submit(runOneDataStreamCommand);

    try
    {
      for (int i = 0; i < numThreads; ++i) future[i].get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    for (int i = 0; i < numThreads; ++i)
    {
      assertTrue("/register thread " + (i+1) + " failed to complete", future[i].isDone());
    }
  }

}
