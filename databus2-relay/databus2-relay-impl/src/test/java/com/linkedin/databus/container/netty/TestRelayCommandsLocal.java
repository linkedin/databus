package com.linkedin.databus.container.netty;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
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
import com.linkedin.databus2.core.container.netty.HttpServerPipelineFactory;
import com.linkedin.databus2.core.container.request.DatabusRequest;
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
    eventBufferConfig.setReadBufferSize(10000);
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
    VersionedSchema vschema31 = new VersionedSchema("test3.source1", (short)1, Schema.parse(schema31Str));
    String schema32Str = "{\"name\":\"source2_v1\",\"namespace\":\"test3\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
    VersionedSchema vschema32 = new VersionedSchema("test3.source2", (short)1, Schema.parse(schema32Str));
    _schemaRegistry.registerSchema(vschema31);
    _schemaRegistry.registerSchema(vschema32);

    String schema41Str = "{\"name\":\"source1_v1\",\"namespace\":\"test4\",\"type\":\"record\",\"fields\":[{\"type\":\"int\",\"name\":\"intField\"}]}";
    VersionedSchema vschema41 = new VersionedSchema("test4.source1", (short)1, Schema.parse(schema41Str));
    String schema42Str = "{\"name\":\"source2_v1\",\"namespace\":\"test4\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
    VersionedSchema vschema42 = new VersionedSchema("test4.source2", (short)1, Schema.parse(schema42Str));
    _schemaRegistry.registerSchema(vschema41);
    _schemaRegistry.registerSchema(vschema42);

    String schema51Str = "{\"name\":\"source1_v1\",\"namespace\":\"test5\",\"type\":\"record\",\"fields\":[{\"type\":\"int\",\"name\":\"intField\"}]}";
    VersionedSchema vschema51 = new VersionedSchema("test5.source1", (short)1, Schema.parse(schema51Str));
    //String schema52Str = "{\"name\":\"source2_v1\",\"namespace\":\"test5\",\"type\":\"record\",\"fields\":[{\"type\":\"string\",\"name\":\"strField\"}]}";
    VersionedSchema vschema52 = new VersionedSchema("test5.source2", (short)1, Schema.parse(schema51Str));
    _schemaRegistry.registerSchema(vschema51);
    _schemaRegistry.registerSchema(vschema52);

    _staticConfigBuilder.getSchemaRegistry().setType(RegistryType.EXISTING.toString());
    _staticConfigBuilder.getSchemaRegistry().useExistingService(_schemaRegistry);
    _staticConfig = _staticConfigBuilder.build();
  }

  @BeforeMethod
  public void setUp() throws Exception
  {

    _relay = new HttpRelay(_staticConfig, null);

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

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/sources");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    List<IdNamePair> res = objMapper.readValue(in, new TypeReference<List<IdNamePair>>(){});
    assertNotNull("has result", res);
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

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    List<IdNamePair> res = objMapper.readValue(in, new TypeReference<List<IdNamePair>>(){});
    assertNotNull("has result", res);
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
    prepareTestSources2Command();
    doTestSources2Command();
  }

  @Test
  public void testSources1CommandThreaded() throws Exception
  {
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
        assertTrue("no /sources exception", ok);
      }
    });

    try
    {
    future.get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    assertTrue("/sources completed", future.isDone());
  }

  @Test
  public void testSources2CommandThreaded() throws Exception
  {
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
            assertTrue("no /sources exception", ok);
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
    assertTrue("/sources call 1 completed", future1.isDone());
    assertTrue("/sources call 2 completed", future1.isDone());
    assertTrue("/sources call 3 completed", future1.isDone());
  }

  @Test
  public void testRegister1Command() throws Exception
  {
    // /register?source=2
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/register?sources=3002");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    String respString = new String(respHandler.getReceivedBytes());
    LOG.debug("Response string: " + respString);
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    List<RegisterResponseEntry> res =
        objMapper.readValue(in, new TypeReference<List<RegisterResponseEntry>>(){});
    assertNotNull("has result", res);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response:" + new String(respHandler.getReceivedBytes()));
    }
    assertEquals("one source", 1, res.size());
    assertEquals("correct source id", 3002, res.get(0).getId());
    Schema resSchema = Schema.parse(res.get(0).getSchema());
    assertEquals("correct source schema", "test3.source2_v1", resSchema.getFullName());
  }

  private void prepareTestRegister2Command() throws Exception
  {
  }

  private void doTestRegister2Command() throws Exception
  {
    ObjectMapper objMapper = new ObjectMapper();

    // /register?source=1,2
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.GET, "/register?sources=4001,4002");
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    List<RegisterResponseEntry> res =
        objMapper.readValue(in, new TypeReference<List<RegisterResponseEntry>>(){});
    assertNotNull("has result", res);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response:" + new String(respHandler.getReceivedBytes()));
    }
    assertEquals("two sources", 2, res.size());
    assertEquals("correct source id", 4001, res.get(0).getId());
    Schema resSchema = Schema.parse(res.get(0).getSchema());
    assertEquals("correct source schema", "test4.source1_v1", resSchema.getFullName());
    assertEquals("correct source id", 4002, res.get(1).getId());
    resSchema = Schema.parse(res.get(1).getSchema());
    assertEquals("correct source schema", "test4.source2_v1", resSchema.getFullName());
  }


  @Test
  public void testRegister2Command() throws Exception
  {
    prepareTestRegister2Command();
    doTestRegister2Command();
  }

  @Test
  public void testRegister3Command() throws Exception
  {

    // /register?source=1,2,3
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                         HttpMethod.GET, "/register?sources=5001,5002,5003");
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);
    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    if (LOG.isDebugEnabled())
    {
      LOG.debug("/register response:" + new String(respHandler.getReceivedBytes()));
    }

    HttpResponse respObj = respHandler.getResponse();
    assertNotNull("exception class present", respObj.getHeader(DatabusRequest.DATABUS_ERROR_CLASS_HEADER));
    assertEquals("exception class name", InvalidRequestParamValueException.class.getName(),
                 respObj.getHeader(DatabusRequest.DATABUS_ERROR_CLASS_HEADER));
  }

  @Test
  public void testRegister1CommandThreaded() throws Exception
  {
    Future<?> future = _executor.submit(new Runnable()
    {
      @Override
      public void run()
      {
        boolean ok = true;
        try
        {
          testRegister1Command();
        }
        catch (Exception e)
        {
          ok = false;
        }
        assertTrue("no /register exception", ok);
      }
    });

    try
    {
    future.get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    assertTrue("/register completed", future.isDone());
  }

  @Test
  public void testRegister2CommandThreaded() throws Exception
  {
    Runnable runRegister2Command = new Runnable()
        {
          @Override
          public void run()
          {
            boolean ok = true;
            try
            {
              testRegister1Command();
            }
            catch (Exception e)
            {
              ok = false;
            }
            assertTrue("no /register exception", ok);
          }
        };

    Future<?>[] future = new Future<?>[4];

    for (int i = 0; i < 4; ++i) future[i] = _executor.submit(runRegister2Command);

    try
    {
      for (int i = 0; i < 4; ++i) future[i].get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    for (int i = 0; i < 4; ++i) assertTrue("/register thread " + (i+1) + " completed",
                                           future[i].isDone());
  }

  @Test
  public void testNoDataStreamCommand() throws Exception
  {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/stream?sources=100&size=1000&output=json&checkPoint={\"windowScn\":-1,\"windowOffset\":-1}");

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));
    assertEquals("empty response", 0, respHandler.getReceivedBytes().length);

    HttpResponse respObj = respHandler.getResponse();
    assertNull("/stream returns no error", respObj.getHeader(DatabusRequest.DATABUS_ERROR_CLASS_HEADER));
    assertNull("/stream returns no error with cause", respObj.getHeader(DatabusRequest.DATABUS_ERROR_CAUSE_CLASS_HEADER));
  }

  @Test
  public void testNoDataStreamCommandThreaded() throws Exception
  {
    int numThreads = 10;
    Runnable runRegister2Command = new Runnable()
        {
          @Override
          public void run()
          {
            LOG.info("Starting thread for /stream");
            boolean ok = true;
            try
            {
              testRegister1Command();
            }
            catch (Exception e)
            {
              ok = false;
            }
            assertTrue("no /register exception", ok);
            LOG.info("Stopping thread for /stream");
          }
        };

    Future<?>[] future = new Future<?>[numThreads];

    for (int i = 0; i < numThreads; ++i) future[i] = _executor.submit(runRegister2Command);

    try
    {
      for (int i = 0; i < numThreads; ++i) future[i].get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    for (int i = 0; i < numThreads; ++i) assertTrue("/register thread " + (i+1) + " completed",
                                           future[i].isDone());
  }

  private void prepareTestOneDataStreamCommand() throws Exception
  {
    //generate an event
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                     HttpMethod.GET, "/genDataEvents/start?src_ids=100&fromScn=10&eventsPerSec=1&duration=10");
    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    String respString = new String(respHandler.getReceivedBytes());
    LOG.debug("Response string:" + respString);

    ByteArrayInputStream in = new ByteArrayInputStream(respHandler.getReceivedBytes());
    ObjectMapper objMapper = new ObjectMapper();
    Map<String,String> genRes = objMapper.readValue(in, new TypeReference<Map<String,String>>(){});
    HttpResponse respObj = respHandler.getResponse();
    assertNull("/genDataEvents returns no error", respObj.getHeader(DatabusRequest.DATABUS_ERROR_CLASS_HEADER));
    assertEquals("generation started", "true", genRes.get("genDataEventsStarted"));

    try {Thread.sleep(2000);} catch (InterruptedException ie) {}
  }

  private void doTestOneDataStreamCommand() throws Exception
  {
    //try to read it
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();

    String streamRequest = "/stream?sources=100&size=100000&output=json&checkPoint=" + cp.toString();

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, streamRequest);

    SimpleTestHttpClient httpClient = SimpleTestHttpClient.createLocal(TimeoutPolicy.ALL_TIMEOUTS);
    SimpleHttpResponseHandler respHandler = httpClient.sendRequest(_serverAddress, httpRequest);

    assertTrue("gets response", respHandler.awaitResponseUninterruptedly(1, TimeUnit.SECONDS));

    HttpResponse respObj = respHandler.getResponse();
    assertNull("/stream returns no error", respObj.getHeader(DatabusRequest.DATABUS_ERROR_CLASS_HEADER));

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
    prepareTestOneDataStreamCommand();
    doTestOneDataStreamCommand();
  }

  @Test
  public void testOneDataStreamCommandThreaded() throws Exception
  {
    prepareTestOneDataStreamCommand();

    int numThreads = 10;
    Runnable runRegister2Command = new Runnable()
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
            assertTrue("no /stream exception", ok);
            LOG.info("Stopping thread for /stream");
          }
        };

    Future<?>[] future = new Future<?>[numThreads];

    for (int i = 0; i < numThreads; ++i) future[i] = _executor.submit(runRegister2Command);

    try
    {
      for (int i = 0; i < numThreads; ++i) future[i].get(2, TimeUnit.SECONDS);
    }
    catch (InterruptedException ie) {}
    for (int i = 0; i < numThreads; ++i) assertTrue("/register thread " + (i+1) + " completed",
                                           future[i].isDone());
  }

}
