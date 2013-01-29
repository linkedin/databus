package com.linkedin.databus.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusHttpClientImpl.RuntimeConfigBuilder;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.netty.NettyHttpDatabusRelayConnection;
import com.linkedin.databus.client.netty.NettyHttpDatabusRelayConnectionInspector;
import com.linkedin.databus.client.netty.NettyTestUtils;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DataChangeEvent;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestDatabusHttpClient
{
  public static final Logger LOG = Logger.getLogger("TestDatabusHttpClient");
  static final Schema SOURCE1_SCHEMA =
      Schema.parse("{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}");
  static final String SOURCE1_SCHEMA_STR = SOURCE1_SCHEMA.toString();
  static final byte[] SOURCE1_SCHEMAID = SchemaHelper.getSchemaId(SOURCE1_SCHEMA_STR);
  static final ExecutorService BOSS_POOL = Executors.newCachedThreadPool();
  static final ExecutorService IO_POOL = Executors.newCachedThreadPool();
  static final int[] RELAY_PORT = {14467, 14468, 14469};
  static final int CLIENT_PORT = 15500;
  static final Timer NETWORK_TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
  static final ChannelGroup TEST_CHANNELS_GROUP = new DefaultChannelGroup();
  static final long DEFAULT_READ_TIMEOUT_MS = 10000;
  static final long DEFAULT_WRITE_TIMEOUT_MS = 10000;
  static final String SOURCE1_NAME = "test.event.source1";
  static SimpleTestServerConnection[] _dummyServer = new SimpleTestServerConnection[RELAY_PORT.length];
  static DbusEventBuffer.StaticConfig _bufCfg;
  static DatabusHttpClientImpl.StaticConfig _stdClientCfg;
  static DatabusHttpClientImpl.Config _stdClientCfgBuilder;

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    //setup logging
    TestUtil.setupLogging(true, null, Level.ERROR);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    //initialize relays
    for (int relayN = 0; relayN < RELAY_PORT.length; ++relayN)
    {
      _dummyServer[relayN] = new SimpleTestServerConnection(DbusEvent.byteOrder,
                                                    SimpleTestServerConnection.ServerType.NIO);
      _dummyServer[relayN].setPipelineFactory(new ChannelPipelineFactory() {
          @Override
          public ChannelPipeline getPipeline() throws Exception {
              return Channels.pipeline(new LoggingHandler(InternalLogLevel.DEBUG),
                                       new HttpServerCodec(),
                                       new LoggingHandler(InternalLogLevel.DEBUG),
                                       new SimpleObjectCaptureHandler());
          }
      });
      _dummyServer[relayN].start(RELAY_PORT[relayN]);
    }

    //create standard client config
    DatabusHttpClientImpl.Config clientCfgBuilder = new DatabusHttpClientImpl.Config();
    clientCfgBuilder.getContainer().setHttpPort(CLIENT_PORT);
    clientCfgBuilder.getContainer().getJmx().setRmiEnabled(false);
    clientCfgBuilder.getContainer().setReadTimeoutMs(10000000);
    clientCfgBuilder.getConnectionDefaults().getPullerRetries().setInitSleep(10);
    clientCfgBuilder.getRuntime().getBootstrap().setEnabled(false);
    clientCfgBuilder.getCheckpointPersistence().setClearBeforeUse(true);
    for (int i = 0; i < RELAY_PORT.length; ++i)
    {
      clientCfgBuilder.getRuntime().getRelay(Integer.toString(i)).setHost("localhost");
      clientCfgBuilder.getRuntime().getRelay(Integer.toString(i)).setPort(RELAY_PORT[i]);
      clientCfgBuilder.getRuntime().getRelay(Integer.toString(i)).setSources(SOURCE1_NAME);
    }

    _stdClientCfgBuilder = clientCfgBuilder;
    _stdClientCfg = clientCfgBuilder.build();

    //create standard relay buffer config
    DbusEventBuffer.Config bufCfgBuilder = new DbusEventBuffer.Config();
    bufCfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    bufCfgBuilder.setMaxSize(100000);
    bufCfgBuilder.setScnIndexSize(128);
    bufCfgBuilder.setReadBufferSize(1);

    _bufCfg = bufCfgBuilder.build();
  }

  private ServerInfo registerRelay(int id, String name, InetSocketAddress addr, String sources,
                             DatabusHttpClientImpl client)
          throws InvalidConfigException
  {
    RuntimeConfigBuilder rtConfigBuilder =
        (RuntimeConfigBuilder)client.getClientConfigManager().getConfigBuilder();

    ServerInfoBuilder relayConfigBuilder = rtConfigBuilder.getRelay(Integer.toString(id));
    relayConfigBuilder.setName(name);
    relayConfigBuilder.setHost(addr.getHostName());
    relayConfigBuilder.setPort(addr.getPort());
    relayConfigBuilder.setSources(sources);
    ServerInfo si = relayConfigBuilder.build();
    client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
    return si;
  }

  @Test
  public void testListGenerics() throws Exception
  {
	  Map<List<String>, List<String>> a = new HashMap<List<String>, List<String>>();
	  List<String> l1 = Arrays.asList("S1", "S2");
	  List<String> v1 = Arrays.asList("localhost:8080");
	  a.put(l1, v1);
	  assertEquals("CompareSize", 1, safeListSize(a.get(l1)));

	  Map<List<DatabusSubscription>, List<String>> b = new HashMap<List<DatabusSubscription>, List<String>>();
      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S2"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S3"));
      b.put(ls1, l1);
	  assertEquals(2, safeListSize(b.get(ls1)));
	  assertEquals(0, safeListSize(b.get(ls2)));
  }

    @Test
	public void testRegisterDatabusStreamListener() throws Exception
	{
	  DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
	  clientConfig.getContainer().getJmx().setRmiEnabled(false);
      clientConfig.getContainer().setHttpPort(12003);
	  DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2", client);
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5", client);

      DummyStreamConsumer listener1 = new DummyStreamConsumer("consumer1");
      client.registerDatabusStreamListener(listener1 , null, "S1");

      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S2"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S3"));
      List<DatabusSubscription> ls3 = DatabusSubscription.createSubscriptionList(Arrays.asList("S3", "S4", "S5"));

      assertEquals("one consumer + logging consumer in (S1,S2) or (S1,S3)", 2,
                   safeListSize(client.getRelayGroupStreamConsumers().get(ls1)) +
                   safeListSize(client.getRelayGroupStreamConsumers().get(ls2)));

      DummyStreamConsumer listener2 = new DummyStreamConsumer("consumer2");
      client.registerDatabusStreamListener(listener2 , null, "S1");
      int consumersNum = safeListSize(client.getRelayGroupStreamConsumers().get(ls1)) +
      safeListSize(client.getRelayGroupStreamConsumers().get(ls2));
      assertTrue("two consumers + 1-2 logging consumer  in (S1,S2) or (S1,S3)", 3 <= consumersNum
                 && consumersNum <= 4);

      DatabusStreamConsumer listener3 = client.getLoggingListener();
      client.registerDatabusStreamListener(listener3, null, "S5");
      assertEquals("one consumers in (S3,S4,S5)", 1,
                   safeListSize(client.getRelayGroupStreamConsumers().get(ls3)));


	}

    @Test(expectedExceptions=DatabusClientException.class)
    public void testRegisterDatabusStreamListenerMissingSource() throws Exception
    {
      DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
      clientConfig.getContainer().getJmx().setRmiEnabled(false);
      clientConfig.getContainer().setHttpPort(10001);
      DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1, S2", client);
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5", client);

      DummyStreamConsumer listener1 = new DummyStreamConsumer("consumer1");
      client.registerDatabusStreamListener(listener1 , null, "S10");
	}

	@Test(expectedExceptions=DatabusClientException.class)
	public void testRegisterDatabusStreamListenerWrongSourceOrder() throws Exception {
      DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
      clientConfig.getContainer().getJmx().setRmiEnabled(false);
      clientConfig.getContainer().setHttpPort(10002);
      DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2", client);
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S5", client);

      DummyStreamConsumer listener1 = new DummyStreamConsumer("consumer1");
      client.registerDatabusStreamListener(listener1 , null, "S5", "S3");
	}

	@Test
	public void testUnregisterDatabusStreamListener() throws Exception
	{
      DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
      clientConfig.getContainer().getJmx().setRmiEnabled(false);
      clientConfig.getContainer().setHttpPort(13403);
      DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2", client);
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5", client);

      DummyStreamConsumer listener1 = new DummyStreamConsumer("consumer1");
      client.registerDatabusStreamListener(listener1 , null, "S1");
      client.registerDatabusStreamListener(listener1 , null, "S2");

      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S2"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S3"));

      int consumersNum1 = safeListSize(client.getRelayGroupStreamConsumers().get(ls1)) +
          safeListSize(client.getRelayGroupStreamConsumers().get(ls2));
      assertTrue("two consumers+ 1-2 logging consumer in (S1,S2) or (S1,S3)", 3 <= consumersNum1 &&
                 consumersNum1 <= 4);
      assertTrue("at least one consumer + logging consumer in (S1,S2)",
                 safeListSize(client.getRelayGroupStreamConsumers().get(ls1))
                 >= 2);

      DummyStreamConsumer listener2 = new DummyStreamConsumer("consumer2");
      client.registerDatabusStreamListener(listener2 , null, "S1");
      int consumersNum2 = safeListSize(client.getRelayGroupStreamConsumers().get(ls1)) +
      safeListSize(client.getRelayGroupStreamConsumers().get(ls2));
      assertTrue("three consumers+ 1-2 logging consumers in (S1,S2) or (S1,S3)", 4 <= consumersNum2 &&
                 consumersNum2 <= 5);

      client.unregisterDatabusStreamListener(listener1);
      int consumersNum3 = safeListSize(client.getRelayGroupStreamConsumers().get(ls1)) +
          safeListSize(client.getRelayGroupStreamConsumers().get(ls2));
      //assertEquals("one consumer + 1-2 logging consumer in (S1,S2) or (S1,S3)", consumersNum2 - 2,
      //             consumersNum3);

      client.unregisterDatabusStreamListener(listener1);
      int consumersNum4 = safeListSize(client.getRelayGroupStreamConsumers().get(ls1)) +
          safeListSize(client.getRelayGroupStreamConsumers().get(ls2));
      assertEquals("one consumer + 1-2 logging consumer  in (S1,S2) or (S1,S3)", consumersNum3,
                   consumersNum4);

      client.unregisterDatabusStreamListener(listener2);
	}

	@Test
	public void testRegisterRelay() throws Exception
	{
      DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
      clientConfig.getContainer().getJmx().setRmiEnabled(false);
      clientConfig.getContainer().setHttpPort(10004);
      DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S2"));
      List<DatabusSubscription> sl1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S2", "S1"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S3"));
      List<DatabusSubscription> ls3 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1"));
      List<DatabusSubscription> ls4 = DatabusSubscription.createSubscriptionList(Arrays.asList("S3", "S4", "S5"));
      List<DatabusSubscription> ls5 = DatabusSubscription.createSubscriptionList(Arrays.asList("S3", "S4"));

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8880), "S1,S2", client);
      assertEquals("one relay group", 1, client.getRelayGroups().size());
      assertEquals("one relay", 1, client.getRelays().size());
      assertEquals("relay group S1,S2", true,
                   client.getRelayGroups().containsKey(ls1));
      assertEquals("no relay group S2,S1", false,
                   client.getRelayGroups().containsKey(sl1));

      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
      assertEquals("two relay groups", 2, client.getRelayGroups().size());
      assertEquals("two relays", 2, client.getRelays().size());
      assertEquals("relay group S1,S2", true, client.getRelayGroups().containsKey(ls1));
      assertEquals("relay group S1,S3", true, client.getRelayGroups().containsKey(ls2));
      assertEquals("no relay group S1", false, client.getRelayGroups().containsKey(ls3));

      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);
      assertEquals("two relay groups", 2, client.getRelayGroups().size());
      assertEquals("three relays", 3, client.getRelays().size());
      assertEquals("S1,S2 has two relays", 2, client.getRelayGroups().get(ls1).size());

      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5", client);
      assertEquals("three relay groups", 3, client.getRelayGroups().size());
      assertEquals("four relays", 4, client.getRelays().size());
      assertEquals("relay group S1,S2", true, client.getRelayGroups().containsKey(ls1));
      assertEquals("relay group S1,S3", true, client.getRelayGroups().containsKey(ls2));
      assertEquals("relay group S3,S4,S5", true, client.getRelayGroups().containsKey(ls4));
      assertEquals("no relay group S3,S4", false, client.getRelayGroups().containsKey(ls5));
	}

	@Test
	public void testDatabusSources() throws Exception
	{
	  DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
	  clientConfig.getContainer().getJmx().setRmiEnabled(false);
      clientConfig.getContainer().setHttpPort(10100);
	  DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

      ServerInfo s1 = registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2", client);
      @SuppressWarnings("unused")
      ServerInfo s2 = registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
      ServerInfo s11 = registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);

      DummyStreamConsumer listener1 = new DummyStreamConsumer("consumer1");
      client.registerDatabusStreamListener(listener1 , null, "S2");

      try
      {
    	  client.start();
      } catch (Exception e){}

      assertEquals("Num connections must be 1", 1, client.getRelayConnections().size());

      DatabusSourcesConnection dsc2 = client.getRelayConnections().get(0);
      Set<ServerInfo> relaysInClient2 = dsc2.getRelays();
      Set<ServerInfo> ssi2 = new HashSet<ServerInfo>();
      ssi2.add(s1); ssi2.add(s11);
      assertTrue(relaysInClient2.equals(ssi2));

      client.shutdown();
	}

	@Test
	public void testPullerRetriesExhausted() throws Exception
	{
		DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
		clientConfig.getConnectionDefaults().getPullerRetries().setMaxRetryNum(1);
		clientConfig.getContainer().getJmx().setRmiEnabled(false);
		clientConfig.getContainer().setHttpPort(10100);
		final DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

		int port = Utils.getAvailablePort(8888);
		@SuppressWarnings("unused")
		ServerInfo s1 = registerRelay(1, "relay1", new InetSocketAddress("localhost", port), "S1", client);
		DummySuccessfulErrorCountingConsumer listener1 = new DummySuccessfulErrorCountingConsumer("consumer1", false);
		client.registerDatabusStreamListener(listener1 , null, "S1");

		Thread startThread = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				client.start();
			}
		}, "client start thread");
		startThread.start();
		int count = 0;
		while (client.getRelayConnections().size() == 0 && count < 10)
		{
			Thread.sleep(500);
			count++;
		}
		if ( count >= 10)
			throw new Exception("Client did not start up in 10 iterations");

		DatabusSourcesConnection dsc = client.getRelayConnections().get(0);

		RelayDispatcher rd = (RelayDispatcher) dsc.getRelayDispatcher();
		Assert.assertEquals(true, null != dsc);

		List<String> sources = new ArrayList<String>();
		Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
		for (int i = 1; i <= 3; ++i)
		{
			IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
			sources.add(sourcePair.getName());
			sourcesMap.put(sourcePair.getId(), sourcePair);
		}

		HashMap<Long, List<RegisterResponseEntry>> schemaMap =
				new HashMap<Long, List<RegisterResponseEntry>>();

		List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
		List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
		List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

		final String SOURCE1_SCHEMA_STR = "{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";
		final String SOURCE2_SCHEMA_STR = "{\"name\":\"source2\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";;
		final String SOURCE3_SCHEMA_STR = "{\"name\":\"source3\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";;

		l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
		l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
		l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

		schemaMap.put(1L, l1);
		schemaMap.put(2L, l2);
		schemaMap.put(3L, l3);

		rd.enqueueMessage(DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
				schemaMap,
				dsc.getDataEventsBuffer()));

		try
		{
			Thread.currentThread();
      Thread.sleep(1000);
		}
		catch (InterruptedException e)
		{
			LOG.warn("interrupted");
		}
		Assert.assertEquals(listener1._errorCount, 1);
		client.shutdown();
	}

	@Test
	public void testDispatcherRetriesExhausted() throws Exception
	{
		DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
		clientConfig.getConnectionDefaults().getDispatcherRetries().setMaxRetryNum(1);
		clientConfig.getContainer().getJmx().setRmiEnabled(false);
		int port = Utils.getAvailablePort(10100);
		clientConfig.getContainer().setHttpPort(port);
		final DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

		port = Utils.getAvailablePort(8888);
		@SuppressWarnings("unused")
		ServerInfo s1 = registerRelay(1, "relay1", new InetSocketAddress("localhost", port), "S1", client);
		//DummyStreamConsumer listener1 = new DummyStreamConsumer("consumer1");
		DummySuccessfulErrorCountingConsumer listener1 = new DummySuccessfulErrorCountingConsumer("consumer1", true);
		client.registerDatabusStreamListener(listener1 , null, "S1");

		Thread startThread = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				client.start();
			}
		}, "client start thread");
		startThread.start();
		int count = 0;
		while (client.getRelayConnections().size() == 0 && count < 10)
		{
		  Thread.sleep(500);
		  count++;
		}
		if ( count >= 10)
			throw new Exception("Client did not start up in 10 iterations");

		DatabusSourcesConnection dsc = client.getRelayConnections().get(0);

		RelayDispatcher rd = (RelayDispatcher) dsc.getRelayDispatcher();
		Assert.assertEquals(true, null != dsc);

		List<String> sources = new ArrayList<String>();
		Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
		for (int i = 1; i <= 3; ++i)
		{
			IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
			sources.add(sourcePair.getName());
			sourcesMap.put(sourcePair.getId(), sourcePair);
		}

		HashMap<Long, List<RegisterResponseEntry>> schemaMap =
				new HashMap<Long, List<RegisterResponseEntry>>();

		List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
		List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
		List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

		final String SOURCE1_SCHEMA_STR = "{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";
		final String SOURCE2_SCHEMA_STR = "{\"name\":\"source2\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";;
		final String SOURCE3_SCHEMA_STR = "{\"name\":\"source3\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";;

		l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
		l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
		l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

		schemaMap.put(1L, l1);
		schemaMap.put(2L, l2);
		schemaMap.put(3L, l3);

        int source1EventsNum = 2;
        int source2EventsNum = 2;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

		// Send dummy e
        DbusEventBuffer eventsBuf = dsc.getDataEventsBuffer();
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
        eventsBuf.endEvents(100L,null);

        rd.enqueueMessage(DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
				schemaMap,
				dsc.getDataEventsBuffer()));

		try
		{
		  Thread.sleep(1000);
		}
		catch (InterruptedException e)
		{
			LOG.warn("interrupted");
		}
		Assert.assertEquals(listener1._errorCount, 1);
		client.shutdown();
	}

    private void initBufferWithEvents(DbusEventBuffer eventsBuf,
            long keyBase,
            int numEvents,
            short srcId,
            Hashtable<Long, AtomicInteger> keyCounts,
            Hashtable<Short, AtomicInteger> srcidCounts)
    {
        if (null != srcidCounts) srcidCounts.put(srcId, new AtomicInteger(0));

        for (long i = 0; i < numEvents; ++i)
        {
            String value = "{\"s\":\"value" + i + "\"}";
            eventsBuf.appendEvent(new DbusEventKey(keyBase + i), (short)0, (short)1, (short)0, srcId,
                    new byte[16], value.getBytes(), false);
            if (null != keyCounts) keyCounts.put(keyBase + i, new AtomicInteger(0));
        }
    }

    /**
     * Tests the logic of the client to disconnect from a relay while still processing
     * data from the relay. We want to make sure that ChunkedBodyReadableByteChannel
     * and thus the StreamHttpResponseProcessor do net get blocked with unprocessed data.
     * One way to trigger such an error is to insert some invalid data inside the
     * response and then some more data ( > 4MB which is the current buffer size). */
    @Test
    public void testInStreamError() throws Exception
    {
        final Logger log = Logger.getLogger("TestDatabusHttpClient.testInStreamError");
        log.setLevel(Level.DEBUG);
        final int eventsNum = 20;
        DbusEventInfo[] events = createSampleSchema1Events(eventsNum);

        //simulate relay buffers
        DbusEventBuffer relayBuffer = new DbusEventBuffer(_bufCfg);
        relayBuffer.start(0);
        writeEventsToBuffer(relayBuffer, events, 4);

        //prepare stream response
        Checkpoint cp = Checkpoint.createFlexibleCheckpoint();
        final DbusEventsStatisticsCollector stats =
            new DbusEventsStatisticsCollector(1, "test1", true, false, null);
        ChannelBuffer streamResPrefix =
            NettyTestUtils.streamToChannelBuffer(relayBuffer, cp, 20000, stats);
        final StringBuilder alotOfDeadbeef = new StringBuilder();
        for (int i = 0; i < 1024 * 1024; ++i) {
          alotOfDeadbeef.append("DEADBEEF ");
        }
        ChannelBuffer streamResSuf =
            ChannelBuffers.wrappedBuffer(alotOfDeadbeef.toString().getBytes("UTF-8"));
        final ChannelBuffer streamRes = ChannelBuffers.wrappedBuffer(streamResPrefix, streamResSuf);

        //create client
        _stdClientCfgBuilder.getContainer().setReadTimeoutMs(DEFAULT_READ_TIMEOUT_MS);

        final DatabusHttpClientImpl client = new DatabusHttpClientImpl(_stdClientCfgBuilder.build());

        final TestConsumer consumer = new TestConsumer();
        client.registerDatabusStreamListener(consumer, null, SOURCE1_NAME);

        client.start();
        try
        {
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return client._relayConnections.size() == 1;
            }
          }, "sources connection present", 100, log);

          final DatabusSourcesConnection clientConn = client._relayConnections.get(0);
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != clientConn.getRelayPullThread().getLastOpenConnection();
            }
          }, "relay connection present", 100, log);

          final NettyHttpDatabusRelayConnection relayConn =
              (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
          final NettyHttpDatabusRelayConnectionInspector relayConnInsp =
              new NettyHttpDatabusRelayConnectionInspector(relayConn);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != relayConnInsp.getChannel() && relayConnInsp.getChannel().isConnected();
            }
          }, "client connected", 200, log);

          //figure out the connection to the relay
          Channel clientChannel = relayConnInsp.getChannel();
          InetSocketAddress relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
          SocketAddress clientAddr = clientChannel.getLocalAddress();
          int relayPort = relayAddr.getPort();
          log.info("relay selected: " + relayPort);

          SimpleTestServerConnection relay = null;
          for (int i = 0; i < RELAY_PORT.length; ++i)
          {
            if (relayPort == RELAY_PORT[i]) relay = _dummyServer[i];
          }
          assertTrue(null != relay);

          final SocketAddress testClientAddr = clientAddr;
          final SimpleTestServerConnection testRelay = relay;
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != testRelay.getChildChannel(testClientAddr);
            }
          }, "relay detects new connection", 1000, log);

          Channel serverChannel = relay.getChildChannel(clientAddr);
          assertTrue(null != serverChannel);
          ChannelPipeline serverPipeline = serverChannel.getPipeline();
          SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
          objCapture.clear();

          //send back the /sources response
          HttpResponse sourcesResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                             HttpResponseStatus.OK);
          sourcesResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          sourcesResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
          HttpChunk body =
              new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
                                   SOURCE1_NAME + "\"}]").getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          log.debug("process the /register request");
          NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          log.debug("send back the /register response");
          RegisterResponseEntry entry = new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR);
          String responseStr = NettyTestUtils.generateRegisterResponse(entry);
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          log.debug("make sure the client processes the response /register correctly");
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", 100, log);

          log.debug("process /stream call and return a response with garbled suffix");
          NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*", 1000);
          objCapture.clear();

          final HttpResponse streamResp =
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          streamResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          streamResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);

          //send the response asynchronously in case the client blocks
          final Thread streamRespThread = new Thread(new Runnable()
          {

            @Override
            public void run()
            {
              NettyTestUtils.sendServerResponses(testRelay, testClientAddr, streamResp,
                                                 new DefaultHttpChunk(streamRes),
                                                 60000);
            }
          }, "send /stream resp");
          streamRespThread.setDaemon(true);
          streamRespThread.start();

          log.debug("make sure the client disconnects and recovers from the /stream response");
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != relayConnInsp.getChannel() && !relayConnInsp.getChannel().isConnected();
            }
          }, "client disconnected", 30000, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return clientConn.getRelayPullThread().getLastOpenConnection() != relayConn;
            }
          }, "new netty connection", 30000, log);

          //make sure the relay send thread is dead
          streamRespThread.join(100);
          Assert.assertTrue(!streamRespThread.isAlive());

          log.debug("PHASE 2: make sure the client has fully recovered and able to connect to another relay");

          final NettyHttpDatabusRelayConnection newRelayConn =
              (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
          final NettyHttpDatabusRelayConnectionInspector newRelayConnInsp =
              new NettyHttpDatabusRelayConnectionInspector(newRelayConn);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != newRelayConnInsp.getChannel() && newRelayConnInsp.getChannel().isConnected();
            }
          }, "client connected to new relay", 200, log);

          //figure out the connection to the relay
          clientChannel = newRelayConnInsp.getChannel();
          relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
          clientAddr = clientChannel.getLocalAddress();
          relayPort = relayAddr.getPort();
          log.info("new relay selected: " + relayPort);

          relay = null;
          int relayIdx = 0;
          for (; relayIdx < RELAY_PORT.length; ++relayIdx)
          {
            if (relayPort == RELAY_PORT[relayIdx]) relay = _dummyServer[relayIdx];
          }
          assertTrue(null != relay);

          serverChannel = relay.getChildChannel(clientAddr);
          assertTrue(null != serverChannel);
          serverPipeline = serverChannel.getPipeline();
          objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
          objCapture.clear();

          //send back the /sources response
          body = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
                                      SOURCE1_NAME + "\"}]").getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
          NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          //send back the /register response
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", 100, log);

          //process /stream call and return a partial window
          Matcher streamMatcher =
              NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*checkPoint=([^&]*)&.*",
                                                1000);
          String cpString = streamMatcher.group(1);
          objCapture.clear();

          cp = new Checkpoint(cpString);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamResPrefix));

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("lastWrittenScn=" + clientConn.getDataEventsBuffer().lastWrittenScn());
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 20;
            }
          }, "client receieves /stream response", 1100, log);


          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", 11000, log);
        }
        finally {
          client.shutdown();
        }
    }

	@Test
	/**
	 * R1 EVB Boundaries ------------------------------------------------------------------------------
	 *                   ^                        ^                          ^                    ^
	 *                   0                        30                         60                   90
	 * R2 EVB Boundaries --------------------------------------------------------------------------------
	 *                   ^               ^                   ^               ^            ^
	 *                   0               20                  40              60           80
	 *
	 * R3 EVB Boundaries --------------------------------------------------------------------------------
	 *                   ^      ^        ^         ^         ^       ^       ^      ^     ^        ^
	 *                   0      10       20        30        40      50      60     70    80       90
	 *
	 *
     *  Client switches from R1 to R2 on server close and from R2 to R3 on timeout while in the middle of the windows.
	 *
	 *  Switch from R1 -> R2 happens when windowScn = 30 and WindowOffset = 8th event
	 *  Switch from R2 -> R3 happens when windowScn = 40 and windowOffset = 5th event
	 *
	 * @throws Exception
	 */
	public void testRelayFailoverPartialWindow2() throws Exception
	{
	    final Logger log = Logger.getLogger("TestDatabusHttpClient.testRelayFailoverPartialWindow2");
	    //log.setLevel(Level.DEBUG);
	    final int eventsNum = 200;
	    DbusEventInfo[] events = createSampleSchema1Events(eventsNum);

	    //simulate relay buffers
	    DbusEventBuffer[] relayBuffer = new DbusEventBuffer[RELAY_PORT.length];
	    List<List<Integer>> eventOfs = new ArrayList<List<Integer>>(3);
	    List<List<DbusEventKey>> eventKeys = new ArrayList<List<DbusEventKey>>(3);

	    for (int i = 0; i < RELAY_PORT.length; ++i)
	    {
	      relayBuffer[i] = new DbusEventBuffer(_bufCfg);
	      relayBuffer[i].start(0);
	      WriteEventsResult wrRes= writeEventsToBuffer(relayBuffer[i], events, (RELAY_PORT.length - i) * 10);
	      List<Integer> ofs = wrRes.getOffsets();
	      eventOfs.add(ofs);
	      eventKeys.add(wrRes.getKeys());
	    }

	    List<DbusEventKey> key = eventKeys.get(0);
	    for (int i = 1; i < RELAY_PORT.length; ++i)
	    {
	    	assertEquals(key, eventKeys.get(i));
	    	key = eventKeys.get(i);
	    }
	    //figure out an event offset inside a window
		int resp1EnfOfs = eventOfs.get(0).get(8);

	    //create client
	    _stdClientCfgBuilder.getContainer().setReadTimeoutMs(DEFAULT_READ_TIMEOUT_MS);

	    final DatabusHttpClientImpl client = new DatabusHttpClientImpl(_stdClientCfgBuilder.build());

	    final TestConsumer consumer = new TestConsumer();
	    client.registerDatabusStreamListener(consumer, null, SOURCE1_NAME);

	    client.start();
	    try
	    {
	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return client._relayConnections.size() == 1;
            }
          }, "sources connection present", 100, log);

	      final DatabusSourcesConnection clientConn = client._relayConnections.get(0);
	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != clientConn.getRelayPullThread().getLastOpenConnection();
            }
          }, "relay connection present", 100, log);

	      final NettyHttpDatabusRelayConnection relayConn =
	          (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
	      final NettyHttpDatabusRelayConnectionInspector relayConnInsp =
	          new NettyHttpDatabusRelayConnectionInspector(relayConn);

	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != relayConnInsp.getChannel() && relayConnInsp.getChannel().isConnected();
            }
          }, "client connected", 200, log);

	      //figure out the connection to the relay
	      Channel clientChannel = relayConnInsp.getChannel();
	      InetSocketAddress relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
	      SocketAddress clientAddr = clientChannel.getLocalAddress();
	      int relayPort = relayAddr.getPort();
	      log.info("relay selected: " + relayPort);

	      SimpleTestServerConnection relay = null;
	      for (int i = 0; i < RELAY_PORT.length; ++i)
	      {
	        if (relayPort == RELAY_PORT[i]) relay = _dummyServer[i];
	      }
	      assertTrue(null != relay);

	      final SocketAddress testClientAddr = clientAddr;
	      final SimpleTestServerConnection testRelay = relay;
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != testRelay.getChildChannel(testClientAddr);
            }
          }, "relay detects new connection", 1000, log);

	      Channel serverChannel = relay.getChildChannel(clientAddr);
	      assertTrue(null != serverChannel);
	      ChannelPipeline serverPipeline = serverChannel.getPipeline();
	      SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
	      objCapture.clear();

	      //send back the /sources response
	      HttpResponse sourcesResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
	                                                         HttpResponseStatus.OK);
	      sourcesResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
	      sourcesResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
	      HttpChunk body =
	          new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
	                               SOURCE1_NAME + "\"}]").getBytes()));
	      NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

	      //make sure the client processes the response correctly
	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
	      NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          //send back the /register response
          RegisterResponseEntry entry = new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR);
          String responseStr = NettyTestUtils.generateRegisterResponse(entry);
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", 100, log);

          //process /stream call and return a partial window
          NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*", 1000);
          objCapture.clear();

          //send back the /stream response
          final DbusEventsStatisticsCollector stats =
              new DbusEventsStatisticsCollector(1, "test1", true, false, null);
          Checkpoint cp = Checkpoint.createFlexibleCheckpoint();
          ChannelBuffer streamRes = NettyTestUtils.streamToChannelBuffer(relayBuffer[0], cp,
                                                                          resp1EnfOfs, stats);
          HttpResponse streamResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                             HttpResponseStatus.OK);
          streamResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          streamResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamRes));

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              LOG.debug("LastWritten SCN:" + clientConn.getDataEventsBuffer().lastWrittenScn() );
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 30;
            }
          }, "client receieves /stream response", 1100, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.error("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", 110000, log);
          assertEquals(-1, consumer.getRollbackScn());
          int rollbackNum = 0;
          assertEquals(stats.getTotalStats().getNumSysEvents() + 1 + rollbackNum,
                       consumer.getWinNum());

          List<DbusEventKey> expKeys = eventKeys.get(0).subList(0, (int)stats.getTotalStats().getNumDataEvents());
          List<Long> expSeqs = new ArrayList<Long>();
          for (int i = 0; i < stats.getTotalStats().getNumDataEvents(); i++)
        	  expSeqs.add(30L);

          long numEvents = stats.getTotalStats().getNumDataEvents();

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          numEvents = stats.getTotalStats().getNumDataEvents();

          //now kill the relay and wait for a failover
          serverChannel.close();

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != relayConnInsp.getChannel() && !relayConnInsp.getChannel().isConnected();
            }
          }, "client disconnected", 200, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return clientConn.getRelayPullThread().getLastOpenConnection() != relayConn;
            }
          }, "new netty connection", 200, log);

          /////////// FAKING CONNECTION TO NEW RELAY

          final NettyHttpDatabusRelayConnection newRelayConn =
              (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
          final NettyHttpDatabusRelayConnectionInspector newRelayConnInsp =
              new NettyHttpDatabusRelayConnectionInspector(newRelayConn);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != newRelayConnInsp.getChannel() && newRelayConnInsp.getChannel().isConnected();
            }
          }, "client connected to new relay", 200, log);

          //figure out the connection to the relay
          clientChannel = newRelayConnInsp.getChannel();
          relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
          clientAddr = clientChannel.getLocalAddress();
          relayPort = relayAddr.getPort();
          log.info("new relay selected: " + relayPort);

          relay = null;
          int relayIdx = 0;
          for (; relayIdx < RELAY_PORT.length; ++relayIdx)
          {
            if (relayPort == RELAY_PORT[relayIdx]) relay = _dummyServer[relayIdx];
          }
          assertTrue(null != relay);

          serverChannel = relay.getChildChannel(clientAddr);
          assertTrue(null != serverChannel);
          serverPipeline = serverChannel.getPipeline();
          objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
          objCapture.clear();

          //send back the /sources response
          body = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
                                      SOURCE1_NAME + "\"}]").getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
          NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          //send back the /register response
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", 100, log);

          //process /stream call and return a partial window
          Matcher streamMatcher =
              NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*checkPoint=([^&]*)&.*",
                                                1000);
          String cpString = streamMatcher.group(1);
          objCapture.clear();

          int respStartOfs = eventOfs.get(1).get(1);
          int respEndOfs = eventOfs.get(1).get(26);

          cp = new Checkpoint(cpString);
          assertTrue(cp.getWindowOffset() > 0); //last window read was partial
          streamRes = NettyTestUtils.streamToChannelBuffer(relayBuffer[1], cp,
                                                           respEndOfs - respStartOfs,
                                                           stats);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamRes));

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("lastWrittenScn=" + clientConn.getDataEventsBuffer().lastWrittenScn());
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 40;
            }
          }, "client receieves /stream response", 1100, log);


          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", 11000, log);
          assertEquals(20, consumer.getRollbackScn());
          //add one more onStartDataEventsSequence after the rollback()
          ++rollbackNum;
          assertEquals(stats.getTotalStats().getNumSysEvents() + 1 + rollbackNum, consumer.getWinNum());
          expKeys.addAll(eventKeys.get(1).subList(0, (int)(stats.getTotalStats().getNumDataEvents() - numEvents)));
          for (int i = 0; i < (stats.getTotalStats().getNumDataEvents() - numEvents); i++)
        	  expSeqs.add((i/20)*20 + 20L);

          log.info("Expected NumKeys :" + expKeys.size() + ", Got NumKeys :" + consumer.getKeys().size());
          for(int i = 0; i < expKeys.size(); i++)
          {
        	  if (! consumer.getKeys().contains(expKeys.get(i)))
        		  log.error(i + " Key :" + expKeys.get(i) + " missing !!");
        	  else
        		  log.info(i + " Key :" + expKeys.get(i) + " present !!");
          }

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          numEvents = stats.getTotalStats().getNumDataEvents();

          assertEquals(clientConn.getRelayPullThread().getConnectionState().getDataEventsBuffer().isSCNRegress(), false);
          ///////////////////////////////////
          //simulate a timeout on the server; the client would have sent a /stream call and there
          //will be no response from the server, so eventually it should time out and switch servers
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("Channel :" + newRelayConnInsp.getChannel());
              return (null != newRelayConnInsp.getChannel()) && (!newRelayConnInsp.getChannel().isConnected());
            }
          }, "waiting for a reconnect", (long)(DEFAULT_READ_TIMEOUT_MS * 1.5), log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return clientConn.getRelayPullThread().getLastOpenConnection() != relayConn;
            }
          }, "new netty connection", 200, log);

          final NettyHttpDatabusRelayConnection new2RelayConn =
              (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
          final NettyHttpDatabusRelayConnectionInspector new2RelayConnInsp =
              new NettyHttpDatabusRelayConnectionInspector(new2RelayConn);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != new2RelayConnInsp.getChannel() && new2RelayConnInsp.getChannel().isConnected();
            }
          }, "client connected to third relay", 200, log);

          //figure out the connection to the relay
          clientChannel = new2RelayConnInsp.getChannel();
          relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
          clientAddr = clientChannel.getLocalAddress();
          relayPort = relayAddr.getPort();
          log.info("third relay selected: " + relayPort);


          relay = null;
          relayIdx = 0;
          for (; relayIdx < RELAY_PORT.length; ++relayIdx)
          {
            if (relayPort == RELAY_PORT[relayIdx]) relay = _dummyServer[relayIdx];
          }
          assertTrue(null != relay);

          serverChannel = relay.getChildChannel(clientAddr);
          assertTrue(null != serverChannel);
          serverPipeline = serverChannel.getPipeline();
          objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
          objCapture.clear();

          //send back the /sources response
          body = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
                                      SOURCE1_NAME + "\"}]").getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
          NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          //send back the /register response
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /sources response", 100, log);

          //process /stream call and return a partial window
          streamMatcher =
              NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*checkPoint=([^&]*)&.*",
                                                1000);
          cpString = streamMatcher.group(1);
          objCapture.clear();

          respStartOfs = eventOfs.get(2).get(20);
          respEndOfs = eventOfs.get(2).get(89);

          log.debug("Checkpoint String is :" + cpString);


          cp = new Checkpoint(cpString);
          //last window read was partial. So the client would have reset the windowOffset
          assertTrue("Is WindowOffset Cleared",cp.getWindowOffset() == -1);
          assertEquals( "WindowSCN == PrevSCN. Ckpt :" + cp, cp.getWindowScn(), cp.getPrevScn());
          streamRes = NettyTestUtils.streamToChannelBuffer(relayBuffer[2], cp,
                                                           respEndOfs - respStartOfs,
                                                           stats);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamRes));

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("lastWrittenScn=" + clientConn.getDataEventsBuffer().lastWrittenScn());
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 90;
            }
          }, "client receieves /stream response, Sequence :" + consumer.getSequences(), 10100, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", 110000, log);

          assertEquals(30, consumer.getRollbackScn());
          //add one more onStartDataEventsSequence because of the rollback
          ++rollbackNum;
          assertEquals(stats.getTotalStats().getNumSysEvents() + 1 + rollbackNum, consumer.getWinNum());

          expKeys.addAll(eventKeys.get(2).subList(20, (int)(stats.getTotalStats().getNumDataEvents() - numEvents + 20)));
          for (int i = 0; i < (stats.getTotalStats().getNumDataEvents() - numEvents); i++)
        	  expSeqs.add((i/10)*10 + 30L);

          numEvents = stats.getTotalStats().getNumDataEvents();

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences Size ", expSeqs.size(), consumer.getSequences().size());
          for (int i = 0; i <expSeqs.size(); i++ )
          {
        	  System.out.println(i + " Exp : " + expSeqs.get(i) + ", Got :" + consumer.getSequences().get(i));
        	  if ( expSeqs.get(i) != consumer.getSequences().get(i))
        	     throw new RuntimeException("Failed at "+ i);
          }
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          numEvents = stats.getTotalStats().getNumDataEvents();
          assertEquals(clientConn.getRelayPullThread().getConnectionState().getDataEventsBuffer().isSCNRegress(), false);

          // Expect none of the keys streamed to be missed.
          expKeys.clear();
          cp = clientConn.getRelayPullThread().getConnectionState().getCheckpoint();
          int offset = (int)((cp.getWindowOffset() < 0) ? 0 : cp.getWindowOffset());
          LOG.info("Client ckpt is :" + cp);
          expKeys.addAll(eventKeys.get(2).subList(0, 80 + offset));
          for(int i = 0; i < expKeys.size(); i++)
          {
        	  if (! consumer.getKeys().contains(expKeys.get(i)))
        	  {
        		  log.error(i + " Key :" + expKeys.get(i) + " missing !!");
         	      throw new RuntimeException("Failed at "+ i);
        	  }
        	  else
        		  log.info(i + " Key :" + expKeys.get(i) + " present !!");
          }
	    } finally {
	    	client.shutdown();
	    }
	}


	  @Test
		/**
		 *
		 * R1 EVB Boundaries --------------------------------------------------------------------------------
		 *                   ^      ^        ^         ^         ^       ^       ^      ^     ^        ^
		 *                   0      10       20        30        40      50      60     70    80       90
		 *
		 * R2 EVB Boundaries --------------------------------------------------------------------------------
		 *                   ^               ^                   ^               ^            ^
		 *                   0               20                  40              60           80
		 *
		 * R3 EVB Boundaries ------------------------------------------------------------------------------
		 *                   ^                        ^                          ^                    ^
		 *                   0                        30                         60                   90
		 *
		 *  Client switches from R1 to R2 on server close and from R2 to R3 on timeout while in the middle of the windows.
		 *
		 *  Switch from R1 -> R2 happens when windowScn = 10 and WindowOffset = 8th event
		 *  Switch from R2 -> R3 happens when windowScn = 40 and windowOffset = 3rd event
		 *
		 * @throws Exception
		 */
	  public void testRelayFailoverPartialWindow1() throws Exception
	  {
	    final boolean debugOn = false;
	    final Logger log = Logger.getLogger("TestDatabusHttpClient.testRelayFailoverPartialWindow1");
	    log.setLevel(Level.INFO);
	    final int eventsNum = 200;
	    DbusEventInfo[] events = createSampleSchema1Events(eventsNum);
	    final long timeoutMult = debugOn ? 100000 : 1;

	    //simulate relay buffers
	    DbusEventBuffer[] relayBuffer = new DbusEventBuffer[RELAY_PORT.length];
	    List<List<Integer>> eventOfs = new ArrayList<List<Integer>>(3);
	    List<List<DbusEventKey>> eventKeys = new ArrayList<List<DbusEventKey>>(3);

	    for (int i = 0; i < RELAY_PORT.length; ++i)
	    {
	      relayBuffer[i] = new DbusEventBuffer(_bufCfg);
	      relayBuffer[i].start(0);
	      WriteEventsResult wrRes = writeEventsToBuffer(relayBuffer[i], events, (i + 1) * 10);
	      List<Integer> ofs = wrRes.getOffsets();
	      eventOfs.add(ofs);
	      eventKeys.add(wrRes.getKeys());
	    }

	    List<DbusEventKey> key = eventKeys.get(0);
	    for (int i = 1; i < RELAY_PORT.length; ++i)
	    {
	    	assertEquals(" For Size index : " + i,key.size(), eventKeys.get(i).size());
	    	assertEquals(" For index : " + i,key, eventKeys.get(i));
	    	key = eventKeys.get(i);
	    }

	    //figure out an event offset inside a window
	    int resp1EnfOfs = eventOfs.get(0).get(8);

	    //create client
	    _stdClientCfgBuilder.getContainer().setReadTimeoutMs(DEFAULT_READ_TIMEOUT_MS);

	    final DatabusHttpClientImpl client = new DatabusHttpClientImpl(_stdClientCfgBuilder.build());

	    final TestConsumer consumer = new TestConsumer();
	    client.registerDatabusStreamListener(consumer, null, SOURCE1_NAME);

	    client.start();
	    try
	    {
	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return client._relayConnections.size() == 1;
            }
          }, "sources connection present", 100, log);

	      final DatabusSourcesConnection clientConn = client._relayConnections.get(0);
	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != clientConn.getRelayPullThread().getLastOpenConnection();
            }
          }, "relay connection present", 100, log);

	      final NettyHttpDatabusRelayConnection relayConn =
	          (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
	      final NettyHttpDatabusRelayConnectionInspector relayConnInsp =
	          new NettyHttpDatabusRelayConnectionInspector(relayConn);

	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != relayConnInsp.getChannel() && relayConnInsp.getChannel().isConnected();
            }
          }, "client connected", 200, log);

	      //figure out the connection to the relay
	      Channel clientChannel = relayConnInsp.getChannel();
	      InetSocketAddress relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
	      SocketAddress clientAddr = clientChannel.getLocalAddress();
	      int relayPort = relayAddr.getPort();
	      log.info("relay selected: " + relayPort);

	      SimpleTestServerConnection relay = null;
	      for (int i = 0; i < RELAY_PORT.length; ++i)
	      {
	        if (relayPort == RELAY_PORT[i]) relay = _dummyServer[i];
	      }
	      assertTrue(null != relay);

	      final SocketAddress testClientAddr = clientAddr;
	      final SimpleTestServerConnection testRelay = relay;
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != testRelay.getChildChannel(testClientAddr);
            }
          }, "relay detects new connection", 1000, log);

	      Channel serverChannel = relay.getChildChannel(clientAddr);
	      assertTrue(null != serverChannel);
	      ChannelPipeline serverPipeline = serverChannel.getPipeline();
	      SimpleObjectCaptureHandler objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
	      objCapture.clear();

	      //send back the /sources response
	      HttpResponse sourcesResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
	                                                         HttpResponseStatus.OK);
	      sourcesResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
	      sourcesResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
	      HttpChunk body =
	          new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
	                               SOURCE1_NAME + "\"}]").getBytes()));
	      NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

	      //make sure the client processes the response correctly
	      TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
	      NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          //send back the /register response
          RegisterResponseEntry entry = new RegisterResponseEntry(1L, (short)1, SOURCE1_SCHEMA_STR);
          String responseStr = NettyTestUtils.generateRegisterResponse(entry);
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", 100, log);

          //process /stream call and return a partial window
          NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*", 1000);
          objCapture.clear();

          //send back the /stream response
          final DbusEventsStatisticsCollector stats =
              new DbusEventsStatisticsCollector(1, "test1", true, false, null);
          Checkpoint cp = Checkpoint.createFlexibleCheckpoint();
          ChannelBuffer streamRes = NettyTestUtils.streamToChannelBuffer(relayBuffer[0], cp,
                                                                          resp1EnfOfs, stats);
          HttpResponse streamResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                             HttpResponseStatus.OK);
          streamResp.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          streamResp.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamRes));

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              LOG.debug("LastWritten SCN:" + clientConn.getDataEventsBuffer().lastWrittenScn() );
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 10;
            }
          }, "client receieves /stream response", 1100, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", 110000, log);
          assertEquals(-1, consumer.getRollbackScn());
          int rollbackNum = 0;
          assertEquals(stats.getTotalStats().getNumSysEvents() + 1 + rollbackNum, consumer.getWinNum());

          List<DbusEventKey> expKeys = eventKeys.get(0).subList(0, (int)stats.getTotalStats().getNumDataEvents());
          List<Long> expSeqs = new ArrayList<Long>();
          for (int i = 0; i < stats.getTotalStats().getNumDataEvents(); i++)
        	  expSeqs.add(10L);

          long numEvents = stats.getTotalStats().getNumDataEvents();

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          //now kill the relay and wait for a failover
          serverChannel.close();

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != relayConnInsp.getChannel() && !relayConnInsp.getChannel().isConnected();
            }
          }, "client disconnected", 200, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return clientConn.getRelayPullThread().getLastOpenConnection() != relayConn;
            }
          }, "new netty connection", 200, log);

          /////////// FAKING CONNECTION TO NEW RELAY

          final NettyHttpDatabusRelayConnection newRelayConn =
              (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
          final NettyHttpDatabusRelayConnectionInspector newRelayConnInsp =
              new NettyHttpDatabusRelayConnectionInspector(newRelayConn);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != newRelayConnInsp.getChannel() && newRelayConnInsp.getChannel().isConnected();
            }
          }, "client connected to new relay", 200, log);

          //figure out the connection to the relay
          clientChannel = newRelayConnInsp.getChannel();
          relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
          clientAddr = clientChannel.getLocalAddress();
          relayPort = relayAddr.getPort();
          log.info("new relay selected: " + relayPort);

          relay = null;
          int relayIdx = 0;
          for (; relayIdx < RELAY_PORT.length; ++relayIdx)
          {
            if (relayPort == RELAY_PORT[relayIdx]) relay = _dummyServer[relayIdx];
          }
          assertTrue(null != relay);

          serverChannel = relay.getChildChannel(clientAddr);
          assertTrue(null != serverChannel);
          serverPipeline = serverChannel.getPipeline();
          objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
          objCapture.clear();

          //send back the /sources response
          body = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
                                      SOURCE1_NAME + "\"}]").getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
          NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          //send back the /register response
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", 100, log);

          //process /stream call and return a partial window
          Matcher streamMatcher =
              NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*checkPoint=([^&]*)&.*",
                                                1000);
          String cpString = streamMatcher.group(1);
          objCapture.clear();

          int respStartOfs = eventOfs.get(1).get(1);
          int respEndOfs = eventOfs.get(1).get(34);

          cp = new Checkpoint(cpString);
          assertTrue(cp.getWindowOffset() > 0); //last window read was partial
          streamRes = NettyTestUtils.streamToChannelBuffer(relayBuffer[1], cp,
                                                           respEndOfs - respStartOfs,
                                                           stats);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamRes));

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("lastWrittenScn=" + clientConn.getDataEventsBuffer().lastWrittenScn());
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 40;
            }
          }, "client receieves /stream response", 1100, log);


          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", 11000, log);
          assertEquals(20, consumer.getRollbackScn());
          //one more onStartDataEventSequence because of the rolback
          ++rollbackNum;
          assertEquals(stats.getTotalStats().getNumSysEvents() + 1 + rollbackNum, consumer.getWinNum());

          assertEquals(clientConn.getRelayPullThread().getConnectionState().getDataEventsBuffer().isSCNRegress(), false);
          expKeys.addAll(eventKeys.get(1).subList(0, (int)(stats.getTotalStats().getNumDataEvents() - numEvents)));
          for (int i = 0; i < stats.getTotalStats().getNumDataEvents() - numEvents; i++)
        	  expSeqs.add((i/20)*20 + 20L);

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          numEvents = stats.getTotalStats().getNumDataEvents();

          ///////////////////////////////////
          //simulate a timeout on the server; the client would have sent a /stream call and there
          //will be no response from the server, so eventually it should time out and switch servers
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("Channel :" + newRelayConnInsp.getChannel());
              return (null != newRelayConnInsp.getChannel()) && (!newRelayConnInsp.getChannel().isConnected());
            }
          }, "waiting for a reconnect", (long)(DEFAULT_READ_TIMEOUT_MS * 1.5), log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return clientConn.getRelayPullThread().getLastOpenConnection() != relayConn;
            }
          }, "new netty connection", 200, log);

          final NettyHttpDatabusRelayConnection new2RelayConn =
              (NettyHttpDatabusRelayConnection)clientConn.getRelayPullThread().getLastOpenConnection();
          final NettyHttpDatabusRelayConnectionInspector new2RelayConnInsp =
              new NettyHttpDatabusRelayConnectionInspector(new2RelayConn);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              return null != new2RelayConnInsp.getChannel() && new2RelayConnInsp.getChannel().isConnected();
            }
          }, "client connected to third relay", 200, log);

          //figure out the connection to the relay
          clientChannel = new2RelayConnInsp.getChannel();
          relayAddr = (InetSocketAddress)clientChannel.getRemoteAddress();
          clientAddr = clientChannel.getLocalAddress();
          relayPort = relayAddr.getPort();
          log.info("third relay selected: " + relayPort);


          relay = null;
          relayIdx = 0;
          for (; relayIdx < RELAY_PORT.length; ++relayIdx)
          {
            if (relayPort == RELAY_PORT[relayIdx]) relay = _dummyServer[relayIdx];
          }
          assertTrue(null != relay);

          serverChannel = relay.getChildChannel(clientAddr);
          assertTrue(null != serverChannel);
          serverPipeline = serverChannel.getPipeline();
          objCapture = (SimpleObjectCaptureHandler)serverPipeline.get("3");

          //process the /sources request
          NettyTestUtils.waitForHttpRequest(objCapture, "^/sources", 1000);
          objCapture.clear();

          //send back the /sources response
          body = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(("[{\"id\":1,\"name\":\"" +
                                      SOURCE1_NAME + "\"}]").getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              String idListString = clientConn.getRelayPullThread()._currentState.getSourcesIdListString();
              return "1".equals(idListString);
            }
          }, "client processes /sources response", 100, log);

          //process the /register request
          NettyTestUtils.waitForHttpRequest(objCapture, "/register.*", 1000);
          objCapture.clear();

          log.info("SEND BACK THE /register RESPONSE");
          clientConn.getRelayDispatcher().getLog().setLevel(Level.DEBUG);
          RangeBasedReaderWriterLock.LOG.setLevel(Level.DEBUG);
          body = new DefaultHttpChunk(
              ChannelBuffers.wrappedBuffer(responseStr.getBytes()));
          NettyTestUtils.sendServerResponses(relay, clientAddr, sourcesResp, body);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              DispatcherState dispState = clientConn.getRelayDispatcher().getDispatcherState();
              return null != dispState.getSchemaMap() && 1 == dispState.getSchemaMap().size();
            }
          }, "client processes /register response", timeoutMult * 100, log);

          log.info("PROCESS the /stream CALL AND RETURN A PARTIAL WINDOW");
          streamMatcher =
              NettyTestUtils.waitForHttpRequest(objCapture, "/stream.*checkPoint=([^&]*)&.*",
                                                1000);
          cpString = streamMatcher.group(1);
          objCapture.clear();

          respStartOfs = eventOfs.get(2).get(1);
          respEndOfs = eventOfs.get(2).get(84);

          log.debug("Checkpoint String is :" + cpString);

          cp = new Checkpoint(cpString);
          //last window read was partial. So the client would have reset the windowOffset
          assertTrue("Is WindowOffset Cleared",cp.getWindowOffset() == -1);
          assertEquals( "WindowSCN == PrevSCN. Ckpt :" + cp, cp.getWindowScn(), cp.getPrevScn());
          streamRes = NettyTestUtils.streamToChannelBuffer(relayBuffer[2], cp,
                                                           respEndOfs - respStartOfs,
                                                           stats);
          NettyTestUtils.sendServerResponses(relay, clientAddr, streamResp,
                                             new DefaultHttpChunk(streamRes));

          log.debug("NumEvents already seen :" + numEvents);

          //make sure the client processes the response correctly
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("lastWrittenScn=" + clientConn.getDataEventsBuffer().lastWrittenScn() + ", NumEvents :" + stats.getTotalStats().getNumDataEvents() );
              return clientConn.getDataEventsBuffer().lastWrittenScn() == 90;
            }
          }, "client receieves /stream response, Sequences :" + consumer.getSequences(),
             timeoutMult * 1100, log);

          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              log.debug("events num=" + consumer.getEventNum());
              return stats.getTotalStats().getNumDataEvents() == consumer.getEventNum();
            }
          }, "client processes /stream response", timeoutMult * 1100, log);

          //one more onStartDataEventSequence because of the rolback
          assertEquals(30, consumer.getRollbackScn());
          ++rollbackNum;
          assertEquals(stats.getTotalStats().getNumSysEvents() + 1 + rollbackNum, consumer.getWinNum());

          expKeys.addAll(eventKeys.get(2).subList(0, (int)(stats.getTotalStats().getNumDataEvents() - numEvents)));
          for (int i = 0; i < stats.getTotalStats().getNumDataEvents() - numEvents; i++)
        	  expSeqs.add((i/30)*30 + 30L);

          assertEquals("Keys", expKeys, consumer.getKeys());
          assertEquals("Sequences", expSeqs, consumer.getSequences());

          numEvents = stats.getTotalStats().getNumDataEvents();

          assertEquals(clientConn.getRelayPullThread().getConnectionState().getDataEventsBuffer().isSCNRegress(), false);
	    }
	    finally
	    {
	      client.shutdown();
	    }
	  }

	@Test
  public void testStartNoConsumers() throws Exception
  {
    DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
    clientConfig.getContainer().getJmx().setRmiEnabled(false);
    clientConfig.getContainer().setHttpPort(10004);
    final DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConfig);

    Thread startThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
         client.start();
      }
    }, "client start thread");
    startThread.setDaemon(true);
    startThread.start();

    try
    {
      startThread.join(5000);
    }
    catch (InterruptedException e)
    {
      LOG.warn("interrupted");
    }

    assertTrue(!startThread.isAlive());
  }

  private static <T>int safeListSize(List<T> l)
	{
		return null == l ? 0 : l.size();
	}

  static DbusEventInfo[] createSampleSchema1Events(int eventsNum) throws IOException
  {
    Random rng = new Random();
    DbusEventInfo[] result = new DbusEventInfo[eventsNum];
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(SOURCE1_SCHEMA);
    for (int i = 0; i < eventsNum; ++i)
    {
      GenericRecord r = new GenericData.Record(SOURCE1_SCHEMA);
      String s = RngUtils.randomString(rng.nextInt(100));
      r.put("s", s);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(s.length() + 100);
      BinaryEncoder out = new BinaryEncoder(baos);
      try
      {
        writer.write(r, out);
        out.flush();

        result[i] = new DbusEventInfo(DbusOpcode.UPSERT, 1, (short)1, (short)1,
                                      System.nanoTime(),
                                      (short)1, SOURCE1_SCHEMAID,
                                      baos.toByteArray(), false, true);
      }
      finally
      {
        baos.close();
      }
    }

    return result;
  }

  public static class WriteEventsResult
  {
	  private final List<Integer> offsets;
	  private final List<DbusEventKey> keys;

	  public WriteEventsResult(List<Integer> offsets, List<DbusEventKey> keys) {
		  super();
		  this.offsets = offsets;
		  this.keys = keys;
	  }

	  public List<Integer> getOffsets() {
		  return offsets;
	  }

	  public List<DbusEventKey> getKeys() {
		  return keys;
	  }
  }

  static WriteEventsResult writeEventsToBuffer(DbusEventBuffer buf, DbusEventInfo[] events,
                                               int winSize)
         throws UnsupportedEncodingException
  {
    Random rng = new Random(100);
    final List<Integer> eventOfs = new ArrayList<Integer>(events.length);
    final List<DbusEventKey> eventKeys = new ArrayList<DbusEventKey>(events.length);

    InternalDatabusEventsListener ofsTracker = new InternalDatabusEventsListener()
    {
      @Override
      public void close() throws IOException {}

      @Override
      public void onEvent(DataChangeEvent event, long offset, int size)
      {
        DbusEvent e = (DbusEvent)event;
        eventOfs.add(e.getRawBytes().position() - e.size());
      }
    };
    buf.addInternalListener(ofsTracker);

    for (int i = 0; i < events.length; ++i)
    {
      if (i % winSize == 0)
      {
        if (i > 0) buf.endEvents(i);
        buf.startEvents();
      }
      DbusEventKey key =
          new DbusEventKey(RngUtils.randomString(rng, rng.nextInt(50)).getBytes("UTF-8"));
      buf.appendEvent(key, events[i], null);
      eventKeys.add(key);
    }
    buf.endEvents(events.length);

    buf.removeInternalListener(ofsTracker);

    return new WriteEventsResult(eventOfs, eventKeys);
  }
}

class DummyStreamConsumer implements DatabusStreamConsumer
{
	private final String _name;
	public int _errorCount = 0;

	public DummyStreamConsumer(String name) {
		super();
		_name = name;
	}

	@Override
	public ConsumerCallbackResult onCheckpoint(SCN checkpointScn) {
		return ConsumerCallbackResult.ERROR;
	}

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder) {
		return ConsumerCallbackResult.ERROR;
	}

	@Override
	public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
	  return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema) {
      return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onRollback(SCN startScn) {
      return ConsumerCallbackResult.ERROR;
	}

	@Override
	public ConsumerCallbackResult onStartDataEventSequence(SCN startScn) {
      return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema) {
      return ConsumerCallbackResult.SUCCESS;
	}

	public String getName() {
		return _name;
	}

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return ConsumerCallbackResult.ERROR;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return ConsumerCallbackResult.ERROR;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
	_errorCount++;
    return ConsumerCallbackResult.SUCCESS;
  }
}

class TestConsumer extends AbstractDatabusCombinedConsumer
{
  public static final String MODULE = TestConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private int _eventNum;
  private int _winNum;
  private long _rollbackScn;

  private final List<DbusEventKey> keys = new ArrayList<DbusEventKey>();
  private final List<Long> sequences = new ArrayList<Long>();

  public TestConsumer()
  {
    resetCounters();
  }

  public void resetCounters()
  {
    _eventNum = 0;
    _winNum = 0;
    _rollbackScn = -1;

  }

  public void resetEvents()
  {
	  keys.clear();
	  sequences.clear();
  }

  public List<DbusEventKey> getKeys() {
	return keys;
}

public List<Long> getSequences() {
	return sequences;
}

protected int getEventNum()
  {
    return _eventNum;
  }

  protected int getWinNum()
  {
    return _winNum;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    ++ _eventNum;
    if ( (! e.isCheckpointMessage()) && (!e.isControlMessage()))
    {
    	sequences.add(e.sequence());
    	if ( e.isKeyNumber())
    		keys.add(new DbusEventKey(e.key()));
    	else
    		keys.add(new DbusEventKey(e.keyBytes()));
    }
    LOG.info("TestConsumer: OnDataEvent : Sequence : " + e.sequence());
    return super.onDataEvent(e, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    ++ _winNum;
    return super.onStartDataEventSequence(startScn);
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    if (startScn instanceof SingleSourceSCN)
    {
      SingleSourceSCN s = (SingleSourceSCN)startScn;
      _rollbackScn = s.getSequence();
    } else {
    	throw new RuntimeException("SCN not instance of SingleSourceSCN");
    }
    return super.onRollback(startScn);
  }

  protected long getRollbackScn()
  {
    return _rollbackScn;
  }

}

/**
 * All callbacks succeed for this consumer
 *
 * @author pganti
 *
 */
class DummySuccessfulErrorCountingConsumer implements DatabusStreamConsumer
{
	private final String _name;
	public int _errorCount;
	private final boolean _returnErrorsOnDataEvent;

	public DummySuccessfulErrorCountingConsumer(String name, boolean returnErrorsOnDataEvent) {
		super();
		_name = name;
		_errorCount = 0;
		_returnErrorsOnDataEvent = returnErrorsOnDataEvent;
	}

	@Override
	public ConsumerCallbackResult onCheckpoint(SCN checkpointScn) {
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder) {
		if (_returnErrorsOnDataEvent)
			return ConsumerCallbackResult.ERROR;
		else
			return ConsumerCallbackResult.SUCCESS;

	}

	@Override
	public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
	  return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema) {
      return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onRollback(SCN startScn) {
      return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartDataEventSequence(SCN startScn) {
      return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema) {
      return ConsumerCallbackResult.SUCCESS;
	}

	public String getName() {
		return _name;
	}

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
	_errorCount++;
    return ConsumerCallbackResult.SUCCESS;
  }
}
