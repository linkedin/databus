package com.linkedin.databus3.espresso.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.RuntimeConfigBuilder;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusServerCoordinates.StateId;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.DeregisterResult;
import com.linkedin.databus.client.pub.FlushRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RegistrationState;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.pub.StartResult;
import com.linkedin.databus.core.CompoundDatabusComponentStatus;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;
import com.linkedin.databus3.espresso.client.cmclient.RelayClusterInfoProvider;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumer;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumerRegistration;
import com.linkedin.databus3.espresso.client.data_model.EspressoSubscriptionUriCodec;

/** Unit tests for the EspressoDatabusClient */
public class TestEspressoDatabusHttpClient
{
    public static final Logger LOG = Logger.getLogger("TestEspressoDatabusHttpClient");
    static final Schema SOURCE1_SCHEMA =
        Schema.parse("{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}");
    static final String SOURCE1_SCHEMA_STR = SOURCE1_SCHEMA.toString();
    static final byte[] SOURCE1_SCHEMAID = SchemaHelper.getSchemaId(SOURCE1_SCHEMA_STR);
    static final ExecutorService BOSS_POOL = Executors.newCachedThreadPool();
    static final ExecutorService IO_POOL = Executors.newCachedThreadPool();
    static final int[] RELAY_PORT = {14471, 14472, 14473};
    static final int CLIENT_PORT = 14470;
    static final InetSocketAddress[] RELAY_ADDR =
      {new InetSocketAddress(RELAY_PORT[0]), new InetSocketAddress(RELAY_PORT[1]),
       new InetSocketAddress(RELAY_PORT[2])};
    static final Timer NETWORK_TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
    static final ChannelGroup TEST_CHANNELS_GROUP = new DefaultChannelGroup();
    static final long DEFAULT_READ_TIMEOUT_MS = 10000;
    static final long DEFAULT_WRITE_TIMEOUT_MS = 10000;
    static final String SOURCE1_NAME = "test.event.source1";
    static SimpleTestServerConnection[] _dummyServer = new SimpleTestServerConnection[RELAY_PORT.length];
    static DbusEventBuffer.StaticConfig _bufCfg;
    static DatabusHttpClientImpl.Config _stdClientCfgBuilder;

	private DatabusHttpV3ClientImpl _client;

	@BeforeClass
	public void setupClass() throws InvalidConfigException
	{
      TestUtil.setupLogging(true, null, Level.ERROR);
      //workaround for TestNG not running static initialization of EspressoSubscriptionUriCodec
      EspressoSubscriptionUriCodec.getInstance();

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

      //create standard relay buffer config
      DbusEventBuffer.Config bufCfgBuilder = new DbusEventBuffer.Config();
      bufCfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
      bufCfgBuilder.setMaxSize(100000);
      bufCfgBuilder.setScnIndexSize(128);
      bufCfgBuilder.setReadBufferSize(1);

      _bufCfg = bufCfgBuilder.build();
	}

	@BeforeMethod
  public void setUp() throws Exception
	{
	}

	@AfterMethod
  public void tearDown() throws Exception
	{
		_client = null;
	}

	private ServerInfo registerRelay(int id, String name, InetSocketAddress addr, String sources)
	throws InvalidConfigException
	{
		RuntimeConfigBuilder rtConfigBuilder =
			(RuntimeConfigBuilder)_client.getClientConfigManager().getConfigBuilder();

		ServerInfoBuilder relayConfigBuilder = rtConfigBuilder.getRelay(Integer.toString(id));
		relayConfigBuilder.setName(name);
		relayConfigBuilder.setHost(addr.getHostName());
		relayConfigBuilder.setPort(addr.getPort());
		relayConfigBuilder.setSources(sources);

		_client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
		return relayConfigBuilder.build();
	}

    private void registerRelayV3(int id, String name, InetSocketAddress addr, DatabusSubscription ds)
    throws InvalidConfigException
    {
      registerRelayV3(_client, id, name, addr, ds);
    }

    private static void registerRelayV3(DatabusHttpV3ClientImpl client, int id, String name,
                                        InetSocketAddress addr, DatabusSubscription ds)
    throws InvalidConfigException
    {
        RuntimeConfigBuilder rtConfigBuilder =
            (RuntimeConfigBuilder)client.getClientConfigManager().getConfigBuilder();

        ServerInfoBuilder relayConfigBuilder = rtConfigBuilder.getRelay(Integer.toString(id));
        relayConfigBuilder.setName(name);
        relayConfigBuilder.setHost(addr.getHostName());
        relayConfigBuilder.setPort(addr.getPort());
        //relayConfigBuilder.setSources("BizProfile.BizCompany:77");
        relayConfigBuilder.setSources(DatabusSubscription.createStringFromSubscription(ds));
        client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
    }

	private static <T>int safeListSize(List<T> l)
	{
		return null == l ? 0 : l.size();
	}
	@Test
	public void testSourcenameSubscriptionIdempotency1()
	throws Exception
	{
		String source = "com.linkedin.events.member2.profile.MemberProfile";
		DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription(source);
		String source2 = ds.getLogicalSource().getName();
		assertEquals("Compare from Sub to source and back", source, source2);

		List<String> sourceList = Arrays.asList(source);
		List<DatabusSubscription> subList	= DatabusSubscription.createSubscriptionList(sourceList);
		List<String> sourceList2 = DatabusSubscription.getStrList(subList);
		assertEquals("Compare from SubList to StrList with orig StrList", sourceList, sourceList2);
		return;
	}

	// This test currently commented out because when the source is specified in this format, it is assumed to be V3.
	// TODO This bug to be fixed: DDSDBUS-409
	  @Test(enabled=false)
	public void testSourcenameSubscriptionIdempotency2()
	throws Exception
	{
		String source = "sdr.sdr_people_search_p1_6";
		DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription(source);
		ds.getLogicalSource().getName();

		List<String> sourceList = Arrays.asList(source);
		List<DatabusSubscription> subList	= DatabusSubscription.createSubscriptionList(sourceList);
		List<String> sourceList2 = DatabusSubscription.getStrList(subList);
		assertEquals(sourceList, sourceList2);
		return;
	}

  // TODO This bug to be fixed: DDSDBUS-409
  @Test(enabled=false)
  public void testSourcenameSubscriptionIdempotency3()
	throws Exception
	{
		String source = "EspressoDB.Email:3";
		DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscriptionV3(source);

		String ps = ds.getPhysicalPartition().getName();
		String ls = ds.getLogicalSource().getName();
		Integer ppNum = ds.getPhysicalPartition().getId();
		short lpNum = ds.getLogicalPartition().getId();
		assertEquals("Check logical partition num = physical partition num", ppNum.shortValue(), lpNum);
		String source2 = ps + "." + ls + ":" + ppNum.intValue();
		assertEquals("Compare from Sub to source and back", source, source2);

		List<String> sourceList = Arrays.asList(source);
		List<DatabusSubscription> subList	= DatabusSubscription.createSubscriptionList(sourceList);
		List<String> sourceList2 = DatabusSubscription.getStrList(subList);
		assertEquals("Compare from SubList to StrList with orig StrList", sourceList, sourceList2);
		return;
	}
	@Test
	public void testSourcenameSubscriptionIdempotency4()
	throws Exception
	{
		String source = "EspressoDB.*:3";
		DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscriptionV3(source);

		String ps = ds.getPhysicalPartition().getName();
		String ls = ds.getLogicalSource().getName();
		Integer ppNum = ds.getPhysicalPartition().getId();
		short lpNum = ds.getLogicalPartition().getId();
		assertEquals("Check logical partition num = physical partition num", ppNum.shortValue(), lpNum);
		String source2 = ps + "." + ls + ":" + ppNum.intValue();
		assertEquals("Compare from Sub to source and back", source, source2);

		List<String> sourceList = Arrays.asList(source);
		List<DatabusSubscription> subList	= DatabusSubscription.createSubscriptionList(sourceList);
		List<String> sourceList2 = DatabusSubscription.getStrList(subList);
		assertEquals("Compare from SubList to StrList with orig StrList", sourceList, sourceList2);
		return;
	}


	@Test
	public void testEspressoDatabusStorageUseCase1() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
	      new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  _client = new DatabusHttpV3ClientImpl(clientConfig.build());
	  _client.setClientStarted(true);

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("Consumer_Subscription1");

      String uri = "esv4-app80.stg.linkedin.com:10015"; // Storage node URI on dev cluster

      PhysicalSource.Builder psb = new PhysicalSource.Builder();
      psb.setUri(uri);
      PhysicalSource ps = psb.build();
      ps = PhysicalSource.ANY_PHISYCAL_SOURCE; // for test

      PhysicalPartition.Builder ppb = new PhysicalPartition.Builder();
      int phyPartitionId = 77;
      ppb.setId(phyPartitionId);
      ppb.setName("BizProfile");
      PhysicalPartition pp = ppb.build();

      // Set ID for LogicalSourceId
      short lsId = 77;  // If wildcard
      // Set LogicalSource builder parameters
      LogicalSource.Builder lsb = new LogicalSource.Builder();
      lsb.makeAllSourcesWildcard();


      LogicalSourceId.Builder lsidb = new LogicalSourceId.Builder();
      lsidb.setId(lsId);
      lsidb.setSource(lsb);
      LogicalSourceId ls = lsidb.build();

      DatabusSubscription ds1 = new DatabusSubscription(ps, pp, ls);

      registerRelayV3(1, "relay1", new InetSocketAddress("localhost", 8888), ds1);

      RegistrationId rid = new RegistrationId("Reg1");
      _client.registerDatabusListener(listener1, rid, null, ds1);

      List<DatabusSubscription> ls1 = Arrays.asList(ds1);

      int numberOfConsumers = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1));
      assertEquals("one consumer + logging consumer", 2, (numberOfConsumers));
	}

	@Test
	public void testRegisterEspressoDatabusStreamListener() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
	      new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  clientConfig.getClusterManager().setEnabled(false);
	  _client = new DatabusHttpV3ClientImpl(clientConfig.build());
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "testDB.S1,testDB.S2");
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "testDB.S1,testDB.S3");
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "testDB.S1,testDB.S2");
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "testDB.S3,testDB.S4,testDB.S5");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds1 = DatabusSubscription.createFromUri("espresso:/testDB/*/S1");
      RegistrationId rid = new RegistrationId("Reg1");
      _client.registerDatabusListener(listener1, rid, null, ds1);
      assertTrue("CM should not be enabled", !_client.isCMEnabled());
      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("testDB.S1", "testDB.S2"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("testDB.S1", "testDB.S3"));
      List<DatabusSubscription> ls3 = DatabusSubscription.createSubscriptionList(Arrays.asList("testDB.S3", "testDB.S4", "testDB.S5"));

      int s1 = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1));
      int s2 = safeListSize(_client.getRelayGroupStreamConsumers().get(ls2));
      assertEquals("one consumer + logging consumer in (S1,S2) or (S1,S3), s1 :" + s1 + ",s2 :" + s2, 2,
                   safeListSize(_client.getRelayGroupStreamConsumers().get(ls1)) +
                   safeListSize(_client.getRelayGroupStreamConsumers().get(ls2)));

      DummyEspressoStreamConsumer listener2 = new DummyEspressoStreamConsumer("consumer2");
      RegistrationId rid2 = new RegistrationId("Reg2");
      _client.registerDatabusListener(listener2, rid2, null, ds1);
      int consumersNum = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1)) +
      safeListSize(_client.getRelayGroupStreamConsumers().get(ls2));
      assertTrue("two consumers + 1-2 logging consumer  in (S1,S2) or (S1,S3)", 3 <= consumersNum
                 && consumersNum <= 4);

      LoggingConsumer listener3 = _client.getLoggingListener();
      DatabusSubscription ds5 = DatabusSubscription.createFromUri("espresso:/testDB/*/S5");
      RegistrationId rid3 = new RegistrationId("Reg3");
      _client.registerDatabusListener(listener3, rid3, null, ds5);
      assertEquals("one consumers in (S3,S4,S5)", 1,
                   safeListSize(_client.getRelayGroupStreamConsumers().get(ls3)));
      assertEquals("consumer3 in (S3,S4,s5)", listener3, _client.getRelayGroupStreamConsumers().
                   get(ls3).get(0).getConsumers().get(0));
	}
    @Test
    public void testRegisterEspressoDatabusStreamListenerMissingSource() throws Exception
    {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1, S2");
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3");
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2");
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds10 = DatabusSubscription.createFromUri("espresso:/testDB/2/S10");
      RegistrationId rid = new RegistrationId("Reg1");
      _client.registerDatabusListener(listener1 , rid, null, ds10);
	}

	@Test
	public void testRegisterEspressoDatabusStreamListenerWrongSourceOrder() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3");
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2");
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S5");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds5 = DatabusSubscription.createSimpleSourceSubscription("S5");
      DatabusSubscription ds3 = DatabusSubscription.createSimpleSourceSubscription("S3");
      RegistrationId rid = new RegistrationId("Reg1");
      _client.registerDatabusListener(listener1 , rid, null, ds5, ds3);
	}

	@Test
	public void testUnregisterEspressoDatabusStreamListener() throws Exception
	{
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3");
      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2");
      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      _client.registerDatabusStreamListener(listener1 , null, "S1");
      _client.registerDatabusStreamListener(listener1 , null, "S2");

      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S2"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S3"));

      int consumersNum1 = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1)) +
          safeListSize(_client.getRelayGroupStreamConsumers().get(ls2));
      assertTrue("two consumers+ 1-2 logging consumer in (S1,S2) or (S1,S3)", 3 <= consumersNum1 &&
                 consumersNum1 <= 4);
      assertTrue("at least one consumer + logging consumer in (S1,S2)",
                 safeListSize(_client.getRelayGroupStreamConsumers().get(ls1))
                 >= 2);

      DummyEspressoStreamConsumer listener2 = new DummyEspressoStreamConsumer("consumer2");
      DatabusSubscription ds1 = DatabusSubscription.createSimpleSourceSubscription("S1");
      RegistrationId rid = new RegistrationId("Reg1");
      _client.registerDatabusListener(listener2 , rid, null, ds1);
      int consumersNum2 = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1)) +
      safeListSize(_client.getRelayGroupStreamConsumers().get(ls2));
      assertTrue("three consumers+ 1-2 logging consumers in (S1,S2) or (S1,S3)", 4 <= consumersNum2 &&
                 consumersNum2 <= 6);

      _client.unregisterDatabusStreamListener(listener1);
      int consumersNum3 = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1)) +
          safeListSize(_client.getRelayGroupStreamConsumers().get(ls2));
      //assertEquals("one consumer + 1-2 logging consumer in (S1,S2) or (S1,S3)", consumersNum2 - 2,
      //             consumersNum3);

      _client.unregisterDatabusStreamListener(listener1);
      int consumersNum4 = safeListSize(_client.getRelayGroupStreamConsumers().get(ls1)) +
          safeListSize(_client.getRelayGroupStreamConsumers().get(ls2));
      assertEquals("one consumer + 1-2 logging consumer  in (S1,S2) or (S1,S3)", consumersNum3,
                   consumersNum4);

      _client.unregisterDatabusStreamListener(listener2);
	}

	@Test
	public void testEspressoRegisterRelay() throws Exception
	{
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      List<DatabusSubscription> ls1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S2"));
      List<DatabusSubscription> sl1 = DatabusSubscription.createSubscriptionList(Arrays.asList("S2", "S1"));
      List<DatabusSubscription> ls2 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1", "S3"));
      List<DatabusSubscription> ls3 = DatabusSubscription.createSubscriptionList(Arrays.asList("S1"));
      List<DatabusSubscription> ls4 = DatabusSubscription.createSubscriptionList(Arrays.asList("S3", "S4", "S5"));
      List<DatabusSubscription> ls5 = DatabusSubscription.createSubscriptionList(Arrays.asList("S3", "S4"));

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8880), "S1,S2");
      assertEquals("one relay group", 1, _client.getRelayGroups().size());
      assertEquals("one relay", 1, _client.getRelays().size());
      assertEquals("relay group S1,S2", true,
                   _client.getRelayGroups().containsKey(ls1));
      assertEquals("no relay group S2,S1", false,
                   _client.getRelayGroups().containsKey(sl1));
      System.out.println("Num relays = " + _client.getRelayGroups().get(ls1).size());

      registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3");
      assertEquals("two relay groups", 2, _client.getRelayGroups().size());
      assertEquals("two relays", 2, _client.getRelays().size());
      assertEquals("relay group S1,S2", true, _client.getRelayGroups().containsKey(ls1));
      assertEquals("relay group S1,S3", true, _client.getRelayGroups().containsKey(ls2));
      assertEquals("no relay group S1", false, _client.getRelayGroups().containsKey(ls3));
      System.out.println("Num relays = " + _client.getRelayGroups().get(ls1).size());

      registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2");
      assertEquals("two relay groups", 2, _client.getRelayGroups().size());
      assertEquals("three relays", 3, _client.getRelays().size());
      assertEquals("S1,S2 has two relays", 2, _client.getRelayGroups().get(ls1).size());
      System.out.println("Num relays = " + _client.getRelayGroups().get(ls1).size());

      registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5");
      assertEquals("three relay groups", 3, _client.getRelayGroups().size());
      assertEquals("four relays", 4, _client.getRelays().size());
      assertEquals("relay group S1,S2", true, _client.getRelayGroups().containsKey(ls1));
      assertEquals("relay group S1,S3", true, _client.getRelayGroups().containsKey(ls2));
      assertEquals("relay group S3,S4,S5", true, _client.getRelayGroups().containsKey(ls4));
      assertEquals("no relay group S3,S4", false, _client.getRelayGroups().containsKey(ls5));
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testRegisterEspressoDatabusStreamListenerRegistrationNullId() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");

      // Illegal argument
      RegistrationId rid = new RegistrationId(null);
      _client.registerDatabusListener(listener1 , rid, null, ds);
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testRegisterEspressoDatabusStreamListenerRegistrationEmptyId() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");

      // Illegal argument
      RegistrationId rid = new RegistrationId("");
      _client.registerDatabusListener(listener1 , rid, null, ds);
	}

	@Test
	public void testRegisterEspressoDatabusStreamListenerRegistrationTestInputIdRespected() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");
      String id = "Rid";
      RegistrationId rid = new RegistrationId(id);
      DatabusV3Registration d3r = _client.registerDatabusListener(listener1 , rid, null, ds);
      assertEquals("Input registration id must be respected", d3r.getId().getId(), id);
	}

	@Test
	public void testRegisterId() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");
      DatabusV3Registration d3r = _client.registerDatabusListener(listener1 , null, null, ds);
      String id = d3r.getId().getId();

      String sn = listener1.getClass().getSimpleName();
      assertEquals(sn, "DummyEspressoStreamConsumer");
      assertEquals(true, id.startsWith("DummyEspressoStreamConsumer"));
	}

	@Test(expectedExceptions=DatabusClientException.class)
	public void testRegisterWithSameId() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");
      DatabusV3Registration d3r = _client.registerDatabusListener(listener1 , null, null, ds);
      RegistrationId rid = new RegistrationId(d3r.getId().getId());
      _client.registerDatabusListener(listener1 , rid, null, ds);
	}

	@Test(expectedExceptions=DatabusClientException.class)
	public void testInvalidParams1() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");

      DummyEspressoStreamConsumer listener1 = null;
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");

      _client.registerDatabusListener(listener1 , null, null, ds);
	}

	@Test(expectedExceptions=Exception.class)
	public void testInvalidParams2() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      _client.registerDatabusListener(listener1 , null, null, (DatabusSubscription)null);
	}

	@Test(expectedExceptions=DatabusClientException.class)
	public void testInvalidParams3() throws Exception {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2");

      DummyEspressoStreamConsumer[] listener1 = null;
      DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("S1");

      _client.registerDatabusListener(listener1 , null, null, ds);
	}

	@Test
	public void testExternalViewProcessing() throws Exception {
		final int testcaseNum = 1;
		RelayClusterInfoProvider rc= new RelayClusterInfoProvider(testcaseNum);
    	Map<String, RelayClusterInfoProvider> rcipMap =
    			new HashMap<String, RelayClusterInfoProvider>();
    	rcipMap.put("BizProfile", rc);

		DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
		_client = new DatabusHttpV3ClientImpl(clientConfig);
		_client.setClientStarted(true);

		DatabusSubscription ds = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
		List<DatabusSubscription> ld = new ArrayList<DatabusSubscription>();
		ld.add(ds);
		List<DatabusCombinedConsumer>  consumers = new ArrayList<DatabusCombinedConsumer>();
		consumers.add(new DummyEspressoStreamConsumer("dummy"));
		DatabusV3ConsumerRegistration reg =
		    new DatabusV3ConsumerRegistration(_client, "BizProfile", consumers,
		                                      new RegistrationId("dummy"), ld, null, null, null);

		RuntimeConfigBuilder rtConfigBuilder =	(RuntimeConfigBuilder)_client.getClientConfigManager().getConfigBuilder();
		_client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
		_client.updateRelayGroups(reg, rc);

		Map<List<DatabusSubscription>, Set<ServerInfo>> grc = _client.getRelayGroups();
		Set<ServerInfo> ssi = grc.get(ld);

		InetSocketAddress address = new InetSocketAddress("eat1-app25.stg.linkedin.com", 11140);
		ServerInfo si = new ServerInfo("default", StateId.ONLINE.name(), address, DatabusSubscription.createStringFromSubscription(ds));
		Set<ServerInfo> ssiExpected = new HashSet<ServerInfo>();
		ssiExpected.add(si);

		assertEquals(ssi, ssiExpected);
	}

	@Test
	public void testDatabusV3ClientSources() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
	      new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      ServerInfo s1i = registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      registerRelay(2, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:1");

      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      RegistrationId rid = new RegistrationId("Reg1");

      DatabusV3Registration dr1 = null;
      dr1 = _client.registerDatabusListener(listener1, rid, null, ds0);
      assertEquals("Num connections must be 0", 0, _client.getRelayConnections().size());

  	  dr1.start();
      assertEquals("Num connections must be 1", 1, _client.getRelayConnections().size());

      DatabusSourcesConnection dsc = _client.getRelayConnections().get(0);
      Set<ServerInfo> relaysInClient = dsc.getRelays();
      Set<ServerInfo> ssi = new HashSet<ServerInfo>();
      ssi.add(s1i);
      assertEquals(ssi, relaysInClient);
	}

	@Test
	public void testDatabusV3ClientTwoRelaysTwoPartitionsEach() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);

      ServerInfo s11 = registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:1");
      ServerInfo s21 = registerRelay(2, "relay2", new InetSocketAddress("localhost", 8889), "BizProfile.*:0");
      registerRelay(2, "relay2", new InetSocketAddress("localhost", 8889), "BizProfile.*:1");

      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DatabusSubscription ds1 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:1");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      //DummyEspressoStreamConsumer listener2 = new DummyEspressoStreamConsumer("consumer2");
      RegistrationId rid = new RegistrationId("Reg1");
      RegistrationId rid2 = new RegistrationId("Reg2");
      DatabusV3Registration r1 = null, r2 = null;
      try
      {
          r1 = _client.registerDatabusListener(listener1, rid, null, ds0);
      } catch (Exception e){
    	  LOG.info("Exception " + e);
      }

      try
      {
          r2 = _client.registerDatabusListener(listener1, rid2, null, ds1);
      } catch (Exception e){
    	  LOG.info("Exception " + e);
      }

      try
      {
    	  assertEquals("Num connections must be 0", 0, _client.getRelayConnections().size());
    	  r1.start();
      } catch (Exception e){
    	  LOG.info("Exception " + e);
      }

      try
      {
    	  assertEquals("Num connections must be 1", 1, _client.getRelayConnections().size());
    	  r2.start();
      } catch (Exception e){
    	  LOG.info("Exception " + e);
      }

      assertEquals("Num connections must be 2", 2, _client.getRelayConnections().size());

      DatabusSourcesConnection dsc = _client.getRelayConnections().get(0);
      Set<ServerInfo> relaysInClient = dsc.getRelays();
      Set<ServerInfo> ssi = new HashSet<ServerInfo>();
      ssi.add(s11);ssi.add(s21);
      if (!relaysInClient.equals(ssi))
      {
           throw new Exception("ServerInfo set is not what is expected");
      }
	}

	@Test
	public void testGetListOfConsumerRegsFromSubList()
	throws Exception
	{
		Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>> groupsListeners =
				new HashMap<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>>();

		DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("EspressoDB.*:0");
		DatabusSubscription ds1 = DatabusSubscription.createSimpleSourceSubscription("EspressoDB.*:1");
		DatabusSubscription ds2 = DatabusSubscription.createSimpleSourceSubscription("EspressoDB.*:2");

		List<DatabusSubscription> subsPresent = new ArrayList<DatabusSubscription>();
		subsPresent.add(ds0);
		subsPresent.add(ds1);

		List<DatabusV2ConsumerRegistration> cReg = new ArrayList<DatabusV2ConsumerRegistration>();

		// Interested in this subscription
		DummyEspressoStreamConsumer dsc = new DummyEspressoStreamConsumer("test");
		DatabusV2ConsumerRegistration cr = new DatabusV2ConsumerRegistration(dsc, DatabusSubscription.getStrList(subsPresent), null);

		// Update registration list
		cReg.add(cr);
		// Update group listeners
		groupsListeners.put(subsPresent, cReg);

		List<DatabusV2ConsumerRegistration> output = null;

		// Test 1
		output = DatabusHttpV3ClientImpl.getListOfConsumerRegsFromSubList(groupsListeners, Arrays.asList(ds0));
		assertEquals(true, output.contains(cr));
		assertEquals(1, output.size());

		// Test 2
		output = DatabusHttpV3ClientImpl.getListOfConsumerRegsFromSubList(groupsListeners, Arrays.asList(ds1));
		assertEquals(true, output.contains(cr));
		assertEquals(1, output.size());

		// Test 3
		output = DatabusHttpV3ClientImpl.getListOfConsumerRegsFromSubList(groupsListeners, Arrays.asList(ds0, ds1));
		assertEquals(true, output.contains(cr));
		assertEquals(1, output.size());

		// Test 4
		output = DatabusHttpV3ClientImpl.getListOfConsumerRegsFromSubList(groupsListeners, Arrays.asList(ds1, ds0));
		assertEquals(true, output.contains(cr));
		assertEquals(1, output.size());

		// Test 5
		output = DatabusHttpV3ClientImpl.getListOfConsumerRegsFromSubList(groupsListeners, Arrays.asList(ds0, ds1, ds2));
		assertEquals(false, output.contains(cr));
		assertEquals(0, output.size());

		List<DatabusSubscription> subsPresent2 = new ArrayList<DatabusSubscription>();
		subsPresent2.add(ds0);
		subsPresent2.add(ds2);
		DatabusV2ConsumerRegistration cr2 = new DatabusV2ConsumerRegistration(dsc, DatabusSubscription.getStrList(subsPresent2), null);

		// Update registration list
		cReg.add(cr2);

		// Update group listeners
		groupsListeners.put(subsPresent2, cReg);

		// Test 6
		output = DatabusHttpV3ClientImpl.getListOfConsumerRegsFromSubList(groupsListeners, Arrays.asList(ds0));
		assertEquals(true, output.contains(cr));
		assertEquals(true, output.contains(cr2));
		assertEquals(2, output.size());

	}

	@Test
	public void testDatabusV3RegistrationAPIStateMachine1() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);
      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      RegistrationId rid = new RegistrationId("Reg1");

      DatabusV3Registration dvr = _client.registerDatabusListener(listener1, rid, null, ds0);
      assertEquals(dvr.getState(),RegistrationState.CREATED);

      // Allowed in all state except when DEREGISTERED
      dvr.addDatabusConsumers(null);
      dvr.addSubscriptions((DatabusSubscription[])null);
      dvr.removeDatabusConsumers(null);
      dvr.removeSubscriptions((DatabusSubscription[])null);

      RelayFindMaxSCNResult rfms = dvr.fetchMaxSCN(null);
      assertEquals(rfms.getResultSummary(), RelayFindMaxSCNResult.SummaryCode.FAIL);
      RelayFlushMaxSCNResult rfr = dvr.flush(rfms, null);
      assertEquals(rfr.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.FAIL);

      // Allowed only after
      assertFalse(dvr.getCheckpoint() == null);

      // Should be allowed in any state
      assertTrue(dvr.getDBName().equals("BizProfile"));
      assertTrue(dvr.getId().equals(rid));
      assertTrue(dvr.getState().equals(RegistrationState.CREATED));
      assertFalse(dvr.getStatus() == null);
      assertTrue(dvr.getSubscriptions().get(0).equals(ds0));
	}

	@Test
	public void testDatabusV3RegistrationAPIStateMachine2() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);
      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      RegistrationId rid = new RegistrationId("Reg1");

      DatabusV3Registration dvr = _client.registerDatabusListener(listener1, rid, null, ds0);
      assertEquals(dvr.getState(),RegistrationState.CREATED);
      dvr.start();

      // Allowed in all state except when DEREGISTERED
      dvr.addDatabusConsumers(null);
      dvr.addSubscriptions((DatabusSubscription[])null);
      dvr.removeDatabusConsumers(null);
      dvr.removeSubscriptions((DatabusSubscription[])null);

      RelayFindMaxSCNResult rfms = dvr.fetchMaxSCN(null);
      assertEquals(rfms.getResultSummary(), RelayFindMaxSCNResult.SummaryCode.FAIL);
      RelayFlushMaxSCNResult rfr = dvr.flush(rfms, null);
      assertEquals(rfr.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.FAIL);

      // Allowed only after
      assertFalse(dvr.getCheckpoint() == null);

      // Should be allowed in any state
      assertTrue(dvr.getDBName().equals("BizProfile"));
      assertTrue(dvr.getId().equals(rid));
      assertTrue(dvr.getState().equals(RegistrationState.STARTED));
      assertFalse(dvr.getStatus() == null);
      assertTrue(dvr.getSubscriptions().get(0).equals(ds0));
	}

	@Test
	public void testDatabusV3RegistrationAPIStateMachine3() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);
      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      RegistrationId rid = new RegistrationId("Reg1");

      DatabusV3Registration dvr = _client.registerDatabusListener(listener1, rid, null, ds0);
      assertEquals(dvr.getState(),RegistrationState.CREATED);
      dvr.start();
      dvr.deregister();

      // When state is DEREGISTERED, it is a no-op
      dvr.addDatabusConsumers(null);
      dvr.addSubscriptions((DatabusSubscription[])null);
      dvr.removeDatabusConsumers(null);
      dvr.removeSubscriptions((DatabusSubscription[])null);

      RelayFindMaxSCNResult rfms = dvr.fetchMaxSCN(null);
      assertEquals(rfms.getResultSummary(), RelayFindMaxSCNResult.SummaryCode.FAIL);
      RelayFlushMaxSCNResult rfr = dvr.flush(rfms, null);
      assertEquals(rfr.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.FAIL);

      assertTrue(dvr.getCheckpoint() == null);

      // Should be allowed in any state
      assertTrue(dvr.getDBName().equals("BizProfile"));
      assertTrue(dvr.getId().equals(rid));
      assertTrue(dvr.getState().equals(RegistrationState.DEREGISTERED));
      assertFalse(dvr.getStatus() == null);
      assertTrue(dvr.getSubscriptions().get(0).equals(ds0));
	}

	@Test
	public void testDatabusV3RegistrationPullerFlushRegistrationError() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  clientConfig.getConnectionDefaults().getPullerRetries().setInitSleep(0);
	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);
      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      final RegistrationId rid = new RegistrationId("Reg1");

      DatabusV3Registration dvr = _client.registerDatabusListener(listener1, rid, null, ds0);
      dvr.start();

      RelayFindMaxSCNResult rfms = RelayFindMaxScnResultImpl.createTestRelayFindMaxScn();
      FlushRequest fr = new FlushRequest();
      _client.getDatabusSourcesConnection(rid.getId()).getRelayPullThread().shutdown();
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return !_client.getDatabusSourcesConnection(rid.getId()).getRelayPullThread().
              getComponentStatus().isRunningStatus();
        }
      }, "puller thread shutdown", 100L, LOG);
      RelayFlushMaxSCNResult rfr = dvr.flush(rfms, fr);
      assertEquals(rfr.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.FAIL);

	}

	@Test
	public void testDatabusV3RegistrationDispatcherFlushRegistrationError() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  clientConfig.getConnectionDefaults().getDispatcherRetries().setInitSleep(0);
	  clientConfig.getConnectionDefaults().getPullerRetries().setInitSleep(10);

	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(true);
      registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      final RegistrationId rid = new RegistrationId("Reg1");

      DatabusV3Registration dvr = _client.registerDatabusListener(listener1, rid, null, ds0);
      dvr.start();

      RelayFindMaxSCNResult rfms = RelayFindMaxScnResultImpl.createTestRelayFindMaxScn();
      FlushRequest fr = new FlushRequest();
      _client.getDatabusSourcesConnection(rid.getId()).getRelayDispatcher().shutdown();
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return !_client.getDatabusSourcesConnection(rid.getId()).getRelayDispatcher().
              getComponentStatus().isRunningStatus();
        }
      }, "dispatcher thread shutdown", 100L, LOG);
      RelayFlushMaxSCNResult rfr = dvr.flush(rfms, fr);
      assertEquals(rfr.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.FAIL);

	}

	@Test
	public void testMultiPartitionConsumerRegistration() throws Exception
	{
	  //Logger log = Logger.getLogger("TestEspressoDatabusHttpClient.testMultiPartitionConsumerRegistration");

      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      clientConfig.getConnectionDefaults().getDispatcherRetries().setInitSleep(0);
      clientConfig.getConnectionDefaults().getPullerRetries().setInitSleep(0);
      clientConfig.getConnectionDefaults().getPullerRetries().setMaxRetryNum(1);

      _client = new DatabusHttpV3ClientImpl(clientConfig);

      DummyEspressoStreamConsumer cons1 = new DummyEspressoStreamConsumer("cons1");
      DatabusV3Registration reg =
          _client.registerDatabusListener(cons1, new RegistrationId("cons1"), null,
                                          DatabusSubscription.createFromUri("espresso:/testDB/0/TableA"),
                                          DatabusSubscription.createFromUri("espresso:/testDB/1/TableA"),
                                          DatabusSubscription.createFromUri("espresso:/testDB/2/TableA"));
      Assert.assertNotNull(reg);
      Assert.assertTrue(reg instanceof PartitionMultiplexingConsumerRegistration);
      PartitionMultiplexingConsumerRegistration r =
          (PartitionMultiplexingConsumerRegistration)reg;
      Assert.assertTrue(r.getConsumer() instanceof PartitionMultiplexingConsumer);
      PartitionMultiplexingConsumer pmc = (PartitionMultiplexingConsumer)r.getConsumer();
      Assert.assertEquals(3, pmc.getConsumers().size());

      Assert.assertEquals(RegistrationState.CREATED, r.getState());
      Assert.assertTrue(r.getStatus() instanceof CompoundDatabusComponentStatus);
      CompoundDatabusComponentStatus rstatus = (CompoundDatabusComponentStatus)r.getStatus();
      Assert.assertEquals(3, rstatus.getChildren().size());
      Assert.assertEquals(3, rstatus.getInitializing().size());

      StartResult startRes = r.start();
      Assert.assertTrue(startRes.getSuccess());
      for (int ppi = 0; ppi < 3; ++ppi)
      {
        PhysicalPartition pp = new PhysicalPartition(ppi, "testDB");
        final DatabusV3Registration regP0 = r.getPartionRegs().get(pp);
        Assert.assertEquals(RegistrationState.STARTED, regP0.getState());
        //TODO - enable once the connection status propagates up properly
        /*
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return regP0.getStatus().getStatus().equals(DatabusComponentStatus.Status.SUSPENDED_ON_ERROR);
          }
        }, "waiting for consumers to go error state because of lack of relays", 10000, log);
        */
      }

      DeregisterResult deregRes = r.deregister();
      Assert.assertTrue(deregRes.getErrorMessage(), deregRes.isSuccess());
      for (int ppi = 0; ppi < 3; ++ppi)
      {
        final DatabusV3Registration regP0 = r.getPartionRegs().get(
           new PhysicalPartition(ppi, "testDB"));
        Assert.assertEquals(RegistrationState.DEREGISTERED, regP0.getState());
      }
	}

	public void testBasicMultiPartitionConsumption() throws Exception
	{
	  //WIP
      Logger log = Logger.getLogger("TestEspressoDatabusHttpClient.testBasicMultiPartitionConsumption");

      final int eventsNum = 50;
      DbusEventInfo[] events = createSampleSchema1Events(eventsNum);

      //simulate relay buffers
      DbusEventBuffer[] relayBuffer = new DbusEventBuffer[RELAY_PORT.length];

      for (int i = 0; i < RELAY_PORT.length; ++i)
      {
        relayBuffer[i] = new DbusEventBuffer(_bufCfg);
        relayBuffer[i].start(0);
        writeEventsToBuffer(relayBuffer[i], events, (RELAY_PORT.length - i) * 10);
      }

      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig = new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      clientConfig.getContainer().setHttpPort(CLIENT_PORT);
      clientConfig.getConnectionDefaults().getEventBuffer().setMaxSize(100000);
      clientConfig.getConnectionDefaults().getEventBuffer().setReadBufferSize(1000);
      clientConfig.getConnectionDefaults().getEventBuffer().setScnIndexSize(100);
      clientConfig.getConnectionDefaults().getDispatcherRetries().setInitSleep(0);
      clientConfig.getConnectionDefaults().getPullerRetries().setInitSleep(10);
      clientConfig.getConnectionDefaults().getPullerRetries().setMaxRetryNum(-1);

      final DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientConfig);
      final DatabusSubscription sub0 = DatabusSubscription.createFromUri("espresso:/testDB/0/TableA");
      final DatabusSubscription sub1 = DatabusSubscription.createFromUri("espresso:/testDB/1/TableA");
      final DatabusSubscription sub2 = DatabusSubscription.createFromUri("espresso:/testDB/2/TableA");

      registerRelayV3(client, RELAY_PORT[0], "relay0", RELAY_ADDR[0], sub0);
      registerRelayV3(client, RELAY_PORT[1], "relay1", RELAY_ADDR[1], sub1);
      registerRelayV3(client, RELAY_PORT[2], "relay2", RELAY_ADDR[2], sub2);

      DummyEspressoStreamConsumer cons1 = new DummyEspressoStreamConsumer("cons1");
      final DatabusV3Registration reg =
          _client.registerDatabusListener(cons1, new RegistrationId("cons1"), null, sub0, sub1,
                                          sub2);
      Assert.assertNotNull(reg);
      Assert.assertTrue(reg instanceof PartitionMultiplexingConsumerRegistration);
      PartitionMultiplexingConsumerRegistration r =
          (PartitionMultiplexingConsumerRegistration)reg;
      Assert.assertTrue(r.getConsumer() instanceof PartitionMultiplexingConsumer);
      PartitionMultiplexingConsumer pmc = (PartitionMultiplexingConsumer)r.getConsumer();
      Assert.assertEquals(3, pmc.getConsumers().size());

      final DatabusV3Consumer[] partCons = new DatabusV3Consumer[3];
      for (int ppi = 0; ppi < 3; ++ppi)
      {
        PhysicalPartition pp = new PhysicalPartition(ppi, "testDB");
        partCons[ppi] = pmc.getPartitionConsumer(pp);
      }

      StartResult startResult = r.start();
      try
      {
        Assert.assertTrue(startResult.getSuccess());
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return client.getRelayConnections().size() == 3;
          }
        }, "sources connection present", 100, log);

        /*
        final DatabusSourcesConnection[] clientConn = new DatabusSourcesConnection[3];
        for (int i = 0; i < 3; ++i)
        {
           clientConn[i] = client.getRelayConnections().get(i);

           final int j = i;
           TestUtil.assertWithBackoff(new ConditionCheck()
           {
             @Override
             public boolean check()
             {
               return null != clientConn[j].getRelayPullThread().getLastOpenConnection();
             }
           }, "relay connection present", 100, log);
        }

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
        SocketAddress clientAddr = clientChannel.getLocalAddress();*/
      }
      finally
      {
        client.shutdown();
      }

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

	  static void writeEventsToBuffer(DbusEventBuffer buf, DbusEventInfo[] events, int winSize)
	      throws UnsupportedEncodingException
	  {
	    Random rng = new Random(100);
	    final List<DbusEventKey> eventKeys = new ArrayList<DbusEventKey>(events.length);

	    for (int i = 0; i < events.length; ++i)
	    {
	      if (i % winSize == 0)
	      {
	        if (i > 0) buf.endEvents(i);
	        buf.startEvents();
	      }
	      DbusEventKey key = new DbusEventKey(RngUtils.randomString(rng, rng.nextInt(50)).getBytes("UTF-8"));
	      buf.appendEvent(key, events[i], null);
	      eventKeys.add(key);
	    }
	    buf.endEvents(events.length);
	  }

	static class DummyEspressoStreamConsumer implements DatabusV3Consumer
	{
		private final String _name;

		public DummyEspressoStreamConsumer(String name) {
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
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public boolean canBootstrap()
		{
			return false;
		}

		@Override
		public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartBootstrap()
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartBootstrapSource(String name, Schema sourceSchema)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStopBootstrap()
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onBootstrapError(Throwable err)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		public String getName() {
			return _name;
		}
	}

}
