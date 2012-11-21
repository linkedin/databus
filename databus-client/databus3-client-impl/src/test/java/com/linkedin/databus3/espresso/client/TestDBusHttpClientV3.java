package com.linkedin.databus3.espresso.client;

import com.linkedin.databus.client.DatabusHttpClientImpl.RuntimeConfigBuilder;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.FlushRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RegistrationState;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult.SummaryCode;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus3.espresso.client.TestEspressoDatabusHttpClient.DummyEspressoStreamConsumer;
import com.linkedin.databus3.espresso.client.TestRelayMaxSCNFinder.FakeRelay;
import com.linkedin.databus3.espresso.client.cmclient.RelayClusterInfoProvider;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumerRegistration;
import com.linkedin.databus3.espresso.client.data_model.EspressoSubscriptionUriCodec;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestDBusHttpClientV3
{
	public static final Logger LOG = Logger.getLogger(TestDBusHttpClientV3.class.getName());
	DatabusHttpV3ClientImpl _client;
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

	@BeforeClass
	public void setupClass()
	{
	  TestUtil.setupLogging(true, null, Level.INFO);
	}

	@Test
	public void TestDeregisterWildcard() throws Exception
	{
	  RelayClusterInfoProvider mockRcip = new RelayClusterInfoProvider(2);
		DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
		    new DatabusHttpV3ClientImpl.StaticConfigBuilder();
		clientConfig.getClusterManager().setEnabled(true);
		final DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientConfig.build(), mockRcip);
		boolean caughtException = false;
		try
		{
		  client.start();
		}
		catch(UnsupportedOperationException e)
		{
		  caughtException = true;
		}
		Assert.assertTrue(caughtException);
		client.setClientStarted(true);

		registerRelayV3(client, 1, "relay1", new InetSocketAddress("localhost", 8888),
		                DatabusSubscription.createFromUri("espresso://MASTER/testDB/0/S1"),
		                DatabusSubscription.createFromUri("espresso://MASTER/testDB/0/S2"));
		registerRelayV3(client, 2, "relay2", new InetSocketAddress("localhost", 7777),
		                DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/S1"),
		                DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/S3"));
		registerRelayV3(client, 3, "relay1.1", new InetSocketAddress("localhost", 8887),
		                DatabusSubscription.createFromUri("espresso://SLAVE/testDB/2/S1"),
		                DatabusSubscription.createFromUri("espresso://SLAVE/testDB/2/S2"));
		registerRelayV3(client, 4, "relay3", new InetSocketAddress("localhost", 6666),
		                DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S3"),
		                DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S4"),
		                DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S5"));

		DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
		DatabusSubscription ds1 = DatabusSubscription.createSimpleSourceSubscription("testDB", "S1");
		final RegistrationId rid = new RegistrationId("Reg1");
		final DatabusV3Registration registration1 = client.registerDatabusListener(listener1, rid, null, ds1);
        Assert.assertTrue(registration1 instanceof PartitionMultiplexingConsumerRegistration);
        PartitionMultiplexingConsumerRegistration r1 =
            (PartitionMultiplexingConsumerRegistration)registration1;

		registration1.start();

		Assert.assertEquals(r1.getPartionRegs().size(), mockRcip.getNumPartitions("testDB"));
		Assert.assertEquals(r1.getPartionRegs().size(), client.getRelayConnections().size());

        for (Map.Entry<PhysicalPartition, DatabusV3Registration> reg: r1.getPartionRegs().entrySet())
        {
          Assert.assertEquals(reg.getValue().getSubscriptions().size(), 1);
          Assert.assertEquals(reg.getValue().getSubscriptions().get(0).getPhysicalPartition(), reg.getKey());
          Assert.assertNotNull(client.getDatabusSourcesConnection(reg.getValue().getId().getId()),
                              "physical partition registered: " + reg.getKey());
        }

		registration1.deregister();
		for (Map.Entry<PhysicalPartition, DatabusV3Registration> reg: r1.getPartionRegs().entrySet())
		{
		  Assert.assertTrue(reg.getValue().getState().equals(RegistrationState.DEREGISTERED),
		                    "physical partition deregistered: " + reg.getKey());
		  Assert.assertNull(client.getDatabusSourcesConnection(reg.getValue().getId().getId()),
		                      "physical partition deregistered: " + reg.getKey());
		}
	}

	@Test
	public void TestDeregister2() throws Exception
	{
      RelayClusterInfoProvider mockRcip = new RelayClusterInfoProvider(1);
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      clientConfig.getClusterManager().setEnabled(true);
      DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientConfig.build(), mockRcip);
      client.setClientStarted(true);

      registerRelayV3(client, 1, "relay1", new InetSocketAddress("localhost", 8888),
                      DatabusSubscription.createFromUri("espresso://MASTER/testDB/0/S1"),
                      DatabusSubscription.createFromUri("espresso://MASTER/testDB/0/S2"));
      registerRelayV3(client, 2, "relay2", new InetSocketAddress("localhost", 7777),
                      DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/S1"),
                      DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/S3"));
      registerRelayV3(client, 3, "relay1.1", new InetSocketAddress("localhost", 8887),
                      DatabusSubscription.createFromUri("espresso://SLAVE/testDB/2/S1"),
                      DatabusSubscription.createFromUri("espresso://SLAVE/testDB/2/S2"));
      registerRelayV3(client, 4, "relay3", new InetSocketAddress("localhost", 6666),
                      DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S3"),
                      DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S4"),
                      DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S5"));

		DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
        DatabusSubscription ds1 = DatabusSubscription.createFromUri("espresso:/testDB/1/S1");
		RegistrationId rid = new RegistrationId("Reg1");
		DatabusV3Registration registration1 = client.registerDatabusListener(listener1, rid, null, ds1);
		registration1.start();

		DatabusSourcesConnection conn = client.getDatabusSourcesConnection(registration1.getId().toString());

		FlushConsumer flushConsumer = new FlushConsumer(3322, registration1.getDBName(), 0);

		DatabusV2ConsumerRegistration flushConsumerRegistration = new DatabusV3ConsumerRegistration(
				client,
				registration1.getDBName(),
				flushConsumer,
				new RegistrationId("Reg2"),
				registration1.getSubscriptions(),
				((DatabusV3ConsumerRegistration)registration1).getFilterConfig(),
				registration1.getCheckpoint(),
				registration1.getStatus()
				);

		conn.getRelayConsumers().add(flushConsumerRegistration);

		Assert.assertEquals(conn.getRelayConsumers().size() , 3);// for the logging consumer
		registration1.deregister();

		Assert.assertEquals(conn.getRelayConsumers().size() , 2);
	}

    @Test
    public void TestDeregisterSinglePartition() throws Exception
    {
      RelayClusterInfoProvider mockRcip = new RelayClusterInfoProvider(1);
        DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
            new DatabusHttpV3ClientImpl.StaticConfigBuilder();
        clientConfig.getClusterManager().setEnabled(true);
        DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientConfig.build(), mockRcip);
        client.setClientStarted(true);

        registerRelayV3(client, 1, "relay1", new InetSocketAddress("localhost", 8888),
                        DatabusSubscription.createFromUri("espresso://MASTER/testDB/0/S1"),
                        DatabusSubscription.createFromUri("espresso://MASTER/testDB/0/S2"));
        registerRelayV3(client, 2, "relay2", new InetSocketAddress("localhost", 7777),
                        DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/S1"),
                        DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/S3"));
        registerRelayV3(client, 3, "relay1.1", new InetSocketAddress("localhost", 8887),
                        DatabusSubscription.createFromUri("espresso://SLAVE/testDB/2/S1"),
                        DatabusSubscription.createFromUri("espresso://SLAVE/testDB/2/S2"));
        registerRelayV3(client, 4, "relay3", new InetSocketAddress("localhost", 6666),
                        DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S3"),
                        DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S4"),
                        DatabusSubscription.createFromUri("espresso://SLAVE/testDB/3/S5"));

        DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
        DatabusSubscription ds1 = DatabusSubscription.createFromUri("espresso:/testDB/1/S1");
        RegistrationId rid = new RegistrationId("Reg1");
        DatabusV3Registration registration1 = client.registerDatabusListener(listener1, rid, null, ds1);
        registration1.start();

        DatabusSourcesConnection conn = client.getDatabusSourcesConnection(registration1.getId().toString());
        Assert.assertEquals(conn.getRelayConsumers().size() , 2);// for the logging consumer
        registration1.deregister();

        Assert.assertEquals(conn.getRelayConsumers().size() , 0);
    }

	/***
	 *
	 * Tests Parallel Flush Call.
	 *
	 * Steps:
	 *
	 * 1. Create DatabusHttpV3Client instance and enable CM flag.
	 * 2. Create a fake external view of relays, fake instances of relays (FakeRelay which returns a fixed maxSCN) and runs on a port.
	 * 3. Register 2 listeners listening to the same partition hosted by the relays. THis will setup the SourcesConnection.
	 * 4. The maxSCN in the fake relays and dispatcher's maxSCN are setup such that the client still needs to get more events to reach relay's maxSCN.
	 * 5. Create 2 instances of Flusher threads and start them. The flusher threads concurrently calls the flush API.
	 *    But the flusher thread will not complete since feeder have not started yet.
	 * 6. Create 2 instances of Feeder threads and start them. These threads will first wait for the flusher threads to open the latch.
	 *    These threads will also wait for flushConsumer to be present in the relay listeners list before opening its own latch. This logic will guarantee parallel
	 *    flush is happening.
	 * 7. Validate response of flush for success.
	 * @throws Exception
	 */
	@Test
	public void TestParallelFlush()
			throws Exception
	{
		DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
		    new DatabusHttpV3ClientImpl.StaticConfigBuilder();
		_client = new DatabusHttpV3ClientImpl(clientConfig.build());
		_client.setClientStarted(true);
		_client.setCMEnabled(true);

		Map<ResourceKey, List<DatabusServerCoordinates>> fakeview = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
		List<DatabusServerCoordinates> relayList = new ArrayList<DatabusServerCoordinates>();
		fakeview.put(new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p77_1,MASTER"), relayList );

		int port1 = Utils.getAvailablePort(27960);
		int port2 = Utils.getAvailablePort(port1 +1);

		relayList.add(new DatabusServerCoordinates(new InetSocketAddress(port1), "ONLINE"));
		relayList.add(new DatabusServerCoordinates(new InetSocketAddress(port2), "ONLINE"));

		RelayClusterInfoProvider fakeProvider = EasyMock.createMock(RelayClusterInfoProvider.class);
		org.easymock.EasyMock.expect(fakeProvider.getExternalView("BizProfile")).andReturn(fakeview).anyTimes();
		EasyMock.makeThreadSafe(fakeProvider, true);
		EasyMock.replay(fakeProvider);

		Field field = _client.getClass().getDeclaredField("_relayClusterInfoProvider");
		field.setAccessible(true);
		field.set(_client, fakeProvider);

		DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("Consumer_Subscription1");
		DummyEspressoStreamConsumer listener2 = new DummyEspressoStreamConsumer("Consumer_Subscription2");
		DummyEspressoStreamConsumer listener3 = new DummyEspressoStreamConsumer("Consumer_Subscription2");

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

		RegistrationId rid1 = new RegistrationId("Reg1");
		RegistrationId rid2 = new RegistrationId("Reg2");
		RegistrationId rid3 = new RegistrationId("Reg3");

		final DatabusV3Registration reg1 = _client.registerDatabusListener(listener1, rid1, null, ds1);
		final DatabusV3Registration reg2 = _client.registerDatabusListener(listener2, rid2, null, ds1);
		final DatabusV3Registration reg3 = _client.registerDatabusListener(listener3, rid3, null, ds1);
		reg1.start();
		reg2.start();
		reg3.start();


		/**
		 *
		 * FlusherThread which calls flush. Ensures concurrent flusherThreads start flushing around the same
		 * time using latches.
		 *
		 */
		class FlusherThread implements Runnable
		{
			private final CountDownLatch _beginLatch;
			private final CountDownLatch _endLatch;
			private final DatabusV3Registration _reg;
			private RelayFlushMaxSCNResult _response;

			public FlusherThread(CountDownLatch beginLatch,
					             CountDownLatch endLatch,
					             DatabusV3Registration reg)
			{
				_beginLatch = beginLatch;
				_endLatch = endLatch;
				_reg = reg;
			}

			@Override
      public void run()
			{
				FetchMaxSCNRequest request = new FetchMaxSCNRequest(500,3);
				FlushRequest flushRequest = new FlushRequest();
				flushRequest.setFlushTimeoutMillis(15000);


				_beginLatch.countDown();

				try {
					_beginLatch.await();
					_response = _reg.flush(request, flushRequest);

				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					_endLatch.countDown();
				}
			}

			public RelayFlushMaxSCNResult getResponse()
			{
				return _response;
			}
		}

		/**
		 *
		 * Feed the Flush Consumer with dummy events (SCN alone matters).
		 * Waits for the FlushBeginLatch to open and flush to register its consumer before generating events.
		 */
		class FlushConsumerFeeder extends TimerTask
		{
			private final DatabusSourcesConnection _conn;
			private final long _startSCN;
			private final CountDownLatch _beginLatch;
			private final CountDownLatch _flushBeginLatch;
			private final CountDownLatch _endLatch;

			private boolean success = true;

			public boolean isSuccess()
			{
				return success;
			}

			public FlushConsumerFeeder(DatabusSourcesConnection conn,
									   CountDownLatch beginLatch,
									   CountDownLatch endLatch,
									   CountDownLatch flushBeginLatch,
					                   long startSCN)
			{
				_conn = conn;
				_startSCN = startSCN;
				_beginLatch = beginLatch;
				_flushBeginLatch = flushBeginLatch;
				_endLatch = endLatch;
			}

			@Override
			public void run()
			{

				try {
					// Wait for flush Call to happen first
					_flushBeginLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// Wait till you have flush Consumer Registered. this would mean that flush is in the middle of its operation
				int retry = 5;
				boolean flushConsumerFound = false;
				try
				{

					while ( retry-- > 0)
					{
						for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
						{
							if(reg.getConsumer() instanceof FlushConsumer)
							{
								flushConsumerFound = true;
								break;
							}
						}

						if ( !flushConsumerFound )
						{
							Thread.sleep(1000);
						}
					}

					// Open Latch so that both feeders start around same time.
					_beginLatch.countDown();
					_beginLatch.await();

				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				if ( !flushConsumerFound)
				{
					success = false;
				} else {

					for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
					{
						if(reg.getConsumer() instanceof FlushConsumer)
						{
							new TestRelayFlusher.FakeEventProducer((FlushConsumer)reg.getConsumer(), 50, 10, 200, _startSCN).start();
						}
					}

					SCN endSCN = new SingleSourceSCN(-1, _startSCN + ( 10 * 200));
					_conn.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
				}
				_endLatch.countDown();

				try {
					_endLatch.await();
				} catch (InterruptedException e) {}
			}
		}

		// Parallel Flush for 2 different registrations
		{
			TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(port1, 9900, true, 50);
			fakeRelay1.start();

			TestRelayMaxSCNFinder.FakeRelay fakeRelay2 = new FakeRelay(port2, 9904, true, 50);
			fakeRelay2.start();

			DatabusSourcesConnection conn1 = _client.getDatabusSourcesConnection(reg1.getId().toString());
			SCN endSCN = new SingleSourceSCN(-1, 9850);
			conn1.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
			DatabusSourcesConnection conn2 = _client.getDatabusSourcesConnection(reg2.getId().toString());
			conn2.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);

			CountDownLatch flushBeginLatch = new CountDownLatch(2);
			CountDownLatch flushEndLatch = new CountDownLatch(2);
			FlusherThread f1 = new FlusherThread(flushBeginLatch, flushEndLatch, reg1);
			FlusherThread f2 = new FlusherThread(flushBeginLatch, flushEndLatch, reg2);

			new Thread(f1).start();
			new Thread(f2).start();

			CountDownLatch feederBeginLatch = new CountDownLatch(2);
			CountDownLatch feederEndLatch = new CountDownLatch(2);

			FlushConsumerFeeder feeder1 = new FlushConsumerFeeder(conn1, feederBeginLatch, feederEndLatch, flushBeginLatch, 8904);
			FlushConsumerFeeder feeder2 = new FlushConsumerFeeder(conn2, feederBeginLatch, feederEndLatch, flushBeginLatch, 8904);

			new Timer().schedule(feeder1, 10);
			new Timer().schedule(feeder2, 10);

			flushEndLatch.await();
			feederEndLatch.await();

			RelayFlushMaxSCNResult result1 = f1.getResponse();
			RelayFlushMaxSCNResult result2 = f2.getResponse();
			Assert.assertEquals(result1.getFetchSCNResult().getResultSummary(), SummaryCode.SUCCESS);
			Assert.assertEquals(((SingleSourceSCN)result1.getFetchSCNResult().getMaxSCN()).getSequence(), 9904);
			Assert.assertEquals(result1.getFetchSCNResult().getMaxScnRelays().size(), 1);
			Assert.assertEquals(result2.getFetchSCNResult().getResultSummary(), SummaryCode.SUCCESS);
			Assert.assertEquals(((SingleSourceSCN)result2.getFetchSCNResult().getMaxSCN()).getSequence(), 9904);
			Assert.assertEquals(result2.getFetchSCNResult().getMaxScnRelays().size(), 1);
			DatabusServerCoordinates coord1 = (DatabusServerCoordinates)(result1.getFetchSCNResult().getMaxScnRelays().toArray()[0]);
			DatabusServerCoordinates coord2 = (DatabusServerCoordinates)(result2.getFetchSCNResult().getMaxScnRelays().toArray()[0]);
			Assert.assertTrue(coord1.isOnlineState());
			Assert.assertTrue(coord1.getAddress().getPort() == port2);
			Assert.assertTrue(coord2.isOnlineState());
			Assert.assertTrue(coord2.getAddress().getPort() == port2);
			fakeRelay1.shutdown();
			fakeRelay2.shutdown();
			Assert.assertEquals(result1.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
			Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
			Assert.assertTrue(feeder1.isSuccess());
			Assert.assertTrue(feeder2.isSuccess());
		}
	}



	@Test
	public void TestEmptyView()
	    throws Exception
	{
		DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
		    new DatabusHttpV3ClientImpl.StaticConfigBuilder();
		_client = new DatabusHttpV3ClientImpl(clientConfig.build());
		_client.setClientStarted(true);
		_client.setCMEnabled(true);

		RelayClusterInfoProvider fakeProvider = EasyMock.createMock(RelayClusterInfoProvider.class);

		//Map<ResourceKey, List<DatabusServerCoordinates>> fakeview = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
		//List<DatabusServerCoordinates> relayList = new ArrayList<DatabusServerCoordinates>();
		//fakeview.put(new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p77_1,MASTER"), relayList );
		//relayList.add(new DatabusServerCoordinates(new InetSocketAddress(27960), "OFFLINE"));
		//relayList.add(new DatabusServerCoordinates(new InetSocketAddress(27961), "OFFLINE"));

		org.easymock.EasyMock.expect(fakeProvider.getExternalView("BizProfile")).andReturn(null).anyTimes();
		EasyMock.replay(fakeProvider);

		Field field = _client.getClass().getDeclaredField("_relayClusterInfoProvider");
		field.setAccessible(true);
		field.set(_client, fakeProvider);

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
		DatabusV3Registration regV3 = _client.registerDatabusListener(listener1, rid, null, ds1);
		regV3.start();

		TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(27960, 9900, true, 50);
		fakeRelay1.start();

		TestRelayMaxSCNFinder.FakeRelay fakeRelay2 = new FakeRelay(27961, 9904, true, 50);
		fakeRelay2.start();

		FetchMaxSCNRequest request = new FetchMaxSCNRequest(500,3);
		RelayFindMaxSCNResult result = regV3.fetchMaxSCN(request);
		Assert.assertEquals(RelayFindMaxSCNResult.SummaryCode.EMPTY_EXTERNAL_VIEW, result.getResultSummary(),"Empty External View");

		// Test for flush
		DatabusSourcesConnection conn = _client.getDatabusSourcesConnection(regV3.getId().toString());
		SCN endSCN = new SingleSourceSCN(-1, 9850);
		conn.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
		FlushRequest flushRequest = new FlushRequest();
		flushRequest.setFlushTimeoutMillis(1500);

		class FlushConsumerFeeder extends TimerTask
		{
			DatabusSourcesConnection _conn;
			long _startSCN;
			int _totalEvents;
			int _interEventLatency;
			int _diff;
			public FlushConsumerFeeder(DatabusSourcesConnection conn, long startSCN, int totalEvents, int interEventLatency, int diff)
			{
				_conn = conn;
				_startSCN = startSCN;
				_totalEvents = totalEvents;
				_interEventLatency = interEventLatency;
				_diff = diff;
			}
			@Override
			public void run()
			{
				for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
				{
					if(reg.getConsumer() instanceof FlushConsumer)
					{
						new TestRelayFlusher.FakeEventProducer((FlushConsumer)reg.getConsumer(), _interEventLatency, _totalEvents, _diff, _startSCN).start();
					}
				}
			}
		}

		new Timer().schedule(new FlushConsumerFeeder(conn, 9704, 50, 20, 40), 100);
		RelayFlushMaxSCNResult result2 = regV3.flush(result, flushRequest);
		Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.EMPTY_EXTERNAL_VIEW,"Empty External View");
	}

	@Test
	public void TestOfflineRelays()
	    throws Exception
	{
		DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
		    new DatabusHttpV3ClientImpl.StaticConfigBuilder();
		_client = new DatabusHttpV3ClientImpl(clientConfig.build());
		_client.setClientStarted(true);
		_client.setCMEnabled(true);
		int port1 = Utils.getAvailablePort(27960);
		int port2 = Utils.getAvailablePort(port1 +1);

		RelayClusterInfoProvider fakeProvider = EasyMock.createMock(RelayClusterInfoProvider.class);

		Map<ResourceKey, List<DatabusServerCoordinates>> fakeview = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
		List<DatabusServerCoordinates> relayList = new ArrayList<DatabusServerCoordinates>();
		fakeview.put(new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p77_1,MASTER"), relayList );
		relayList.add(new DatabusServerCoordinates(new InetSocketAddress(port1), "OFFLINE"));
		relayList.add(new DatabusServerCoordinates(new InetSocketAddress(port2), "OFFLINE"));

		org.easymock.EasyMock.expect(fakeProvider.getExternalView("BizProfile")).andReturn(fakeview).anyTimes();
		EasyMock.replay(fakeProvider);

		Field field = _client.getClass().getDeclaredField("_relayClusterInfoProvider");
		field.setAccessible(true);
		field.set(_client, fakeProvider);

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
		DatabusV3Registration regV3 = _client.registerDatabusListener(listener1, rid, null, ds1);
		regV3.start();

		TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(port1, 9900, false, 50);
		fakeRelay1.start();

		TestRelayMaxSCNFinder.FakeRelay fakeRelay2 = new FakeRelay(port2, 9904, false, 50);
		fakeRelay2.start();

		FetchMaxSCNRequest request = new FetchMaxSCNRequest(500, 3);
		RelayFindMaxSCNResult result = regV3.fetchMaxSCN(request);
		Assert.assertEquals(RelayFindMaxSCNResult.SummaryCode.NO_ONLINE_RELAYS_IN_VIEW, result.getResultSummary(),"NO ONLINE RELAYS IN VIEW");

		// Test for flush
		DatabusSourcesConnection conn = _client.getDatabusSourcesConnection(regV3.getId().toString());
		SCN endSCN = new SingleSourceSCN(-1, 9850);
		conn.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
		FlushRequest flushRequest = new FlushRequest();
		flushRequest.setFlushTimeoutMillis(1500);

		class FlushConsumerFeeder extends TimerTask
		{
			DatabusSourcesConnection _conn;
			long _startSCN;
			int _totalEvents;
			int _interEventLatency;
			int _diff;
			public FlushConsumerFeeder(DatabusSourcesConnection conn, long startSCN, int totalEvents, int interEventLatency, int diff)
			{
				_conn = conn;
				_startSCN = startSCN;
				_totalEvents = totalEvents;
				_interEventLatency = interEventLatency;
				_diff = diff;
			}
			@Override
			public void run()
			{
				for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
				{
					if(reg.getConsumer() instanceof FlushConsumer)
					{
						new TestRelayFlusher.FakeEventProducer((FlushConsumer)reg.getConsumer(), _interEventLatency, _totalEvents, _diff, _startSCN).start();
					}
				}
			}
		}

		new Timer().schedule(new FlushConsumerFeeder(conn, 9704, 50, 20, 40), 100);
		RelayFlushMaxSCNResult result2 = regV3.flush(result, flushRequest);
		Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.NO_ONLINE_RELAYS_IN_VIEW,"NO ONLINE RELAYS IN VIEW");
	}

	@Test
	public void TestObtainMaxSCNAndFlush()
			throws Exception
	{
		DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
		    new DatabusHttpV3ClientImpl.StaticConfigBuilder();
		_client = new DatabusHttpV3ClientImpl(clientConfig.build());
		_client.setClientStarted(true);
		_client.setCMEnabled(true);

		RelayClusterInfoProvider fakeProvider = EasyMock.createMock(RelayClusterInfoProvider.class);

		Map<ResourceKey, List<DatabusServerCoordinates>> fakeview = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
		List<DatabusServerCoordinates> relayList = new ArrayList<DatabusServerCoordinates>();
		fakeview.put(new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p77_1,MASTER"), relayList );
		relayList.add(new DatabusServerCoordinates(new InetSocketAddress(27960), "ONLINE"));
		relayList.add(new DatabusServerCoordinates(new InetSocketAddress(27961), "ONLINE"));

		org.easymock.EasyMock.expect(fakeProvider.getExternalView("BizProfile")).andReturn(fakeview).anyTimes();
		EasyMock.replay(fakeProvider);

		Field field = _client.getClass().getDeclaredField("_relayClusterInfoProvider");
		field.setAccessible(true);
		field.set(_client, fakeProvider);

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
		DatabusV3Registration regV3 = _client.registerDatabusListener(listener1, rid, null, ds1);

		CheckpointPersistenceProvider ckp = regV3.getCheckpoint();
		Checkpoint cp = new Checkpoint();
		cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
		cp.setWindowScn(9850L);
		cp.setWindowOffset(-1);
		RegistrationId regId = regV3.getId();
		List<DatabusSubscription> dsList = new Vector<DatabusSubscription>();
		dsList.add(ds1);
		ckp.storeCheckpointV3(dsList, cp, regId);

		regV3.start();

		TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(27960, 9900, true, 50);
		fakeRelay1.start();

		TestRelayMaxSCNFinder.FakeRelay fakeRelay2 = new FakeRelay(27961, 9904, true, 50);
		fakeRelay2.start();

		FetchMaxSCNRequest request = new FetchMaxSCNRequest(500,3);
		RelayFindMaxSCNResult result = regV3.fetchMaxSCN(request);
		// Test for obtain max scn
		Assert.assertEquals(result.getResultSummary(), SummaryCode.SUCCESS);
		Assert.assertEquals(((SingleSourceSCN)result.getMaxSCN()).getSequence(), 9904);
		Assert.assertEquals(result.getMaxScnRelays().size(), 1);
		DatabusServerCoordinates coord = (DatabusServerCoordinates)(result.getMaxScnRelays().toArray()[0]);
		Assert.assertTrue(coord.isOnlineState());
		Assert.assertTrue(coord.getAddress().getPort() == 27961);
		fakeRelay1.shutdown();
		fakeRelay2.shutdown();

		DatabusSourcesConnection conn = _client.getDatabusSourcesConnection(regV3.getId().toString());
		// Test for flush
		FlushRequest flushRequest = new FlushRequest();
		flushRequest.setFlushTimeoutMillis(1500);

		class FlushConsumerFeeder extends TimerTask
		{
			DatabusSourcesConnection _conn;
			long _startSCN;
			int _totalEvents;
			int _interEventLatency;
			int _diff;
			public FlushConsumerFeeder(DatabusSourcesConnection conn, long startSCN, int totalEvents, int interEventLatency, int diff)
			{
				_conn = conn;
				_startSCN = startSCN;
				_totalEvents = totalEvents;
				_interEventLatency = interEventLatency;
				_diff = diff;
			}
			@Override
			public void run()
			{
				for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
				{
					if(reg.getConsumer() instanceof FlushConsumer)
					{
						new TestRelayFlusher.FakeEventProducer((FlushConsumer)reg.getConsumer(), _interEventLatency, _totalEvents, _diff, _startSCN).start();
					}
				}
			}
		}

		List<DatabusV2ConsumerRegistration> expectedConsumers = new ArrayList<DatabusV2ConsumerRegistration>(conn.getRelayConsumers());

		new Timer().schedule(new FlushConsumerFeeder(conn, 9704, 50, 20, 40), 100);
		RelayFlushMaxSCNResult result2 = regV3.flush(result, flushRequest);
		Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
		List<DatabusV2ConsumerRegistration> gotConsumers = new ArrayList<DatabusV2ConsumerRegistration>(conn.getRelayConsumers());

		// Test for DDSDBUS-723
		assertEqualityByReference(gotConsumers, expectedConsumers, "Flush should not alter the list of relay consumers");

		Assert.assertTrue(conn.isRunning());
		regV3.deregister();
		Assert.assertFalse(conn.isRunning());
	}

    @Test
    public void TestScnZero()
                    throws Exception
    {
            DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
                new DatabusHttpV3ClientImpl.StaticConfigBuilder();
            _client = new DatabusHttpV3ClientImpl(clientConfig.build());
            _client.setClientStarted(true);
            _client.setCMEnabled(true);

            RelayClusterInfoProvider fakeProvider = EasyMock.createMock(RelayClusterInfoProvider.class);

            Map<ResourceKey, List<DatabusServerCoordinates>> fakeview = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
            List<DatabusServerCoordinates> relayList = new ArrayList<DatabusServerCoordinates>();
            fakeview.put(new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p77_1,MASTER"), relayList );
            int port1 = Utils.getAvailablePort(27960);
            relayList.add(new DatabusServerCoordinates(new InetSocketAddress(port1), "ONLINE"));

            org.easymock.EasyMock.expect(fakeProvider.getExternalView("BizProfile")).andReturn(fakeview).anyTimes();
            EasyMock.replay(fakeProvider);

            Field field = _client.getClass().getDeclaredField("_relayClusterInfoProvider");
            field.setAccessible(true);
            field.set(_client, fakeProvider);

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
            DatabusV3Registration regV3 = _client.registerDatabusListener(listener1, rid, null, ds1);

    		CheckpointPersistenceProvider ckp = regV3.getCheckpoint();
    		Checkpoint cp = new Checkpoint();
    		cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    		cp.setWindowScn(-1L);
    		cp.setWindowOffset(-1);
    		RegistrationId regId = regV3.getId();
    		List<DatabusSubscription> dsList = new Vector<DatabusSubscription>();
    		dsList.add(ds1);
    		ckp.storeCheckpointV3(dsList, cp, regId);

            regV3.start();

            TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(port1, 0, true, 50);
            fakeRelay1.start();

            FetchMaxSCNRequest request = new FetchMaxSCNRequest(500,3);
            RelayFindMaxSCNResult result = regV3.fetchMaxSCN(request);
            // Test for obtain max scn
            Assert.assertEquals(result.getResultSummary(), SummaryCode.SUCCESS);
            Assert.assertEquals(((SingleSourceSCN)result.getMaxSCN()).getSequence(), 0);
            Assert.assertEquals(result.getMaxScnRelays().size(), 1);
            DatabusServerCoordinates coord = (DatabusServerCoordinates)(result.getMaxScnRelays().toArray()[0]);
            Assert.assertTrue(coord.isOnlineState());
            Assert.assertTrue(coord.getAddress().getPort() == port1);
            fakeRelay1.shutdown();

            DatabusSourcesConnection conn = _client.getDatabusSourcesConnection(regV3.getId().toString());
            // Test for flush
            FlushRequest flushRequest = new FlushRequest();
            flushRequest.setFlushTimeoutMillis(1500);

            class FlushConsumerFeeder extends TimerTask
            {
                    DatabusSourcesConnection _conn;
                    long _startSCN;
                    int _totalEvents;
                    int _interEventLatency;
                    int _diff;
                    public FlushConsumerFeeder(DatabusSourcesConnection conn, long startSCN, int totalEvents, int interEventLatency, int diff)
                    {
                            _conn = conn;
                            _startSCN = startSCN;
                            _totalEvents = totalEvents;
                            _interEventLatency = interEventLatency;
                            _diff = diff;
                    }
                    @Override
                    public void run()
                    {
                            for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
                            {
                                    if(reg.getConsumer() instanceof FlushConsumer)
                                    {
                                            new TestRelayFlusher.FakeEventProducer((FlushConsumer)reg.getConsumer(), _interEventLatency, _totalEvents, _diff, _startSCN).start();
                                    }
                            }
                    }
            }

            List<DatabusV2ConsumerRegistration> expectedConsumers = new ArrayList<DatabusV2ConsumerRegistration>(conn.getRelayConsumers());

            new Timer().schedule(new FlushConsumerFeeder(conn, 0, 1, 20, 40), 100);
            RelayFlushMaxSCNResult result2 = regV3.flush(result, flushRequest);
            Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
            List<DatabusV2ConsumerRegistration> gotConsumers = new ArrayList<DatabusV2ConsumerRegistration>(conn.getRelayConsumers());

            // Test for DDSDBUS-723
            assertEqualityByReference(gotConsumers, expectedConsumers, "Flush should not alter the list of relay consumers");

            Assert.assertTrue(conn.isRunning());
            regV3.deregister();
            Assert.assertFalse(conn.isRunning());
    }

	private void assertEqualityByReference(List<?> actual, List<?> expected, String message)
	{
		if (actual == expected)
			return;

		if ( ((null == actual) && ( null != expected)) ||
		     ((null != actual) && (null == expected)) )
			throw new AssertionError(message + ", List differs, Expected :" + expected + ", Got :" + actual);

		if ( actual.size() != expected.size())
			throw new AssertionError(message + ", List Size differs, Expected :" + expected + ", Got :" + actual);

		for ( int i = 0; i < actual.size(); i++)
		{
			Object exp = expected.get(i);
			Object got = actual.get(i);

			if ( exp != got )
				throw new AssertionError(message + ", List differs at position " + i + ", Expected :" + exp + ", Got :" + got);
		}
	}

	@Test
	public void testClientStartOnRegister() throws Exception
	{
	  DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
	      new DatabusHttpV3ClientImpl.StaticConfigBuilder();
	  int port = Utils.getAvailablePort(10000);
	  clientConfig.getContainer().setHttpPort(port);

	  _client = new DatabusHttpV3ClientImpl(clientConfig);
	  _client.setClientStarted(false);

      ServerInfo s11 = registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "BizProfile.*:0");
      ServerInfo s21 = registerRelay(2, "relay2", new InetSocketAddress("localhost", 8889), "BizProfile.*:0");

      DatabusSubscription ds0 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:0");
      DatabusSubscription ds1 = DatabusSubscription.createSimpleSourceSubscription("BizProfile.*:1");

      DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("consumer1");
      RegistrationId rid1 = new RegistrationId("Reg1");
      RegistrationId rid2 = new RegistrationId("Reg2");

      try
      {
    	  Assert.assertTrue(!_client.hasClientStarted(), "Client Should not have started yet !!");

          DatabusV3Registration dvr = _client.registerDatabusListener(listener1, rid1, null, ds0);
          dvr.start();

    	  Assert.assertTrue(_client.hasClientStarted(), "Client Should have started yet !!");

          DatabusV3Registration dv3r = _client.registerDatabusListener(listener1, rid2, null, ds1);
          dv3r.start();
      } catch (Exception e){throw new RuntimeException(e);}
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


		/***
		 *
		 * Tests Parallel Flush Call.
		 *
		 * Steps:
		 * 1. Start parallel flushes
		 * 2. In separate threads initiate external view change notifications.
		 * 3. Validate response of flush for success.
		 * @throws Exception
		 */
		//@Test
		public void TestExternalViewAndFlushSynchronization()
		throws Exception
		{
			DatabusHttpV3ClientImpl.StaticConfigBuilder clientConfig =
			    new DatabusHttpV3ClientImpl.StaticConfigBuilder();
			_client = new DatabusHttpV3ClientImpl(clientConfig.build());
			_client.setClientStarted(true);
			_client.setCMEnabled(true);

			final Map<ResourceKey, List<DatabusServerCoordinates>> fakeview = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
			List<DatabusServerCoordinates> relayList = new ArrayList<DatabusServerCoordinates>();
			ResourceKey rk = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,BizFollow,p77_1,MASTER");
			fakeview.put(rk, relayList );

			int port1 = Utils.getAvailablePort(27960);
			int port2 = Utils.getAvailablePort(port1 + 1);

			DatabusServerCoordinates dsc1 = new DatabusServerCoordinates(new InetSocketAddress(port1), "ONLINE");
			DatabusServerCoordinates dsc2 = new DatabusServerCoordinates(new InetSocketAddress(port2), "ONLINE");
			relayList.add(dsc1);
			relayList.add(dsc2);

			final Map<DatabusServerCoordinates, List<ResourceKey>> fakeInverseView = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
			List<ResourceKey> rkl = new ArrayList<ResourceKey>();
			rkl.add(rk);
			fakeInverseView.put(dsc1, rkl);
			fakeInverseView.put(dsc2, rkl);

			RelayClusterInfoProvider fakeProvider = EasyMock.createMock(RelayClusterInfoProvider.class);
			org.easymock.EasyMock.expect(fakeProvider.getExternalView("BizProfile")).andReturn(fakeview).anyTimes();
			EasyMock.makeThreadSafe(fakeProvider, true);
			EasyMock.replay(fakeProvider);

			Field field = _client.getClass().getDeclaredField("_relayClusterInfoProvider");
			field.setAccessible(true);
			field.set(_client, fakeProvider);

			DummyEspressoStreamConsumer listener1 = new DummyEspressoStreamConsumer("Consumer_Subscription1");
			DummyEspressoStreamConsumer listener2 = new DummyEspressoStreamConsumer("Consumer_Subscription2");
			DummyEspressoStreamConsumer listener3 = new DummyEspressoStreamConsumer("Consumer_Subscription2");

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

			RegistrationId rid1 = new RegistrationId("Reg1");
			RegistrationId rid2 = new RegistrationId("Reg2");
			RegistrationId rid3 = new RegistrationId("Reg3");

			final DatabusV3Registration reg1 = _client.registerDatabusListener(listener1, rid1, null, ds1);
			final DatabusV3Registration reg2 = _client.registerDatabusListener(listener2, rid2, null, ds1);
			final DatabusV3Registration reg3 = _client.registerDatabusListener(listener3, rid3, null, ds1);
			reg1.start();
			reg2.start();
			reg3.start();


			/**
			 *
			 * FlusherThread which calls flush. Ensures concurrent flusherThreads start flushing around the same
			 * time using latches.
			 *
			 */
			class FlusherThread implements Runnable
			{
				private final CountDownLatch _beginLatch;
				private final CountDownLatch _endLatch;
				private final DatabusV3Registration _reg;
				private RelayFlushMaxSCNResult _response;

				public FlusherThread(CountDownLatch beginLatch,
						             CountDownLatch endLatch,
						             DatabusV3Registration reg)
				{
					_beginLatch = beginLatch;
					_endLatch = endLatch;
					_reg = reg;
				}

				@Override
        public void run()
				{
					FetchMaxSCNRequest request = new FetchMaxSCNRequest(500,3);
					FlushRequest flushRequest = new FlushRequest();
					flushRequest.setFlushTimeoutMillis(15000);


					_beginLatch.countDown();

					try {
						_beginLatch.await();
						_response = _reg.flush(request, flushRequest);

					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
						_endLatch.countDown();
					}
				}

				public RelayFlushMaxSCNResult getResponse()
				{
					return _response;
				}
			}

			/**
			 *
			 * Feed the Flush Consumer with dummy events (SCN alone matters).
			 * Waits for the FlushBeginLatch to open and flush to register its consumer before generating events.
			 */
			class FlushConsumerFeeder extends TimerTask
			{
				private final DatabusSourcesConnection _conn;
				private final long _startSCN;
				private final CountDownLatch _beginLatch;
				private final CountDownLatch _flushBeginLatch;
				private final CountDownLatch _endLatch;

				private boolean success = true;

				public boolean isSuccess()
				{
					return success;
				}

				public FlushConsumerFeeder(DatabusSourcesConnection conn,
										   CountDownLatch beginLatch,
										   CountDownLatch endLatch,
										   CountDownLatch flushBeginLatch,
						                   long startSCN)
				{
					_conn = conn;
					_startSCN = startSCN;
					_beginLatch = beginLatch;
					_flushBeginLatch = flushBeginLatch;
					_endLatch = endLatch;
				}

				@Override
				public void run()
				{

					try {
						// Wait for flush Call to happen first
						_flushBeginLatch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					// Wait till you have flush Consumer Registered. this would mean that flush is in the middle of its operation
					int retry = 5;
					boolean flushConsumerFound = false;
					try
					{

						while ( retry-- > 0)
						{
							for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
							{
								if(reg.getConsumer() instanceof FlushConsumer)
								{
									flushConsumerFound = true;
									break;
								}
							}

							if ( !flushConsumerFound )
							{
								Thread.sleep(1000);
							}
						}

						// Open Latch so that both feeders start around same time.
						_beginLatch.countDown();
						_beginLatch.await();

					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					if ( !flushConsumerFound)
					{
						success = false;
					} else {

						for(DatabusV2ConsumerRegistration reg : _conn.getRelayConsumers())
						{
							if(reg.getConsumer() instanceof FlushConsumer)
							{
								new TestRelayFlusher.FakeEventProducer((FlushConsumer)reg.getConsumer(), 50, 10, 200, _startSCN).start();
							}
						}

						SCN endSCN = new SingleSourceSCN(-1, _startSCN + ( 10 * 200));
						_conn.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
					}
					_endLatch.countDown();

					try {
						_endLatch.await();
					} catch (InterruptedException e) {}
				}
			}

			// Parallel Flush for 2 different registrations with an external view change in between
			{
				TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(port1, 9900, true, 50);
				fakeRelay1.start();

				TestRelayMaxSCNFinder.FakeRelay fakeRelay2 = new FakeRelay(port2, 9904, true, 50);
				fakeRelay2.start();

				DatabusSourcesConnection conn1 = _client.getDatabusSourcesConnection(reg1.getId().toString());
				SCN endSCN = new SingleSourceSCN(-1, 9850);
				conn1.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
				DatabusSourcesConnection conn2 = _client.getDatabusSourcesConnection(reg2.getId().toString());
				conn2.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);

				CountDownLatch flushBeginLatch = new CountDownLatch(2);
				CountDownLatch flushEndLatch = new CountDownLatch(2);
				FlusherThread f1 = new FlusherThread(flushBeginLatch, flushEndLatch, reg1);
				FlusherThread f2 = new FlusherThread(flushBeginLatch, flushEndLatch, reg2);

				new Thread(f1).start();
				_client.onExternalViewChange("BizProfile", fakeview, fakeInverseView, fakeview, fakeInverseView);
				new Thread(f2).start();

				CountDownLatch feederBeginLatch = new CountDownLatch(2);
				CountDownLatch feederEndLatch = new CountDownLatch(2);

				FlushConsumerFeeder feeder1 = new FlushConsumerFeeder(conn1, feederBeginLatch, feederEndLatch, flushBeginLatch, 8904);
				FlushConsumerFeeder feeder2 = new FlushConsumerFeeder(conn2, feederBeginLatch, feederEndLatch, flushBeginLatch, 8904);

				new Timer().schedule(feeder1, 10);
				new Timer().schedule(feeder2, 10);

				flushEndLatch.await();
				feederEndLatch.await();

				RelayFlushMaxSCNResult result1 = f1.getResponse();
				RelayFlushMaxSCNResult result2 = f2.getResponse();
				Assert.assertEquals(result1.getFetchSCNResult().getResultSummary(), SummaryCode.SUCCESS);
				Assert.assertEquals(((SingleSourceSCN)result1.getFetchSCNResult().getMaxSCN()).getSequence(), 9904);
				Assert.assertEquals(result1.getFetchSCNResult().getMaxScnRelays().size(), 1);
				Assert.assertEquals(result2.getFetchSCNResult().getResultSummary(), SummaryCode.SUCCESS);
				Assert.assertEquals(((SingleSourceSCN)result2.getFetchSCNResult().getMaxSCN()).getSequence(), 9904);
				Assert.assertEquals(result2.getFetchSCNResult().getMaxScnRelays().size(), 1);
				DatabusServerCoordinates coord1 = (DatabusServerCoordinates)(result1.getFetchSCNResult().getMaxScnRelays().toArray()[0]);
				DatabusServerCoordinates coord2 = (DatabusServerCoordinates)(result2.getFetchSCNResult().getMaxScnRelays().toArray()[0]);
				Assert.assertTrue(coord1.isOnlineState());
				Assert.assertTrue(coord1.getAddress().getPort() == port2);
				Assert.assertTrue(coord2.isOnlineState());
				Assert.assertTrue(coord2.getAddress().getPort() == port2);
				fakeRelay1.shutdown();
				fakeRelay2.shutdown();
				Assert.assertEquals(result1.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
				Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
				Assert.assertTrue(feeder1.isSuccess());
				Assert.assertTrue(feeder2.isSuccess());
			}

			class ExternalViewChanger implements Runnable
			{
				ExternalViewChanger()
				{

				}

				@Override
        public void run()
				{
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e){}
					_client.onExternalViewChange("BizProfile", fakeview, fakeInverseView, fakeview, fakeInverseView);
				}
			}

			// Parallel Flush for 2 different registrations with an external view change in between
			{
				TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(port1, 9900, true, 50);
				fakeRelay1.start();

				TestRelayMaxSCNFinder.FakeRelay fakeRelay2 = new FakeRelay(port2, 9904, true, 50);
				fakeRelay2.start();

				DatabusSourcesConnection conn1 = _client.getDatabusSourcesConnection(reg1.getId().toString());
				SCN endSCN = new SingleSourceSCN(-1, 9850);
				conn1.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);
				DatabusSourcesConnection conn2 = _client.getDatabusSourcesConnection(reg2.getId().toString());
				conn2.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);

				CountDownLatch flushBeginLatch = new CountDownLatch(1);
				CountDownLatch flushEndLatch = new CountDownLatch(1);
				FlusherThread f1 = new FlusherThread(flushBeginLatch, flushEndLatch, reg1);
				FlusherThread f2 = new FlusherThread(flushBeginLatch, flushEndLatch, reg2);

				for (int i = 0; i < 10; i++)
				{
					(new ExternalViewChanger()).run();
				}
				new Thread(f1).start();
				for (int i = 0; i < 10; i++)
				{
					(new ExternalViewChanger()).run();
				}
				new Thread(f2).start();
				(new ExternalViewChanger()).run();

			    CountDownLatch feederBeginLatch = new CountDownLatch(2);
				CountDownLatch feederEndLatch = new CountDownLatch(2);

				FlushConsumerFeeder feeder1 = new FlushConsumerFeeder(conn1, feederBeginLatch, feederEndLatch, flushBeginLatch, 8904);
				FlushConsumerFeeder feeder2 = new FlushConsumerFeeder(conn2, feederBeginLatch, feederEndLatch, flushBeginLatch, 8904);

				new Timer().schedule(feeder1, 10);
				new Timer().schedule(feeder2, 10);

				flushEndLatch.await();
				feederEndLatch.await();

				RelayFlushMaxSCNResult result1 = f1.getResponse();
				RelayFlushMaxSCNResult result2 = f2.getResponse();
				Assert.assertEquals(result1.getFetchSCNResult().getResultSummary(), SummaryCode.SUCCESS);
				Assert.assertEquals(((SingleSourceSCN)result1.getFetchSCNResult().getMaxSCN()).getSequence(), 9904);
				Assert.assertEquals(result1.getFetchSCNResult().getMaxScnRelays().size(), 1);
				Assert.assertEquals(result2.getFetchSCNResult().getResultSummary(), SummaryCode.SUCCESS);
				Assert.assertEquals(((SingleSourceSCN)result2.getFetchSCNResult().getMaxSCN()).getSequence(), 9904);
				Assert.assertEquals(result2.getFetchSCNResult().getMaxScnRelays().size(), 1);
				DatabusServerCoordinates coord1 = (DatabusServerCoordinates)(result1.getFetchSCNResult().getMaxScnRelays().toArray()[0]);
			    DatabusServerCoordinates coord2 = (DatabusServerCoordinates)(result2.getFetchSCNResult().getMaxScnRelays().toArray()[0]);
				Assert.assertTrue(coord1.isOnlineState());
				Assert.assertTrue(coord1.getAddress().getPort() == port2);
				Assert.assertTrue(coord2.isOnlineState());
				Assert.assertTrue(coord2.getAddress().getPort() == port2);
				fakeRelay1.shutdown();
				fakeRelay2.shutdown();
				Assert.assertEquals(result1.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
				Assert.assertEquals(result2.getResultStatus(), RelayFlushMaxSCNResult.SummaryCode.MAXSCN_REACHED);
				Assert.assertTrue(feeder1.isSuccess());
				Assert.assertTrue(feeder2.isSuccess());
			}

			// Back to back flush on same registration after external view change. To ensure, flush is not holding up the lcoks
			{
				TestRelayMaxSCNFinder.FakeRelay fakeRelay1 = new FakeRelay(port1, 9900, true, 50);
				fakeRelay1.start();

				DatabusSourcesConnection conn1 = _client.getDatabusSourcesConnection(reg1.getId().toString());
				SCN endSCN = new SingleSourceSCN(-1, 9850);
				conn1.getRelayDispatcher().getDispatcherState().switchToEndStreamEventWindow(endSCN);

				_client.onExternalViewChange("BizProfile", fakeview, fakeInverseView, fakeview, fakeInverseView);

				FetchMaxSCNRequest request = new FetchMaxSCNRequest(500,3);
				FlushRequest flushRequest = new FlushRequest();
				flushRequest.setFlushTimeoutMillis(15000);
				RelayFlushMaxSCNResult result1 = reg1.flush(request, flushRequest);
				Assert.assertNotEquals(result1.getFetchSCNResult().getResultSummary(), com.linkedin.databus.client.pub.RelayFlushMaxSCNResult.SummaryCode.TIMED_OUT);
				RelayFlushMaxSCNResult result2 = reg1.flush(request, flushRequest);
				Assert.assertNotEquals(result2.getFetchSCNResult().getResultSummary(), com.linkedin.databus.client.pub.RelayFlushMaxSCNResult.SummaryCode.TIMED_OUT);
			}


		}


	private void registerRelayV3(int id, String name, InetSocketAddress addr, DatabusSubscription ds)
			throws InvalidConfigException
	{
		RuntimeConfigBuilder rtConfigBuilder =
				(RuntimeConfigBuilder)_client.getClientConfigManager().getConfigBuilder();

		ServerInfoBuilder relayConfigBuilder = rtConfigBuilder.getRelay(Integer.toString(id));
		relayConfigBuilder.setName(name);
		relayConfigBuilder.setHost(addr.getHostName());
		relayConfigBuilder.setPort(addr.getPort());
		relayConfigBuilder.setSources("BizProfile.BizCompany:77");
		_client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
	}


    private static void registerRelayV3(DatabusHttpV3ClientImpl client,
                                        int id, String name, InetSocketAddress addr,
                                        DatabusSubscription... subs)
            throws InvalidConfigException
    {
        RuntimeConfigBuilder rtConfigBuilder =
                (RuntimeConfigBuilder)client.getClientConfigManager().getConfigBuilder();
        StringBuilder sourceListStr = new StringBuilder();
        List<String> l =
            DatabusSubscription.createUriStringList(Arrays.asList(subs),
                                                    EspressoSubscriptionUriCodec.getInstance());
        for (String s: l)
        {
          if (sourceListStr.length() > 0) sourceListStr.append(',');
          sourceListStr.append(s);
        }

        ServerInfoBuilder relayConfigBuilder = rtConfigBuilder.getRelay(Integer.toString(id));
        relayConfigBuilder.setName(name);
        relayConfigBuilder.setHost(addr.getHostName());
        relayConfigBuilder.setPort(addr.getPort());
        relayConfigBuilder.setSources(sourceListStr.toString());
        client.getClientConfigManager().setNewConfig(rtConfigBuilder.build());
    }


}

