package com.linkedin.databus.client.registration;
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


import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.RuntimeConfigBuilder;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider.StaticConfig;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DatabusRegistration.RegistrationState;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.DbusPartitionListener;
import com.linkedin.databus.client.pub.DbusServerSideFilterFactory;
import com.linkedin.databus.client.pub.FileSystemCheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.cluster.DatabusCluster;
import com.linkedin.databus.cluster.DatabusCluster.DatabusClusterMember;
import com.linkedin.databus.cluster.DatabusClusterNotifier;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
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
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestDatabusV2ClusterRegistrationImpl 
{
	public static final Logger LOG = Logger.getLogger("TestDatabusV2ClusterRegistrationImpl");
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
		TestUtil.setupLogging(true, null, Level.INFO);
		InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

		//initialize relays
		for (int relayN = 0; relayN < RELAY_PORT.length; ++relayN)
		{
			_dummyServer[relayN] = new SimpleTestServerConnection(new DbusEventV2Factory().getByteOrder(),
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
		bufCfgBuilder.setAverageEventSize(1);

		_bufCfg = bufCfgBuilder.build();
	}
	
	@Test
	public void testRegistration() throws Exception
	{
		DatabusHttpClientImpl client = null;

		try
		{
			DatabusHttpClientImpl.Config clientConfig = new DatabusHttpClientImpl.Config();
			clientConfig.getContainer().getJmx().setRmiEnabled(false);
			clientConfig.getContainer().setHttpPort(12003);
			client = new DatabusHttpClientImpl(clientConfig);

			registerRelay(1, "relay1", new InetSocketAddress("localhost", 8888), "S1,S2", client);
			registerRelay(2, "relay2", new InetSocketAddress("localhost", 7777), "S1,S3", client);
			registerRelay(3, "relay1.1", new InetSocketAddress("localhost", 8887), "S1,S2", client);
			registerRelay(4, "relay3", new InetSocketAddress("localhost", 6666), "S3,S4,S5", client);

			TestDbusPartitionListener listener = new TestDbusPartitionListener();
			StaticConfig ckptConfig = new StaticConfig("localhost:1356", "dummy", 1,1);
			DbusClusterInfo clusterInfo = new DbusClusterInfo("dummy", 10,1);
			
			DatabusV2ClusterRegistrationImpl reg = new TestableDatabusV2ClusterRegistrationImpl(null,
					                                                                       client, 
					                                                                       ckptConfig,
					                                                                       clusterInfo,
					                                                                       new TestDbusClusterConsumerFactory(),
					                                                                       new TestDbusServerSideFilterFactory(),
					                                                                       listener,
					                                                                       "S1", "S3");
			
			// Start
			reg.start();
			assertEquals("State CHeck", reg.getState(), RegistrationState.STARTED);
			
			// Add Partition(s)
			reg.onGainedPartitionOwnership(1);
			assertEquals("Listener called ", listener.isAddPartitionCalled(1), true);
			reg.onGainedPartitionOwnership(2);
			assertEquals("Listener called ", listener.isAddPartitionCalled(2), true);
			assertEquals("Partition Regs size ", 2, reg.getPartitionRegs().size());
			reg.onGainedPartitionOwnership(3);
			assertEquals("Listener called ", listener.isAddPartitionCalled(3), true);
			assertEquals("Partition Regs size ", 3, reg.getPartitionRegs().size());
			reg.onGainedPartitionOwnership(4);
			assertEquals("Listener called ", listener.isAddPartitionCalled(4), true);
			//duplicate call
			listener.clearCallbacks();
			reg.onGainedPartitionOwnership(4);
			assertEquals("Listener called ", listener.isAddPartitionCalled(4), false);
			assertEquals("Partition Regs size ", 4, reg.getPartitionRegs().size());
			
			List<String> gotPartitionList = new ArrayList<String>();
			for (DbusPartitionInfo p : reg.getPartitions())
				gotPartitionList.add(p.toString());
			Collections.sort(gotPartitionList);
			
			assertEquals("Partitions Check", gotPartitionList.toString(), "[1, 2, 3, 4]");
			
			// Drop Partitions
			reg.onLostPartitionOwnership(1);
			gotPartitionList.clear();
			for (DbusPartitionInfo p : reg.getPartitions())
				gotPartitionList.add(p.toString());
			Collections.sort(gotPartitionList);
			
			assertEquals("Partitions Check", "[2, 3, 4]", gotPartitionList.toString());
			assertEquals("Listener called ", true, listener.isDropPartitionCalled(1));
			
			reg.onLostPartitionOwnership(2);
			assertEquals("Listener called ", true, listener.isDropPartitionCalled(2));
			//duplicate call
			listener.clearCallbacks();
			reg.onLostPartitionOwnership(2);
			assertEquals("Listener called ", false, listener.isDropPartitionCalled(2));
			assertEquals("Partitions Check", "[3, 4]", reg.getPartitions().toString());
			assertEquals("Partition Regs size ", 2, reg.getPartitionRegs().size());

			reg.onReset(3);
			assertEquals("Listener called ", true, listener.isDropPartitionCalled(3));
			//duplicate call
			listener.clearCallbacks();
			reg.onReset(3);
			assertEquals("Listener called ", false, listener.isDropPartitionCalled(3));
			assertEquals("Partitions Check", "[4]", reg.getPartitions().toString());
			assertEquals("Partition Regs size ", 1, reg.getPartitionRegs().size());
			
			reg.onError(4);
			assertEquals("Listener called ", true, listener.isDropPartitionCalled(4));
			//duplicate call
			listener.clearCallbacks();
			reg.onError(4);
			assertEquals("Listener called ", false, listener.isDropPartitionCalled(4));
			assertEquals("Partitions Check", "[]", reg.getPartitions().toString());
			assertEquals("Partition Regs size ", 0, reg.getPartitionRegs().size());
			
			// Add Partiton 1 again
			listener.clearCallbacks();
			reg.onGainedPartitionOwnership(1);
			assertEquals("Listener called ", listener.isAddPartitionCalled(1), true);
			assertEquals("Partition Regs size ", 1, reg.getPartitionRegs().size());
			assertEquals("Child State CHeck", reg.getPartitionRegs().values().iterator().next().getState(), RegistrationState.STARTED);

			// Pausing 
			reg.pause();
			assertEquals("State CHeck", reg.getState(), RegistrationState.PAUSED);
			assertEquals("Child State CHeck", reg.getPartitionRegs().values().iterator().next().getState(), RegistrationState.PAUSED);

			// Resume
			reg.resume();
			assertEquals("State CHeck", reg.getState(), RegistrationState.RESUMED);
			assertEquals("Child State CHeck", reg.getPartitionRegs().values().iterator().next().getState(), RegistrationState.RESUMED);

			// Suspended 
			reg.suspendOnError(null);
			assertEquals("State CHeck", reg.getState(), RegistrationState.SUSPENDED_ON_ERROR);
			assertEquals("Child State CHeck", reg.getPartitionRegs().values().iterator().next().getState(), RegistrationState.SUSPENDED_ON_ERROR);
			
			// resume
			reg.resume();
			assertEquals("State CHeck", reg.getState(), RegistrationState.RESUMED);
			assertEquals("Child State CHeck", reg.getPartitionRegs().values().iterator().next().getState(), RegistrationState.RESUMED);
			
			
			// Active node change notification
			List<String> newActiveNodes = new ArrayList<String>();
			newActiveNodes.add("localhost:7070");
			newActiveNodes.add("localhost:8080");
			newActiveNodes.add("localhost:9090");
			reg.onInstanceChange(newActiveNodes);
			assertEquals("Active Nodes", newActiveNodes, reg.getCurrentActiveNodes());
			newActiveNodes.remove(2);
			reg.onInstanceChange(newActiveNodes);
			assertEquals("Active Nodes", newActiveNodes, reg.getCurrentActiveNodes());
			newActiveNodes.add("localhost:1010");
			reg.onInstanceChange(newActiveNodes);
			assertEquals("Active Nodes", newActiveNodes, reg.getCurrentActiveNodes());
			
			// Partition Mapping change notification
			Map<Integer, String> activePartitionMap = new HashMap<Integer, String>();
			for (int i = 0 ; i < 10; i++)
			{
				String node = null;
				if (i%2 == 0)
					node = "localhost:8080";
				else
					node = "localhost:7070";
				activePartitionMap.put(i, node);
			}
			reg.onPartitionMappingChange(activePartitionMap);
			assertEquals("Partition Mapping Check", activePartitionMap, reg.getActivePartitionMap());
			
			activePartitionMap.remove(9);
			reg.onPartitionMappingChange(activePartitionMap);
			assertEquals("Partition Mapping Check", activePartitionMap, reg.getActivePartitionMap());

			activePartitionMap.put(9,"localhost:8708");
			reg.onPartitionMappingChange(activePartitionMap);
			assertEquals("Partition Mapping Check", activePartitionMap, reg.getActivePartitionMap());

			// shutdown
			reg.shutdown();
			assertEquals("State Check", reg.getState(), RegistrationState.SHUTDOWN);
			assertEquals("Child State CHeck", reg.getPartitionRegs().values().iterator().next().getState(), RegistrationState.SHUTDOWN);
			
			// Operations during shutdown state
			boolean gotException = false;
			try
			{
				reg.onGainedPartitionOwnership(1);
			} catch (IllegalStateException ex) {
				gotException = true;
			}
			assertEquals("Exception", true, gotException);

			gotException = false;
			try
			{
				reg.onLostPartitionOwnership(1);
			} catch (IllegalStateException ex) {
				gotException = true;
			}
			assertEquals("Exception", true, gotException);

			
			gotException = false;
			try
			{
				reg.pause();
			} catch (IllegalStateException ex) {
				gotException = true;
			}
			assertEquals("Exception", true, gotException);

			gotException = false;
			try
			{
				reg.suspendOnError(null);
			} catch (IllegalStateException ex) {
				gotException = true;
			}
			assertEquals("Exception", true, gotException);

			gotException = false;
			try
			{
				reg.resume();
			} catch (IllegalStateException ex) {
				gotException = true;
			}
			assertEquals("Exception", true, gotException);
			
			// deregister
			reg.deregister();
			assertEquals("State Check", reg.getState(), RegistrationState.DEREGISTERED);
			assertEquals("Child State CHeck", 0,reg.getPartitionRegs().size());
			
		} finally {
			if ( null != client)
				client.shutdown();
		}
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

	static class TestConsumer extends AbstractDatabusCombinedConsumer
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
	
	public class TestDbusClusterConsumerFactory 
			implements DbusClusterConsumerFactory
	{
		@Override
		public Collection<DatabusCombinedConsumer> createPartitionedConsumers(
				DbusClusterInfo clusterInfo, DbusPartitionInfo partitionInfo) {
			TestConsumer consumer = new TestConsumer();
			List<DatabusCombinedConsumer> consumers = new ArrayList<DatabusCombinedConsumer>();
			consumers.add(consumer);
			return consumers;
		}
	}
	
	public class TestDbusServerSideFilterFactory 
	    implements DbusServerSideFilterFactory
	{
		@Override
		public DbusKeyCompositeFilterConfig createServerSideFilter(
				DbusClusterInfo cluster, DbusPartitionInfo partition)
				throws InvalidConfigException {
			return null;
		}	    
	}
	
	public class TestDbusPartitionListener 
	    implements DbusPartitionListener
	{

		private Map<Long, Boolean> addCallbacks = new HashMap<Long, Boolean>();
		private Map<Long, Boolean> dropCallbacks = new HashMap<Long, Boolean>();

		@Override
		public void onAddPartition(DbusPartitionInfo partitionInfo,
				DatabusRegistration reg) {			
			addCallbacks.put(partitionInfo.getPartitionId(), true);
		}

		@Override
		public void onDropPartition(DbusPartitionInfo partitionInfo,
				DatabusRegistration reg) {
			dropCallbacks.put(partitionInfo.getPartitionId(), true);
		}

		public boolean isAddPartitionCalled(long partition) {
			Boolean o = addCallbacks.get(partition);
			
			if ( null == o)
				return false;
			
			return o;
		}

		public boolean isDropPartitionCalled(long partition) {
			Boolean o = dropCallbacks.get(partition);
			
			if ( null == o)
				return false;
			
			return o;		
		}
		
		public void clearCallbacks()
		{
			addCallbacks.clear();
			dropCallbacks.clear();
		}
		
	}
	
	
	public class TestableDatabusV2ClusterRegistrationImpl 
		extends DatabusV2ClusterRegistrationImpl
	{

		public TestableDatabusV2ClusterRegistrationImpl(RegistrationId id,
				DatabusHttpClientImpl client,
				StaticConfig ckptPersistenceProviderConfig,
				DbusClusterInfo clusterInfo,
				DbusClusterConsumerFactory consumerFactory,
				DbusServerSideFilterFactory filterFactory,
				DbusPartitionListener partitionListener, String ... sources) {
			super(id, client, ckptPersistenceProviderConfig, clusterInfo, consumerFactory,
					filterFactory, partitionListener, sources);
		}
		
		@Override
		protected DatabusCluster createCluster() throws Exception
		{
			return new TestDatabusCluster();
		}
		
		@Override
		public CheckpointPersistenceProvider createCheckpointPersistenceProvider(DbusPartitionInfo partition) 
				throws InvalidConfigException
		{
			return new FileSystemCheckpointPersistenceProvider();
		}
		
	}
	
	public class TestDatabusCluster 
		extends DatabusCluster
	{
		
		public TestDatabusCluster()
		{
			
		}
		
		public DatabusClusterMember addMember(String id,DatabusClusterNotifier notifier)
		{
			return new TestDatabusClusterMember(this);
		}
		
		public void start()
		{
		}
	}
	
	public class TestDatabusClusterMember 
	   extends DatabusClusterMember
	{

		TestDatabusClusterMember(DatabusCluster databusCluster) {
			databusCluster.super();
		}
	
		@Override
		public boolean join()
		{
			return true;
		}
		
		@Override
		public boolean leave()
		{
			return true;
		}
	}
}
