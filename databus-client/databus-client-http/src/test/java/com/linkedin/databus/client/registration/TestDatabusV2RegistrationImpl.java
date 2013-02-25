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
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DatabusRegistration.RegistrationState;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.core.DatabusComponentStatus.Status;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
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

public class TestDatabusV2RegistrationImpl 
{
	public static final Logger LOG = Logger.getLogger("TestDatabusV2RegistrationImpl");
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

	@Test
	public void testOneConsumerRegistrationOps() throws Exception
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

			TestConsumer listener1 = new TestConsumer();
			DatabusRegistration reg = client.register(listener1, "S1", "S3");

			assertEquals("Registered State", RegistrationState.REGISTERED,
					reg.getState());

			assertEquals("Component Name" , "Status_TestConsumer_a62d57a7", reg.getStatus().getComponentName());
			assertEquals("Component Status", Status.INITIALIZING, reg.getStatus().getStatus());


			// Start
			boolean started = reg.start();
			assertEquals("Started", true, started);
			assertEquals("Registered State", RegistrationState.STARTED, reg.getState());
			assertEquals("Component Status", Status.RUNNING, reg.getStatus().getStatus());
			
			
			//Start again
			started = reg.start();
			assertEquals("Started", false, started);
			assertEquals("Registered State", RegistrationState.STARTED, reg.getState());

			// Pause
			reg.pause();
			assertEquals("Registered State", RegistrationState.PAUSED, reg.getState());
			assertEquals("Component Status", Status.PAUSED, reg.getStatus().getStatus());

			// resume
			reg.resume();
			assertEquals("Registered State", RegistrationState.RESUMED, reg.getState());
			assertEquals("Component Status", Status.RUNNING, reg.getStatus().getStatus());

			// suspend due to error
			reg.suspendOnError(new Exception("dummy"));
			assertEquals("Registered State", RegistrationState.SUSPENDED_ON_ERROR, reg.getState());
			assertEquals("Component Status", Status.SUSPENDED_ON_ERROR, reg.getStatus().getStatus());

			// SHutdown
			reg.shutdown();
			assertEquals("Registered State", RegistrationState.SHUTDOWN, reg.getState());
			assertEquals("Component Status", Status.SHUTDOWN, reg.getStatus().getStatus());   
			
			// Duplicate regId
			DatabusRegistration reg2 = client.register(listener1, "S1", "S3");
			boolean isException = false;
			try
			{
				reg2.withRegId(reg.getRegistrationId());
			} catch (DatabusClientException ex) {
				isException = true;
			}

			assertEquals("Exception expected", true, isException);

			reg2.deregister();
			reg.deregister();
			
		} finally {
			if ( null != client)
				client.shutdown();
		}
	}
	
	@Test
	public void testErrorRegistration() throws Exception
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

			TestConsumer listener1 = new TestConsumer();
			DatabusRegistration reg = client.register(listener1, "S6", "S2");

			assertEquals("Registered State", RegistrationState.REGISTERED,
					reg.getState());

			assertEquals("Component Name" , "Status_TestConsumer_6fdc9d8d", reg.getStatus().getComponentName());
			assertEquals("Component Status", Status.INITIALIZING, reg.getStatus().getStatus());


			// Start
			boolean started = false;
			boolean gotException = false;
			try
			{
				started = reg.start();
			} catch (DatabusClientException ex) {
				gotException = true;
			}
			
			assertEquals("gotException", true, gotException);
			assertEquals("Registered State", RegistrationState.REGISTERED, reg.getState());
			assertEquals("Component Status", Status.INITIALIZING, reg.getStatus().getStatus());
			
			gotException = false;
			try
			{
				reg = client.register((AbstractDatabusCombinedConsumer)null, "S6", "S2");
			} catch (DatabusClientException ex) {
				gotException = true;
			}
			assertEquals("gotException", true, gotException);

			gotException = false;
			try
			{
				reg = client.register(listener1, null);
			} catch (DatabusClientException ex) {
				gotException = true;
			}
			assertEquals("gotException", true, gotException);
			
			if ( reg != null)
				reg.deregister();
			
		} finally {
			if ( null != client)
				client.shutdown();
		}
	}
	
	@Test
	public void testMultiConsumerRegistrationOps() throws Exception
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

			TestConsumer listener1 = new TestConsumer();
			TestConsumer listener2 = new TestConsumer();

			List<DatabusCombinedConsumer> listeners = new ArrayList<DatabusCombinedConsumer>();
			listeners.add(listener1);
			listeners.add(listener2);

			DatabusRegistration reg = client.register(listeners, "S1", "S2");

			assertEquals("Registered State", RegistrationState.REGISTERED,
					reg.getState());

			assertEquals("Component Name" , "Status_TestConsumer_922c5e28", reg.getStatus().getComponentName());
			assertEquals("Component Status", Status.INITIALIZING, reg.getStatus().getStatus());


			// Start
			boolean started = reg.start();
			assertEquals("Started", true, started);
			assertEquals("Registered State", RegistrationState.STARTED, reg.getState());
			assertEquals("Component Status", Status.RUNNING, reg.getStatus().getStatus());

			//Start again
			started = reg.start();
			assertEquals("Started", false, started);
			assertEquals("Registered State", RegistrationState.STARTED, reg.getState());

			// Pause
			reg.pause();
			assertEquals("Registered State", RegistrationState.PAUSED, reg.getState());
			assertEquals("Component Status", Status.PAUSED, reg.getStatus().getStatus());

			// resume
			reg.resume();
			assertEquals("Registered State", RegistrationState.RESUMED, reg.getState());
			assertEquals("Component Status", Status.RUNNING, reg.getStatus().getStatus());

			// suspend due to error
			reg.suspendOnError(new Exception("dummy"));
			assertEquals("Registered State", RegistrationState.SUSPENDED_ON_ERROR, reg.getState());
			assertEquals("Component Status", Status.SUSPENDED_ON_ERROR, reg.getStatus().getStatus());

			// SHutdown
			reg.shutdown();
			assertEquals("Registered State", RegistrationState.SHUTDOWN, reg.getState());
			assertEquals("Component Status", Status.SHUTDOWN, reg.getStatus().getStatus());   
			
			
			reg.deregister();
		} finally {
			if ( null != client )
				client.shutdown();
		}
	}
	

	@BeforeClass
	public void setUpClass() throws InvalidConfigException
	{
		//setup logging
		TestUtil.setupLogging(true, null, Level.INFO);
		InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

		//initialize relays
		for (int relayN = 0; relayN < RELAY_PORT.length; ++relayN)
		{
			_dummyServer[relayN] = new SimpleTestServerConnection(DbusEventV1.byteOrder,
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
}
