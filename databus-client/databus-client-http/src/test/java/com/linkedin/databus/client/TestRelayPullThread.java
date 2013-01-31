package com.linkedin.databus.client;
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


import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.IArgumentMatcher;
import org.easymock.classextension.EasyMock;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ConnectionState.StateId;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DatabusComponentStatus.Status;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.Range;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.test.TestUtil;

/**
 * Unit tests for the {@link RelayPullThread} class
 * @author cbotev
 */
public class TestRelayPullThread
{
  public static final Logger LOG = Logger.getLogger("TestRelayPullThread");

  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"LiarJobRelay\",\"namespace\":\"com.linkedin.events.liar.jobrelay\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"eventId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=EVENT_ID;dbFieldPosition=2;\"},{\"name\":\"isDelete\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_DELETE;dbFieldPosition=3;\"},{\"name\":\"state\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STATE;dbFieldPosition=4;\"}],\"meta\":\"dbFieldName=SY$LIAR_JOB_RELAY_1;\"}");

  @BeforeClass
  public void setUp()
  {
    TestUtil.setupLogging(true, null, Level.OFF);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
  }


  private RelayPullThread createFellOffRelayPullThread(boolean failRelayConnection, boolean muteTransition)
		  	throws Exception
  {
	  RelayPullThread r = createRelayPullThread(failRelayConnection, muteTransition, false, false, false, false, null);
	  r.getConnectionState().setRelayFellOff(true);
	  return r;
  }

  private RelayPullThread createRelayPullThread(boolean failRelayConnection, boolean muteTransition)
  	throws Exception
  {
	 return createRelayPullThread(failRelayConnection, muteTransition, false, false, false, false, null);
  }

  private RelayPullThread createRelayPullThread(boolean failRelayConnection, boolean muteTransition,
                                                boolean bootstrapEnabled)
    throws Exception
  {
     return createRelayPullThread(failRelayConnection, muteTransition, bootstrapEnabled,
                                  false, false, false, null);
  }

  private RelayPullThread createRelayPullThread(boolean failRelayConnection,
			boolean muteTransition,
			boolean bootstrapEnabled,
			boolean readLatestScnEnabled,
			boolean readDataThrowException,
			boolean readDataException,
			String exceptionName)
  		throws Exception
  {
	  return createRelayPullThread(failRelayConnection,
			  					   muteTransition,
			  					   bootstrapEnabled,
			  					   readLatestScnEnabled,
			  					   readDataThrowException,
			  					   readDataException,
			  					   exceptionName, 0);

  }

  private RelayPullThread createRelayPullThread(boolean failRelayConnection,
		  										boolean muteTransition,
		  										boolean bootstrapEnabled,
		  										boolean readLatestScnEnabled,
		  										boolean readDataThrowException,
		  										boolean readDataException,
		  										String exceptionName,
		  										int numRetriesOnFellOff)
		  	throws Exception
  {
	    List<String> sources = Arrays.asList("source1");

	    Properties clientProps = new Properties();
	    if ( bootstrapEnabled)
	    {
  	      clientProps.setProperty("client.runtime.bootstrap.enabled", "true");
		  clientProps.setProperty("client.runtime.bootstrap.service(1).name", "bs1");
		  clientProps.setProperty("client.runtime.bootstrap.service(1).port", "10001");
		  clientProps.setProperty("client.runtime.bootstrap.service(1).sources", "source1");
	    } else {
		  clientProps.setProperty("client.runtime.bootstrap.enabled", "false");
  		}

	    clientProps.setProperty("client.container.jmx.rmiEnabled", "false");

	    clientProps.setProperty("client.runtime.relay(1).name", "relay1");
	    clientProps.setProperty("client.runtime.relay(1).port", "10001");
	    clientProps.setProperty("client.runtime.relay(1).sources", "source1");
	    clientProps.setProperty("client.runtime.relay(2).name", "relay2");
	    clientProps.setProperty("client.runtime.relay(2).port", "10002");
	    clientProps.setProperty("client.runtime.relay(2).sources", "source1");
	    clientProps.setProperty("client.runtime.relay(3).name", "relay3");
	    clientProps.setProperty("client.runtime.relay(3).port", "10003");
	    clientProps.setProperty("client.runtime.relay(3).sources", "source1");

	    if (readLatestScnEnabled)
	      clientProps.setProperty("client.enableReadLatestOnRelayFallOff", "true");

	    clientProps.setProperty("client.connectionDefaults.eventBuffer.maxSize", "100000");
	    clientProps.setProperty("client.connectionDefaults.pullerRetries.maxRetryNum", "9");
	    clientProps.setProperty("client.connectionDefaults.pullerRetries.sleepIncFactor", "1.0");
	    clientProps.setProperty("client.connectionDefaults.pullerRetries.sleepIncDelta", "1");
	    clientProps.setProperty("client.connectionDefaults.pullerRetries.initSleep", "1");
	    clientProps.setProperty("client.connectionDefaults.numRetriesOnFallOff", "" + numRetriesOnFellOff);

	    DatabusHttpClientImpl.Config clientConfBuilder = new DatabusHttpClientImpl.Config();
	    ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
	        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("client.", clientConfBuilder);
	    configLoader.loadConfig(clientProps);

	    DatabusHttpClientImpl.StaticConfig clientConf = clientConfBuilder.build();
	    DatabusSourcesConnection.StaticConfig srcConnConf = clientConf.getConnectionDefaults();

	    DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConf);

	    if (bootstrapEnabled)
	      client.registerDatabusBootstrapListener(new LoggingConsumer(), null, "source1");

	    Assert.assertNotNull(client, "client instantiation ok");

	    DatabusHttpClientImpl.RuntimeConfig clientRtConf = clientConf.getRuntime().build();

	    //we keep the index of the next server we expect to see
	    AtomicInteger serverIdx = new AtomicInteger(-1);

	    //generate the order in which we should see the servers
	    List<ServerInfo> relayOrder = new ArrayList<ServerInfo>(clientRtConf.getRelays());
	    if (LOG.isInfoEnabled())
	    {
	      StringBuilder sb = new StringBuilder();
	      for (ServerInfo serverInfo: relayOrder)
	      {
	        sb.append(serverInfo.getName());
	        sb.append(" ");
	      }
	      LOG.info("Relay order:" + sb.toString());
	    }

	    List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
	    sourcesResponse.add(new IdNamePair(1L, "source1"));

	    Map<Long, List<RegisterResponseEntry>> registerResponse = new HashMap<Long, List<RegisterResponseEntry>>();

	    List<RegisterResponseEntry> regResponse = new ArrayList<RegisterResponseEntry>();
	    regResponse.add(new RegisterResponseEntry(1L, (short)1, SCHEMA$.toString()));
	    registerResponse.put(1L, regResponse);

	    ChunkedBodyReadableByteChannel channel = EasyMock.createMock(ChunkedBodyReadableByteChannel.class);

	    if ( ! readDataException)
	    {
	    	org.easymock.EasyMock.expect(channel.getMetadata(org.easymock.EasyMock.<String>notNull())).andReturn(null).anyTimes();
	    } else {
	    	org.easymock.EasyMock.expect(channel.getMetadata(org.easymock.EasyMock.<String>notNull())).andReturn(exceptionName).anyTimes();
	    }
	    EasyMock.replay(channel);

	    DbusEventBuffer dbusBuffer = EasyMock.createMock(DbusEventBuffer.class);
	    org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull(), org.easymock.EasyMock.<List<InternalDatabusEventsListener>>notNull(), org.easymock.EasyMock.<DbusEventsStatisticsCollector>isNull())).andReturn(1).anyTimes();
	    org.easymock.EasyMock.expect(dbusBuffer.getSeenEndOfPeriodScn()).andReturn(1L).anyTimes();
	    if ( readDataThrowException)
	    {
		  org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull())).andThrow(new RuntimeException("dummy")).anyTimes();
	    } else {
	      org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull())).andReturn(1).anyTimes();
	    }

	    org.easymock.EasyMock.expect(dbusBuffer.acquireIterator(org.easymock.EasyMock.<String>notNull())).andReturn(null).anyTimes();
        dbusBuffer.waitForFreeSpace((int)(10000 * 100.0 / clientConf.getPullerBufferUtilizationPct()));
	    org.easymock.EasyMock.expectLastCall().anyTimes();
	    org.easymock.EasyMock.expect(dbusBuffer.getBufferFreeReadSpace()).andReturn(0).anyTimes();

	    EasyMock.replay(dbusBuffer);

	    //This guy succeeds on /sources but fails on /register
	    MockRelayConnection mockSuccessConn = new MockRelayConnection(sourcesResponse, registerResponse, channel,
	                                                                  serverIdx, muteTransition);

	    DatabusRelayConnectionFactory mockConnFactory =
	            EasyMock.createMock("mockRelayFactory", DatabusRelayConnectionFactory.class);

	    //each server should be tried MAX_RETRIES time until all retries are exhausted

	    if ( failRelayConnection )
	    {
		    org.easymock.EasyMock.expect(mockConnFactory.createRelayConnection(
			          org.easymock.EasyMock.<ServerInfo>notNull(),
			          org.easymock.EasyMock.<ActorMessageQueue>notNull(),
			          org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andThrow(new RuntimeException("Mock Error")).anyTimes();
	    } else {
	    	org.easymock.EasyMock.expect(mockConnFactory.createRelayConnection(
	          org.easymock.EasyMock.<ServerInfo>notNull(),
	          org.easymock.EasyMock.<ActorMessageQueue>notNull(),
	          org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockSuccessConn).anyTimes();
	    }


	    List<DatabusSubscription> sourcesSubList = DatabusSubscription.createSubscriptionList(sources);

		// Mock Bootstrap Puller
		BootstrapPullThread mockBsPuller = EasyMock.createMock("bpt",BootstrapPullThread.class);
		mockBsPuller.enqueueMessage(org.easymock.EasyMock.notNull());
		org.easymock.EasyMock.expectLastCall().anyTimes();

		// Mock Relay Dispatcher
		RelayDispatcher mockDispatcher = EasyMock.createMock("rd", RelayDispatcher.class);
		mockDispatcher.enqueueMessage(org.easymock.EasyMock.notNull());
		org.easymock.EasyMock.expectLastCall().anyTimes();

	   DatabusSourcesConnection sourcesConn2 = EasyMock.createMock(DatabusSourcesConnection.class);

	   DatabusSourcesConnection.SourcesConnectionStatus scs = sourcesConn2.new SourcesConnectionStatus();
	   org.easymock.EasyMock.expect(sourcesConn2.getSourcesNames()).andReturn(Arrays.asList("source1")).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getSubscriptions()).andReturn(sourcesSubList).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getConnectionConfig()).andReturn(srcConnConf).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getConnectionStatus()).andReturn(scs).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getLocalRelayCallsStatsCollector()).andReturn(null).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getInboundEventsStatsCollector()).andReturn(null).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getRelayCallsStatsCollector()).andReturn(null).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getRelayConnFactory()).andReturn(mockConnFactory).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.loadPersistentCheckpoint()).andReturn(null).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getDataEventsBuffer()).andReturn(dbusBuffer).anyTimes();

	   if ( bootstrapEnabled)
	     org.easymock.EasyMock.expect(sourcesConn2.isBootstrapEnabled()).andReturn(true).anyTimes();
	   else
		 org.easymock.EasyMock.expect(sourcesConn2.isBootstrapEnabled()).andReturn(false).anyTimes();

	   org.easymock.EasyMock.expect(sourcesConn2.getBootstrapConsumers()).andReturn(null).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getBootstrapServices()).andReturn(null).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getBootstrapPuller()).andReturn(mockBsPuller).anyTimes();
	   org.easymock.EasyMock.expect(sourcesConn2.getRelayDispatcher()).andReturn(mockDispatcher).anyTimes();


	   EasyMock.makeThreadSafe(mockConnFactory, true);
	   EasyMock.makeThreadSafe(mockDispatcher, true);
	   EasyMock.makeThreadSafe(mockBsPuller, true);
	   EasyMock.makeThreadSafe(sourcesConn2, true);

	   EasyMock.replay(mockConnFactory);
	   EasyMock.replay(sourcesConn2);
	   EasyMock.replay(mockDispatcher);
	   EasyMock.replay(mockBsPuller);
	   RelayPullThread relayPuller = new RelayPullThread("RelayPuller", sourcesConn2, dbusBuffer, clientRtConf.getRelaysSet(),
			   new ArrayList<DbusKeyCompositeFilterConfig>(),
			   srcConnConf.getConsumeCurrent(),
			   srcConnConf.getReadLatestScnOnError(),
			   srcConnConf.getPullerUtilizationPct(),
			   ManagementFactory.getPlatformMBeanServer());
	   RemoteExceptionHandler mockRemoteExceptionHandler = new MockRemoteExceptionHandler(sourcesConn2, dbusBuffer, relayPuller);

	   Field field = relayPuller.getClass().getDeclaredField("_remoteExceptionHandler");
	   field.setAccessible(true);
	   field.set(relayPuller, mockRemoteExceptionHandler);

	   mockSuccessConn.setCallback(relayPuller);

	   return relayPuller;
  }

  @Test(groups={"small", "functional"})
  public void testRelayTransition() throws Exception
  {
	   // Test Case : Initial State Check
	   {
		   RelayPullThread relayPuller = createRelayPullThread(false, false);
		   Assert.assertEquals(relayPuller.getConnectionState().getStateId(), StateId.INITIAL, "Initial State Check");

		   //Let the show begin
		   Thread relayPullerThread = new Thread(relayPuller);
		   relayPullerThread.setDaemon(false);
		   relayPullerThread.start();

		   relayPuller.enqueueMessage(LifecycleMessage.createStartMessage());

		   try
		   {
			   Thread.sleep(500);
		   } catch (InterruptedException ie){}

		   for (int i = 0 ; i < 100; i++ )
		   {
			   try
			   {
				   Thread.sleep(5);
				   Assert.assertTrue(relayPuller.getConnectionState().getStateId() != StateId.INITIAL,"StateId can never be INITIAL once started");
			   } catch (InterruptedException ie){}
		   }
		   //EasyMock.verify(mockConnFactory);

		   relayPuller.enqueueMessage(LifecycleMessage.createShutdownMessage());
		   relayPuller.awaitShutdown();
	   }


	   // Main Transition Test (Without Server-Set Change)
	   {
		   // PICK_SERVER - Happy Path
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);

			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
		   }

		   // PICK_SERVER  - Relays exhausted
		   {
			   RelayPullThread relayPuller = createRelayPullThread(true, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.PICK_SERVER, "SUSPEND_ON_ERROR");
		   }


		   // PICK_SERVER  - No Servers
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   relayPuller.getServers().clear();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.PICK_SERVER, "SUSPEND_ON_ERROR");
		   }


		   // Request_Sources to Re Sources_Request_Sent
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
		   }


		   // Request_Sources to Sources_Response_Success
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
		   }

		   // Sources_Response_Success - Happy Path
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);

			   String expSubsListStr = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"ANY\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":1,\"name\":\"source1\"},\"id\":-1}}]";
			   String expSourcesIdListStr = "1";
			   Assert.assertEquals(connState.getSourcesIdListString(), expSourcesIdListStr, "SourcesId Added");
			   String subsListStr = connState.getSubsListString();
			   boolean value = subsListStr.equals(expSubsListStr);
			   Assert.assertTrue(value);
		   }

		   // Sources_Response_Success - When source not found in server
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   connState.getSourcesNameMap().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.PICK_SERVER);
		   }

		   // Request_Register to Register_Request_Sent
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");
		   }

		   // Request_Register to Register_Response_success
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
		   }

		   // Register_Response_Success : Error Case
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   connState.getSourcesSchemas().clear();
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.PICK_SERVER);
		   }

		   // Register_Response_Success to Request Stream
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   Assert.assertEquals(relayPuller.getConnectionState().isSCNRegress(), false, "SCN Regress check");
		   }

		   // Register_Response_Success to Request Stream, when partially consumed window
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
			   cp.setWindowScn(100L);
			   cp.setWindowOffset(20);
			   cp.setPrevScn(80L);
			   relayPuller.getConnectionState().setCheckpoint(cp);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   Assert.assertEquals(relayPuller.getConnectionState().getCheckpoint().getWindowScn(), 80L, "WindowSCN check");
			   Assert.assertEquals(relayPuller.getConnectionState().getCheckpoint().getWindowOffset(), new Long(-1), "WindowOffset check");
			   Assert.assertEquals(relayPuller.getConnectionState().isSCNRegress(), true, "SCN Regress check");
		   }

		   // Register_Response_Success, No PrevSCN for partially consumed window
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
			   cp.setWindowScn(100L);
			   cp.setWindowOffset(20);
			   relayPuller.getConnectionState().setCheckpoint(cp);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS,StateId.REGISTER_RESPONSE_SUCCESS, "SUSPEND_ON_ERROR");
		   }

		   // Register_Response_Success to Bootstrap ( when checkpoint is Bootstrap_SnapShot
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Is Relay FellOff");

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   connState.setCheckpoint(cp);
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.BOOTSTRAP);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   Assert.assertEquals(relayPuller.getConnectionState().isSCNRegress(), false, "SCN Regress check");
		   }

		   // Register_Response_Success to Bootstrap ( when checkpoint is Bootstrap_Catchup
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Is Relay FellOff");

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   connState.setCheckpoint(cp);
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.BOOTSTRAP);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   Assert.assertEquals(relayPuller.getConnectionState().isSCNRegress(), false, "SCN Regress check");
		   }


		   // Request_Stream to Request_Stream_Sent
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");
			   relayPuller.getMessageQueue().clear();
			   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
			   connState.switchToRegisterSuccess(conn.getRegisterResponse());
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "");
		   }

		   // Request_Stream to Stream_Response_Success
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
		   }


		   //Stream_Request_Success to Stream_Request_Done
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_RESPONSE_DONE);
		   }


		   //Stream_Request_Success : ScnNotFoundException but retries set to 5 and  bootstrap enabled
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName(),5);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();

			   for ( int i = 1; i <= 6 ; i++)
			   {
				   System.out.println("Iteration :" + i);
				   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   if ( i < 6 )
				   {
					   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.PICK_SERVER);
					   Assert.assertEquals(relayPuller.getRetryonFallOff().getRemainingRetriesNum(), 6-i ,"Retry State");
				   } else {
					   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
					   Assert.assertEquals(relayPuller.getRetryonFallOff().getRemainingRetriesNum(), 5 ,"Retry State"); //reset
				   }
				   relayPuller.getMessageQueue().clear();
			   }
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "RelayFellOff State");
		   }

		   //Stream_Request_Success : ScnNotFoundException and  bootstrap enabled
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
		   }

		   //Stream_Request_Success : ScnNotFoundException but retries set to 5, bootstrap disabled and readLatestScnOnFallOff enabled.
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, false, true, false, true, ScnNotFoundException.class.getName(),5);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   for (int i=1; i<=6;i++)
			   {
				   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   if ( i < 6 )
				   {
					   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.PICK_SERVER);
					   Assert.assertEquals(relayPuller.getRetryonFallOff().getRemainingRetriesNum(), 6-i ,"Retry State");
				   } else {
					   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_RESPONSE_DONE);
					   Assert.assertEquals(relayPuller.getRetryonFallOff().getRemainingRetriesNum(), 5 ,"Retry State"); //reset
				   }
				   relayPuller.getMessageQueue().clear();
			   }
			   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
			   Assert.assertEquals(conn.isReadFromLatestScn(),true, "ReadFromLatestScn set");
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "RelayFellOff State");

		   }

		   //Stream_Request_Success : ScnNotFoundException, bootstrap disabled and readLatestScnOnFallOff enabled.
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, false, true, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_RESPONSE_DONE);
			   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
			   Assert.assertEquals(conn.isReadFromLatestScn(),true, "ReadFromLatestScn set");
		   }

		   //Stream_Request_Success : ScnNotFoundException but retries set to 5, bootstrap disabled and readLatestScnOnFallOff disabled.
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, false, false, false, true, ScnNotFoundException.class.getName(),5);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   for (int i = 1; i <= 6; i++)
			   {
				   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
				   relayPuller.getMessageQueue().clear();
				   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
				   relayPuller.getMessageQueue().clear();
				   if ( i < 6 )
				   {
					   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.PICK_SERVER);
					   Assert.assertEquals(relayPuller.getRetryonFallOff().getRemainingRetriesNum(), 6-i ,"Retry State");
				   } else {
					   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_REQUEST_SUCCESS, "SUSPEND_ON_ERROR");
					   Assert.assertEquals(relayPuller.getRetryonFallOff().getRemainingRetriesNum(), 5 ,"Retry State"); //reset
				   }
				   relayPuller.getMessageQueue().clear();
			   }
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "RelayFellOff State");

		   }


		   //Stream_Request_Success : ScnNotFoundException, bootstrap disabled and readLatestScnOnFallOff disabled.
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, false, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_REQUEST_SUCCESS, "SUSPEND_ON_ERROR");
		   }

		   //Stream_Request_Success : Non-ScnNotFoundException
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, "DummyError");
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_RESPONSE_ERROR);
		   }

		   //Stream_Request_Done to Request_Stream
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_RESPONSE_DONE);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_RESPONSE_DONE,StateId.REQUEST_STREAM);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Is Relay FellOff");
		   }

		   // Bootstrap to Bootstrap_Requested
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP_REQUESTED,"");
		   }

		   //Exception while doBootstrap()
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, true, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Is Relay FellOff");
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP, "SUSPEND_ON_ERROR");
		   }

		   // Bootstrap Failed : Case 1
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP_REQUESTED,"");
			   BootstrapResultMessage msg = BootstrapResultMessage.createBootstrapFailedMessage(new Exception("Dummy"));
			   doExecuteAndChangeState(relayPuller,msg);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP, "BOOTSTRAP_REQUESTED to BOOTSTRAP");
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [BOOTSTRAP]", "Queue :BOOTSTRAP_REQUESTED to BOOTSTRAP");
		   }

		   // Bootstrap Failed : Case 2
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP_REQUESTED,"");
			   BootstrapResultMessage msg = BootstrapResultMessage.createBootstrapCompleteMessage(null);
			   doExecuteAndChangeState(relayPuller,msg);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP, "BOOTSTRAP_REQUESTED to BOOTSTRAP");
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [BOOTSTRAP]", "Queue :BOOTSTRAP_REQUESTED to BOOTSTRAP");
		   }

		   // Bootstrap Success
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Is Relay FellOff");
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP_REQUESTED,"");
			   BootstrapResultMessage msg = BootstrapResultMessage.createBootstrapCompleteMessage(new Checkpoint());
			   doExecuteAndChangeState(relayPuller,msg);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Is Relay FellOff");
			   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_STREAM, "BOOTSTRAP_REQUESTED to REQUEST_STREAM");
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [REQUEST_STREAM]", "Queue :BOOTSTRAP_REQUESTED to REQUEST_STREAM");
		   }

		   // Error States to Pick_Server
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   connState.switchToSourcesRequestError();
			   testTransitionCase(relayPuller, StateId.SOURCES_REQUEST_ERROR, StateId.PICK_SERVER);
			   relayPuller.getMessageQueue().clear();
			   connState.switchToSourcesResponseError();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_ERROR, StateId.PICK_SERVER);
			   relayPuller.getMessageQueue().clear();
			   connState.switchToRegisterRequestError();
			   testTransitionCase(relayPuller, StateId.REGISTER_REQUEST_ERROR, StateId.PICK_SERVER);
			   relayPuller.getMessageQueue().clear();
			   connState.switchToRegisterResponseError();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_ERROR, StateId.PICK_SERVER);
			   relayPuller.getMessageQueue().clear();
			   connState.switchToStreamRequestError();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_ERROR, StateId.PICK_SERVER);
			   relayPuller.getMessageQueue().clear();
			   connState.switchToStreamResponseError();
			   testTransitionCase(relayPuller, StateId.STREAM_RESPONSE_ERROR, StateId.PICK_SERVER);
		   }
	   }
    }

    @Test
    public void testServerSetChanges()
    	throws Exception
    {
	   // Server-Set Change TestCases
	   {
		   Set<ServerInfo> expServerInfo = new TreeSet<ServerInfo>();
		   expServerInfo.add(new ServerInfo("relay1", "ONLINE", new InetSocketAddress("localhost",10001),"source1"));
		   expServerInfo.add(new ServerInfo("relay2","ONLINE",  new InetSocketAddress("localhost",10002),"source1"));
		   expServerInfo.add(new ServerInfo("relay3","ONLINE", new InetSocketAddress("localhost",10003),"source1"));

		   Set<ServerInfo> expServerInfo2 = new TreeSet<ServerInfo>(expServerInfo);
		   expServerInfo2.add(new ServerInfo("relay4","ONLINE", new InetSocketAddress("localhost",10000),"source1"));

		   Set<ServerInfo> expServerInfo3 = new TreeSet<ServerInfo>();
		   expServerInfo3.add(new ServerInfo("relay4","ONLINE", new InetSocketAddress("localhost",10000),"source1"));


		   // when on PICK_SERVER
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false);

			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   relayPuller.enqueueMessage(connState);

			   // ServerSetChange
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), -1, "Current Server Index");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), -1, "Current Server Index");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while Pick_Server");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while Pick_Server");
			   }

		   }

		   // When on Request_Sources
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, false);

			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_SOURCES, "ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [REQUEST_SOURCES]", "Queue :ServerSetChange while REQUEST_SOURCES");
			   }

			   // ServerSetChange when New Set excludes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on SOURCES-REQUEST-SENT and Response Successful
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.SOURCES_REQUEST_SENT, "ServerSetChange while SOURCES_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while SOURCES_REQUEST_SENT");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.SOURCES_REQUEST_SENT, "ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff Check");

				   // Now Response arrives
				   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
				   sourcesResponse.add(new IdNamePair(1L, "source1"));
				   connState.switchToSourcesSuccess(sourcesResponse);
				   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on SOURCES-REQUEST-SENT and SOURCES_RESPONSE_ERROR
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.SOURCES_REQUEST_SENT, "ServerSetChange while SOURCES_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while SOURCES_REQUEST_SENT");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SOURCES_RESPONSE_ERROR
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.SOURCES_REQUEST_SENT, "ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
				   sourcesResponse.add(new IdNamePair(1L, "source1"));
				   connState.switchToSourcesResponseError();
				   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_ERROR, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on SOURCES-REQUEST-SENT and SOURCES_REQUEST_ERROR
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.SOURCES_REQUEST_SENT, "ServerSetChange while SOURCES_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while SOURCES_REQUEST_SENT");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SOURCES_REQUEST_ERROR
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.SOURCES_REQUEST_SENT, "ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_SOURCES");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
				   sourcesResponse.add(new IdNamePair(1L, "source1"));
				   connState.switchToSourcesRequestError();
				   testTransitionCase(relayPuller, StateId.SOURCES_REQUEST_ERROR, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");

			   }
		   }

		   // When on Request Register
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_REGISTER, "ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [REQUEST_REGISTER]", "Queue :ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on REGISTER-REQUEST-SENT and REGISTER_RESPONSE_ERROR
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");


			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REGISTER_REQUEST_SENT, "ServerSetChange while REGISTER_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REGISTER_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REGISTER_REQUEST_SENT, "ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   connState.switchToRegisterResponseError();
				   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_ERROR, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");

			   }
		   }

		   // When on REGISTER-REQUEST-SENT and REGISTER_REQUEST_ERROR
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");


			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REGISTER_REQUEST_SENT, "ServerSetChange while REGISTER_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REGISTER_REQUEST_SENT");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REGISTER_REQUEST_SENT, "ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   connState.switchToSourcesRequestError();
				   testTransitionCase(relayPuller, StateId.REGISTER_REQUEST_ERROR, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");

			   }
		   }

		   // When on REGISTER-REQUEST-SENT and Response Successful
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");


			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REGISTER_REQUEST_SENT, "ServerSetChange while REGISTER_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REGISTER_REQUEST_SENT");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REGISTER_REQUEST_SENT, "ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_REGISTER");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
				   connState.switchToRegisterSuccess(conn.getRegisterResponse());
				   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");

			   }
		   }

		   // when on REQUEST_STREAM
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_STREAM, "ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [REQUEST_STREAM]", "Queue :ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on STREAM_REQUEST_SENT and response successful
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");
			   relayPuller.getMessageQueue().clear();
			   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
			   connState.switchToRegisterSuccess(conn.getRegisterResponse());
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "");


			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while STREAM_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while STREAM_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   conn = (MockRelayConnection)connState.getRelayConnection();
				   connState.switchToStreamSuccess(conn.getStreamResponse());
				   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on STREAM_REQUEST_SENT and STREAM_RESPONSE_ERROR
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");
			   relayPuller.getMessageQueue().clear();
			   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
			   connState.switchToRegisterSuccess(conn.getRegisterResponse());
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "");


			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while STREAM_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while STREAM_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and StreamResponse Error
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   connState.switchToStreamResponseError();
				   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When on STREAM_REQUEST_SENT and STREAM_REQUEST_ERROR
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, true);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_REQUEST_SENT, "");
			   List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
			   sourcesResponse.add(new IdNamePair(1L, "source1"));
			   connState.switchToSourcesSuccess(sourcesResponse);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   String subsListString = "[{\"physicalSource\":{\"uri\":\"databus:physical-source:ANY\",\"role\":\"MASTER\"},\"physicalPartition\":{\"id\":-1,\"name\":\"*\"},\"logicalPartition\":{\"source\":{\"id\":0,\"name\":\"source1\"},\"id\":-1}}]";
			   String sourcesIdListString = "1";
			   connState.switchToRequestSourcesSchemas(sourcesIdListString, subsListString);
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_REQUEST_SENT,"");
			   relayPuller.getMessageQueue().clear();
			   MockRelayConnection conn = (MockRelayConnection)connState.getRelayConnection();
			   connState.switchToRegisterSuccess(conn.getRegisterResponse());
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "");


			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while STREAM_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while STREAM_REQUEST_SENT");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and StreamRequest Error
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   connState.switchToStreamRequestError();
				   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER);
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");

			   }
		   }

		   // when Stream_Response_done
		   {
			   RelayPullThread relayPuller = createFellOffRelayPullThread(false, false);
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.STREAM_RESPONSE_DONE);
			   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   relayPuller.getConnectionState().setRelayFellOff(true);

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.STREAM_RESPONSE_DONE, "ServerSetChange while STREAM_RESPONSE_DONE");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [STREAM_RESPONSE_DONE]", "Queue :ServerSetChange while STREAM_RESPONSE_DONE");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while STREAM_RESPONSE_DONE");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while STREAM_RESPONSE_DONE");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When doBootstrap
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP, "ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [BOOTSTRAP]", "Queue :ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When doBootstrapRequested and bootstrap Successful
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP_REQUESTED,"");

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP_REQUESTED, "ServerSetChange while BOOTSTRAP_REQUESTED");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while BOOTSTRAP_REQUESTED");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP_REQUESTED, "ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   BootstrapResultMessage msg = BootstrapResultMessage.createBootstrapCompleteMessage(new Checkpoint());
				   doExecuteAndChangeState(relayPuller,msg);
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "BOOTSTRAP_REQUESTED to PICK_SERVER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :BOOTSTRAP_REQUESTED to REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // When doBootstrapRequested and bootstrap failed
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();
			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.SOURCES_RESPONSE_SUCCESS, StateId.REQUEST_REGISTER);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_REGISTER, StateId.REGISTER_RESPONSE_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REGISTER_RESPONSE_SUCCESS, StateId.REQUEST_STREAM);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.STREAM_REQUEST_SUCCESS,StateId.BOOTSTRAP);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.BOOTSTRAP,StateId.BOOTSTRAP_REQUESTED,"");

			   // ServerSetChange when New Set includes CurrentServer
			   {
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(true, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP_REQUESTED, "ServerSetChange while BOOTSTRAP_REQUESTED");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while BOOTSTRAP_REQUESTED");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");
			   }

			   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
			   {
				   int oldServerIndex = relayPuller.getCurrentServerIdx();
				   ServerInfo oldServer = relayPuller.getCurentServer();
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo2,"Server Set");
				   doExecuteAndChangeState(relayPuller,createSetServerMessage(false, relayPuller));
				   Assert.assertEquals(relayPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
				   Assert.assertEquals(relayPuller.getCurentServer(), oldServer, "Current Server unchanged");
				   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
				   Assert.assertEquals(connState.getStateId(),StateId.BOOTSTRAP_REQUESTED, "ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while BOOTSTRAP");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), true, "Relay FellOff CHeck");

				   // Now Response arrives
				   BootstrapResultMessage msg = BootstrapResultMessage.createBootstrapFailedMessage(new RuntimeException("dummy"));
				   doExecuteAndChangeState(relayPuller,msg);
				   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "BOOTSTRAP_REQUESTED to PICK_SERVER");
				   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :BOOTSTRAP_REQUESTED to REQUEST_STREAM");
				   Assert.assertEquals(relayPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
				   Assert.assertEquals(relayPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
				   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server Null");
				   Assert.assertEquals(relayPuller.getConnectionState().isRelayFellOff(), false, "Relay FellOff CHeck");
			   }
		   }

		   // Server-Set-Change message when suspended_due_to_error
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();

			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   ServerSetChangeMessage msg = createSetServerMessage(false, relayPuller);
			   relayPuller.enqueueMessage(msg);
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [SOURCES_RESPONSE_SUCCESS, SET_SERVERS]", "Queue :ServerSetChange");
			   doExecuteAndChangeState(relayPuller,LifecycleMessage.createSuspendOnErroMessage(new RuntimeException("null")));
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [SET_SERVERS]", "Queue :ServerSetChange");
			   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
			   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
			   Assert.assertEquals(relayPuller.getComponentStatus().getStatus(),Status.SUSPENDED_ON_ERROR,"Suspended state check");

			   connState.switchToRequestSourcesSchemas(null, null);
			   relayPuller.getMessageQueue().clear();
			   doExecuteAndChangeState(relayPuller,msg);
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [RESUME]", "Queue :ServerSetChange");
			   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server null");
			   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");

			   // on doResume verify if PICK_SERVER happens
			   doExecuteAndChangeState(relayPuller,relayPuller.getMessageQueue().poll());
			   Assert.assertEquals(relayPuller.getComponentStatus().getStatus(),Status.RUNNING," state check");
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange");
		   }

		   // Server-Set-Change message when PAUSED
		   {
			   RelayPullThread relayPuller = createRelayPullThread(false, false, true, false, false, true, ScnNotFoundException.class.getName());
			   relayPuller.getComponentStatus().start();

			   ConnectionState connState = relayPuller.getConnectionState();
			   connState.switchToPickServer();
			   testTransitionCase(relayPuller, StateId.PICK_SERVER, StateId.REQUEST_SOURCES);
			   relayPuller.getMessageQueue().clear();
			   testTransitionCase(relayPuller, StateId.REQUEST_SOURCES, StateId.SOURCES_RESPONSE_SUCCESS);
			   ServerSetChangeMessage msg = createSetServerMessage(false, relayPuller);
			   relayPuller.enqueueMessage(msg);
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [SOURCES_RESPONSE_SUCCESS, SET_SERVERS]", "Queue :ServerSetChange");
			   doExecuteAndChangeState(relayPuller,LifecycleMessage.createPauseMessage());
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [SET_SERVERS]", "Queue :ServerSetChange");
			   Assert.assertEquals(relayPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(relayPuller.getCurentServer() != null, true, "Current Server not Null");
			   Assert.assertEquals(relayPuller.getServers(),expServerInfo,"Server Set");
			   Assert.assertEquals(relayPuller.getComponentStatus().getStatus(),Status.PAUSED,"PAUSED state check");

			   connState.switchToRequestSourcesSchemas(null, null);
			   relayPuller.getMessageQueue().clear();
			   doExecuteAndChangeState(relayPuller,msg);
			   Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange");
			   Assert.assertEquals(relayPuller.getCurentServer() == null, true, "Current Server null");
			   Assert.assertEquals(relayPuller.getServers(),expServerInfo3,"Server Set");
		   }
	   }
  }


  private ServerSetChangeMessage createSetServerMessage(boolean keepCurrent, BasePullThread puller)
  {
    Set<ServerInfo> serverInfoSet = new HashSet<ServerInfo>();
	InetSocketAddress inetAddr1 = new InetSocketAddress("localhost",10000);

	ServerInfo s = new ServerInfo("newRelay1", "ONLINE", inetAddr1, Arrays.asList("source1"));
	serverInfoSet.add(s);

	if ( keepCurrent )
	{
	  serverInfoSet.addAll(puller.getServers());
	}

	ServerSetChangeMessage msg = ServerSetChangeMessage.createSetServersMessage(serverInfoSet);

	return msg;
  }

  private void testTransitionCase(RelayPullThread relayPuller, StateId currState, StateId finalState)
  {
	  testTransitionCase(relayPuller, currState, finalState, finalState.toString());
  }


  private void testTransitionCase(RelayPullThread relayPuller, StateId currState, StateId finalState, String msgQueueState)
  {
    ConnectionState connState = relayPuller.getConnectionState();
	doExecuteAndChangeState(relayPuller,connState);
	Assert.assertEquals(connState.getStateId(),finalState, "" + currState + " to " + finalState );
	Assert.assertEquals(relayPuller.getQueueListString(), "RelayPuller queue: [" + msgQueueState + "]", "Queue :" + currState + " to " + finalState);
  }

  @Test(groups={"small", "functional"})
  public void testRelayFailOver() throws Exception
  {
    List<String> sources = Arrays.asList("source1");

    Properties clientProps = new Properties();
    clientProps.setProperty("client.runtime.bootstrap.enabled", "false");
    clientProps.setProperty("client.runtime.relay(1).name", "relay1");
    clientProps.setProperty("client.runtime.relay(1).port", "10001");
    clientProps.setProperty("client.runtime.relay(1).sources", "source1");
    clientProps.setProperty("client.runtime.relay(2).name", "relay2");
    clientProps.setProperty("client.runtime.relay(2).port", "10002");
    clientProps.setProperty("client.runtime.relay(2).sources", "source1");
    clientProps.setProperty("client.runtime.relay(3).name", "relay3");
    clientProps.setProperty("client.runtime.relay(3).port", "10003");
    clientProps.setProperty("client.runtime.relay(3).sources", "source1");
    clientProps.setProperty("client.connectionDefaults.eventBuffer.maxSize", "100000");
    clientProps.setProperty("client.connectionDefaults.pullerRetries.maxRetryNum", "9");
    clientProps.setProperty("client.connectionDefaults.pullerRetries.sleepIncFactor", "1.0");
    clientProps.setProperty("client.connectionDefaults.pullerRetries.sleepIncDelta", "1");
    clientProps.setProperty("client.connectionDefaults.pullerRetries.initSleep", "1");

    DatabusHttpClientImpl.Config clientConfBuilder = new DatabusHttpClientImpl.Config();
    ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("client.", clientConfBuilder);
    configLoader.loadConfig(clientProps);

    DatabusHttpClientImpl.StaticConfig clientConf = clientConfBuilder.build();
    DatabusSourcesConnection.StaticConfig srcConnConf = clientConf.getConnectionDefaults();

    DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConf);
    Assert.assertNotNull(client, "client instantiation ok");

    DatabusHttpClientImpl.RuntimeConfig clientRtConf = clientConf.getRuntime().build();
    DbusEventBuffer.StaticConfig bufferConf = clientConf.getConnectionDefaults().getEventBuffer();

    DbusEventBuffer relayBuffer = new DbusEventBuffer(bufferConf);
    DbusEventBuffer bootstrapBuffer = new DbusEventBuffer(bufferConf);

    //we keep the index of the next server we expect to see
    AtomicInteger serverIdx = new AtomicInteger(-1);

    Set<ServerInfo> relays = clientRtConf.getRelaysSet();

    //generate the order in which we should see the servers
    List<ServerInfo> relayOrder = new ArrayList<ServerInfo>(relays);
    if (LOG.isInfoEnabled())
    {
      StringBuilder sb = new StringBuilder();
      for (ServerInfo serverInfo: relayOrder)
      {
        sb.append(serverInfo.getName());
        sb.append(" ");
      }
      LOG.info("Relay order:" + sb.toString());
    }

    //This guy always fails on /sources
    MockRelayConnection mockFailConn = new MockRelayConnection(null, null, null, serverIdx);

    List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
    sourcesResponse.add(new IdNamePair(1L, "source1"));

    //This guy succeeds on /sources but fails on /register
    MockRelayConnection mockSuccessConn = new MockRelayConnection(sourcesResponse, null, null,
                                                                  serverIdx);

    DatabusRelayConnectionFactory mockConnFactory =
        EasyMock.createMock("mockRelayFactory", DatabusRelayConnectionFactory.class);

    //each server should be tried MAX_RETRIES time until all retries are exhausted
    for (int i = 0; i < clientConf.getConnectionDefaults().getPullerRetries().getMaxRetryNum() / 3; ++i)
    {
      org.easymock.EasyMock.expect(mockConnFactory.createRelayConnection(
          serverNameMatcher(serverIdx, relayOrder),
          org.easymock.EasyMock.<ActorMessageQueue>notNull(),
          org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockFailConn);
      org.easymock.EasyMock.expect(mockConnFactory.createRelayConnection(
          serverNameMatcher(serverIdx, relayOrder),
          org.easymock.EasyMock.<ActorMessageQueue>notNull(),
          org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockFailConn);
      org.easymock.EasyMock.expect(mockConnFactory.createRelayConnection(
          serverNameMatcher(serverIdx, relayOrder),
          org.easymock.EasyMock.<ActorMessageQueue>notNull(),
          org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockSuccessConn);
    }

    EasyMock.replay(mockConnFactory);

    List<DatabusSubscription> sourcesSubList = DatabusSubscription.createSubscriptionList(sources);
    //Dummy connection object as expected by the puller thread
    // Note that in this case, it is ok to pass Set<relays> as all the relays serve the same source "source1"
    DatabusSourcesConnection sourcesConn = new DatabusSourcesConnection(
        srcConnConf, sourcesSubList, relays, null, null, null, relayBuffer, bootstrapBuffer,
        Executors.newCachedThreadPool(), null, null, null, null, null, null, mockConnFactory, null, null,null, null);

    RelayPullThread relayPuller = new RelayPullThread("RelayPuller", sourcesConn, relayBuffer, relays,
                                                      new ArrayList<DbusKeyCompositeFilterConfig>(),
                                                      !clientConf.getRuntime().getBootstrap().isEnabled(),
                                                      clientConf.isReadLatestScnOnErrorEnabled(),
                                                      clientConf.getPullerBufferUtilizationPct(),
                                                      ManagementFactory.getPlatformMBeanServer());
    RemoteExceptionHandler mockRemoteExceptionHandler = new MockRemoteExceptionHandler(sourcesConn, relayBuffer, relayPuller);

    Field field = relayPuller.getClass().getDeclaredField("_remoteExceptionHandler");
    field.setAccessible(true);
    field.set(relayPuller, mockRemoteExceptionHandler);

    mockFailConn.setCallback(relayPuller);
    mockSuccessConn.setCallback(relayPuller);

    //Let the show begin
    Thread relayPullerThread = new Thread(relayPuller);
    relayPullerThread.setDaemon(true);
    relayPullerThread.start();

    relayPuller.enqueueMessage(LifecycleMessage.createStartMessage());

    try
    {
      Thread.sleep(500);
    }
    catch (InterruptedException ie){}

    Assert.assertEquals(mockFailConn.getSourcesCallCounter(),
                        6,
                        "failConn: correct number of /sources");
    Assert.assertEquals(mockFailConn.getRegisterCallCounter(),
                        0,
                        "failConn: correct number of /register");
    Assert.assertEquals(mockFailConn.getStreamCallCounter(),
                        0,
                        "failConn: correct number of /stream");

    Assert.assertEquals(mockSuccessConn.getSourcesCallCounter(),
                        3,
                        "successConn: correct number of /sources");
    Assert.assertEquals(mockSuccessConn.getRegisterCallCounter(),
                        3,
                        "successConn: correct number of /register");
    Assert.assertEquals(mockSuccessConn.getStreamCallCounter(),
                        0,
                        "successConn: correct number of /stream");
    Assert.assertEquals(relayPuller.getComponentStatus().getStatus(),
                        DatabusComponentStatus.Status.SUSPENDED_ON_ERROR,
                        "puller suspended because of out of retries");

    EasyMock.verify(mockConnFactory);

    relayPuller.enqueueMessage(LifecycleMessage.createShutdownMessage());
    relayPuller.awaitShutdown();
  }

  public static ServerInfo serverNameMatcher(AtomicInteger serverIdx, List<ServerInfo> serverOrder)
  {
    org.easymock.EasyMock.reportMatcher(new ServerNameMatcher(serverIdx, serverOrder));
    return null;
  }

  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
  }

  @BeforeClass
  public void beforeClass() {
  }

  @AfterClass
  public void afterClass() {
  }

  @BeforeTest
  public void beforeTest() {
  }

  @AfterTest
  public void afterTest() {
  }

  @BeforeSuite
  public void beforeSuite() {
  }

  @AfterSuite
  public void afterSuite() {
  }

  private void doExecuteAndChangeState(RelayPullThread thread, Object msg)
  {
	  Object obj = msg;

	  if ( msg instanceof ConnectionState)
	  {
		  ConnectionState o = (ConnectionState)msg;
		  obj = new ConnectionStateMessage(o.getStateId(), o);
	  }

	  thread.doExecuteAndChangeState(obj);
  }

}

class ServerNameMatcher implements IArgumentMatcher
{
  private final AtomicInteger _sharedServerIdx;
  private final List<ServerInfo> _serverOrder;

  public ServerNameMatcher(AtomicInteger serverIdx, List<ServerInfo> serverOrder)
  {
    _sharedServerIdx = serverIdx;
    _serverOrder = serverOrder;
    Collections.sort(_serverOrder);
  }

  @Override
  public void appendTo(StringBuffer sbuf)
  {
    int idx = _sharedServerIdx.get();
    String serverName = idx < 0 ? "null" : _serverOrder.get(idx % _serverOrder.size()).getName();

    sbuf.append("serverNameMatch(");
    sbuf.append(idx % _serverOrder.size());
    sbuf.append("-->");
    sbuf.append(serverName);
    sbuf.append(")");
  }

  @Override
  public boolean matches(Object o)
  {
    boolean success = true;

    ServerInfo serverInfo = null;
    if (success) success = o instanceof ServerInfo;
    if (success) serverInfo = (ServerInfo)o;

    int serverIdx = -1;
    if (success)
    {
      serverIdx = _serverOrder.indexOf(serverInfo);
      success = serverIdx >= 0;
    }

    int expectedIndex = _sharedServerIdx.get() % _serverOrder.size();
    if (success) success = -1 == expectedIndex || serverIdx == expectedIndex;
    if (success && (-1 == expectedIndex)) _sharedServerIdx.set(serverIdx);

    return success;
  }
}

class MockRelayConnection implements DatabusRelayConnection
{

  private final List<IdNamePair> _sourceIds;
  private final Map<Long, List<RegisterResponseEntry>> _registerResponse;
  private final ChunkedBodyReadableByteChannel _streamResponse;
  private final AtomicInteger _sharedServerIdx;

  private AbstractActorMessageQueue _callback = null;
  private int _sourcesCallCounter = 0;
  private int _registerCallCounter = 0;
  private int _streamCallCounter = 0;
  private boolean _muteTransition = false;

  private boolean enableReadLatest = false;

  public MockRelayConnection(List<IdNamePair> sourceIds,
                             Map<Long, List<RegisterResponseEntry>> registerResponse,
                             ChunkedBodyReadableByteChannel streamResponse,
                             AtomicInteger sharedServerIdx)
  {
	  this(sourceIds, registerResponse, streamResponse, sharedServerIdx, false);
  }

  public MockRelayConnection(List<IdNamePair> sourceIds,
          Map<Long, List<RegisterResponseEntry>> registerResponse,
          ChunkedBodyReadableByteChannel streamResponse,
          AtomicInteger sharedServerIdx,
          boolean muteTransition)
  {
    super();
    _sourceIds = sourceIds;
    _registerResponse = registerResponse;
    _streamResponse = streamResponse;
    _sharedServerIdx = sharedServerIdx;
    _muteTransition = muteTransition;
  }

  @Override
  public void close()
  {

  }

  @Override
  public void requestSources(DatabusRelayConnectionStateMessage stateReuse)
  {
    ++ _sourcesCallCounter;
    if (null == _sourceIds)
    {
      if ( !_muteTransition) stateReuse.switchToSourcesRequestError();
      _sharedServerIdx.incrementAndGet();
    }
    else
    {
    	if ( !_muteTransition) stateReuse.switchToSourcesSuccess(_sourceIds);
    }

    if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
  }

  @Override
  public void requestRegister(String sourcesIdList,
                              DatabusRelayConnectionStateMessage stateReuse)
  {
    ++ _registerCallCounter;

    if (null == _registerResponse)
    {
    	if ( !_muteTransition) stateReuse.switchToRegisterRequestError();
      _sharedServerIdx.incrementAndGet();
    }
    else
    {
    	if ( !_muteTransition) stateReuse.switchToRegisterSuccess(_registerResponse);
    }

    if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
  }

  @Override
  public void requestStream(String sourcesIdList,
		  				DbusKeyCompositeFilter filter,
                            int freeBufferSpace,
                            CheckpointMult cp,
                            Range keyRange,
                            DatabusRelayConnectionStateMessage stateReuse)
  {
    ++ _streamCallCounter;
    if (null == _streamResponse)
    {
    	if ( !_muteTransition) stateReuse.switchToStreamRequestError();
      _sharedServerIdx.incrementAndGet();
    }
    else
    {
    	if ( !_muteTransition) stateReuse.switchToStreamSuccess(_streamResponse);
    }

    if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
  }

  public int getSourcesCallCounter()
  {
    return _sourcesCallCounter;
  }

  public void setSourcesCallCounter(int sourcesCallCounter)
  {
    _sourcesCallCounter = sourcesCallCounter;
  }

  public int getRegisterCallCounter()
  {
    return _registerCallCounter;
  }

  public void setRegisterCallCounter(int registerCallCounter)
  {
    _registerCallCounter = registerCallCounter;
  }

  public int getStreamCallCounter()
  {
    return _streamCallCounter;
  }

  public void setStreamCallCounter(int streamCallCounter)
  {
    _streamCallCounter = streamCallCounter;
  }

  public AbstractActorMessageQueue getCallback()
  {
    return _callback;
  }

  public void setCallback(AbstractActorMessageQueue callback)
  {
    _callback = callback;
  }

  @Override
  public void enableReadFromLatestScn(boolean enable) {  enableReadLatest = enable;}


  public boolean isReadFromLatestScn()
  {
	  return enableReadLatest;
  }


  @Override
  public int getVersion()  {
    return 0;
  }

  public Map<Long, List<RegisterResponseEntry>> getRegisterResponse() {
	return _registerResponse;
  }

  public ChunkedBodyReadableByteChannel getStreamResponse() {
	return _streamResponse;
  }

}

class MockRemoteExceptionHandler extends RemoteExceptionHandler
{
	RelayPullThread _rp;
	public MockRemoteExceptionHandler(DatabusSourcesConnection sourcesConn,
			DbusEventBuffer dataEventsBuffer, RelayPullThread rp)
	{
		super(sourcesConn, dataEventsBuffer);
		_rp = rp;
	}

	@Override
	public void handleException(Throwable remoteException)
			throws InvalidEventException, InterruptedException
			{
		_rp.enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new Exception("")));

		return;
			}

}
