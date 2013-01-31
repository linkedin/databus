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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ConnectionState.StateId;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.test.TestUtil;

public class TestBootstrapPullThread
{
  public static final Logger LOG = Logger.getLogger("TestBootstrapPullThread");

  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"LiarJobRelay\",\"namespace\":\"com.linkedin.events.liar.jobrelay\",\"fields\":[{\"name\":\"txn\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"eventId\",\"type\":[\"int\",\"null\"],\"meta\":\"dbFieldName=EVENT_ID;dbFieldPosition=2;\"},{\"name\":\"isDelete\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=IS_DELETE;dbFieldPosition=3;\"},{\"name\":\"state\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=STATE;dbFieldPosition=4;\"}],\"meta\":\"dbFieldName=SY$LIAR_JOB_RELAY_1;\"}");

  public static int _port = -1;
  public static String _host = null;
  public static String _serverInfoName = null;
  public static ServerInfo _serverInfo = null;

  static
  {
    TestUtil.setupLogging(true, null, Level.OFF);
    try
    {
      _host = InetAddress.getLocalHost().getHostName();
      _port  = 7874;
      _serverInfoName = _host  + ":" + _port;
      _serverInfo = ServerInfo.buildServerInfoFromHostPort(_serverInfoName, ":");
    } catch (Exception ex) {
      throw new RuntimeException("Unable to generate local serverInfo !!");
    }
  }

  private void doExecuteAndChangeState(BootstrapPullThread thread, Object msg)
  {
	  Object obj = msg;

	  if ( msg instanceof ConnectionState)
	  {
		  ConnectionState o = (ConnectionState)msg;
		  obj = new ConnectionStateMessage(o.getStateId(), o);
	  }

	  thread.doExecuteAndChangeState(obj);
  }

  @Test
  public void testBootstrapTransition()
  	throws Exception
  {
	    String dummyHost = "NonExistantHost";

    	String dummyServerInfoName = dummyHost + ":" + _port;
    	String malformedServerInfoName = dummyHost + _port; // no delim

        LOG.info("serverInfoName :" + _serverInfoName);

	   // Main Transition Test (Without Server-Set Change)
	   {
		   // BOOTSTRAP - Happy Path without startScn
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
		   }

		   // BOOTSTRAP - Happy Path with startScn
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   cp.setBootstrapSinceScn(50L);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.START_SCN_RESPONSE_SUCCESS, cp);
			   Assert.assertEquals(cp.getBootstrapStartScn().longValue(), 100L, "Cleared Bootstrap StartSCN");
			   Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
			   String actualHost = bsPuller.getCurentServer().getAddress().getHostName();
			   int actualPort = bsPuller.getCurentServer().getAddress().getPort();
			   Assert.assertEquals(actualHost, _host, "Current Server Host Check");
			   Assert.assertEquals(actualPort, _port, "Server Port Check");
			   int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
			   Assert.assertEquals(numRetries, 25, "NumRetries Check");
		   }


		   // BOOTSTRAP - Bootstrap Restart since no serverInfo
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapStartScn(1111L);
			   cp.setBootstrapSinceScn(900L);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   cp.setBootstrapSinceScn(50L);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
			   Assert.assertEquals(cp.getBootstrapStartScn().longValue(), Checkpoint.INVALID_BOOTSTRAP_START_SCN, "Cleared Bootstrap StartSCN");
			   Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
			   int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
			   Assert.assertEquals(numRetries, 25, "NumRetries Check");
		   }

		   // BOOTSTRAP - Bootstrap Restart since current errors in current serverInfo
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapStartScn(1111L);
			   cp.setBootstrapSinceScn(900L);
			   cp.setBootstrapServerInfo(dummyServerInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   cp.setBootstrapSinceScn(50L);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
			   Assert.assertEquals(cp.getBootstrapStartScn().longValue(), Checkpoint.INVALID_BOOTSTRAP_START_SCN, "Cleared Bootstrap StartSCN");
			   Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
			   int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
			   Assert.assertEquals(numRetries, 25, "NumRetries Check");
		   }

		   // BOOTSTRAP - Bootstrap Restart since malformed serverInfo
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapStartScn(1111L);
			   cp.setBootstrapSinceScn(900L);
			   cp.setBootstrapServerInfo(malformedServerInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   cp.setBootstrapSinceScn(50L);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
			   Assert.assertEquals(cp.getBootstrapStartScn().longValue(), Checkpoint.INVALID_BOOTSTRAP_START_SCN, "Cleared Bootstrap StartSCN");
			   Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
			   int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
			   Assert.assertEquals(numRetries, 25, "NumRetries Check");
		   }

		   // BOOTSTRAP  - Servers exhausted
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, true, false);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.BOOTSTRAP, "SUSPEND_ON_ERROR", cp);
		   }


		   // Bootstrap : Connection Factory returned null with resumeCkpt startScn not set
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(true, false, false);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.BOOTSTRAP, "SUSPEND_ON_ERROR", cp);
		   }


		   // Bootstrap : Connection Factory returned null with resumeCkpt startScn  set
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(true, false, false);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapStartScn(100L);
			   cp.setBootstrapSinceScn(50L);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.BOOTSTRAP, "SUSPEND_ON_ERROR", cp);
		   }


		   // Request_Start_Scn to Start_Scn_Sent
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, true);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_REQUEST_SENT, "", null);
		   }


		   // Request_StartSCN to StarSCN_Response_Success
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
		   }

		   //StartSCN_Response_Success : Happy path
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   }

		   //StartSCN_Response_Success : when ServerInfo does not match
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo("localhost" + ":" + 9999);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.PICK_SERVER, null);
		   }

		   //StartSCN_Response_Success : when no ServerInfo
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);
			   connState.setCurrentBSServerInfo(null);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.START_SCN_RESPONSE_ERROR, null);
		   }

		   //StartSCN_Response_Success : Error Case
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   entries.put(2L, new ArrayList<RegisterResponseEntry>());

			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.START_SCN_RESPONSE_ERROR, "SUSPEND_ON_ERROR", null);
		   }

		   // Request_Stream when not enough space in the buffer
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 10, 1); // available space is 10 which is less than the threshold of 10000
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.REQUEST_STREAM, null);
		   }

		   // Request_Stream to Stream_request_success
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);
		   }

		   // Request_Stream to Stream_request_sent
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
			   mockConn.setMuteTransition(true);

			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "", null);
		   }

		   // Stream_Response_Success - Happy Path
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

		   }

		   // Stream_Response_Success - readEvents returned 0 bytes
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 0);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER, null);
		   }

		   // Stream_Response_Success - readEvents threw Exception
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, true, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER, null);
		   }

		   // Stream_Response_Success - Server returned Bootstrap_Too_Old_Exception
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, true, BootstrapDatabaseTooOldException.class.getName(), 12000, 1, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_ERROR, null);
		   }

		   // Stream_Response_Success - Server returned other exception
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, true, "Dummy Exception", 12000, 1, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_ERROR, null);
		   }

		   // Stream_Response_Success - Happy Path: Phase Completed
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

		   }


		   // Stream_Response_Done - when phase not yet completed
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, false);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   cp.setCatchupSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setSnapshotOffset(10); // non -1 value
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_STREAM, null);
		   }

		   // Stream_Response_Done - when snapshot phase  completed
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setSnapshotOffset(-1);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
			   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
			   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");
		   }

		   // Stream_Response_Done - when catchup phase  completed and going to catchup next table
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   cp.setWindowOffset(-1);
			   cp.setBootstrapStartScn(100L);
			   cp.setBootstrapSnapshotSourceIndex(1);
			   cp.setBootstrapCatchupSourceIndex(-1);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_STREAM, null);
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
			   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
			   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");
		   }

		   // Stream_Response_Done - when catchup phase  completed and going to snapshot next table
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   cp.setSnapshotOffset(10000);
			   cp.setBootstrapStartScn(100L);
			   cp.setBootstrapSnapshotSourceIndex(0);
			   cp.setBootstrapCatchupSourceIndex(0);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_STREAM, null);
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
			   Assert.assertEquals(cp.getSnapshotSource(), "source1", "Catchup Source check");
			   Assert.assertEquals(cp.getSnapshotOffset(), new Long(0), "Offset Check");
		   }

		   // Stream_Response_Done - Bootstrap Done
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			   cp.setSnapshotOffset(10000);
			   cp.setBootstrapStartScn(100L);
			   cp.setBootstrapSnapshotSourceIndex(1);
			   cp.setBootstrapCatchupSourceIndex(1);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.BOOTSTRAP_DONE, "", null);
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.ONLINE_CONSUMPTION, "Consumption Mode check");
		   }

		   // Request_target_Scn to Target_Scn_Sent
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setSnapshotOffset(-1);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
			   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
			   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

			   bsPuller.getMessageQueue().clear();
			   MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
			   mockConn.setMuteTransition(true);
			   testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_REQUEST_SENT, "", null);
		   }

		   // Request_target_Scn to Target_Scn_Success
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setSnapshotOffset(-1);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
			   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
			   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);
		   }

		   // Target_Scn_Success : Happy Path
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setSnapshotOffset(-1);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
			   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
			   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   }

		   // Target_Scn_Success : Error
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
			   bsPuller.getMessageQueue().clear();
			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
			   bsPuller.getMessageQueue().clear();

			   cp.setSnapshotSource("source1");
			   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
			   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
			   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

			   bsPuller.getMessageQueue().clear();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setSnapshotOffset(-1);
			   cp.setBootstrapStartScn(100L);
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
			   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
			   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
			   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

			   bsPuller.getMessageQueue().clear();
			   testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

			   bsPuller.getMessageQueue().clear();
			   entries = new HashMap<Long, List<RegisterResponseEntry>>();

			   connState.setSourcesSchemas(entries);

			   testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.TARGET_SCN_RESPONSE_ERROR, "SUSPEND_ON_ERROR", null);
		   }

		   // Error States
		   {
			   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
			   Checkpoint cp = new Checkpoint();
			   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			   cp.setBootstrapServerInfo(_serverInfoName);

			   bsPuller.getComponentStatus().start();
			   ConnectionState connState = bsPuller.getConnectionState();
			   connState.switchToBootstrap(cp);
			   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
			   bsPuller.getMessageQueue().clear();
			   connState.switchToStartScnRequestError();
			   testTransitionCase(bsPuller, StateId.START_SCN_REQUEST_ERROR, StateId.PICK_SERVER, cp);
			   bsPuller.getMessageQueue().clear();
			   connState.switchToStartScnResponseError();
			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_ERROR, StateId.PICK_SERVER, cp);
			   bsPuller.getMessageQueue().clear();
			   connState.switchToTargetScnRequestError();
			   testTransitionCase(bsPuller, StateId.TARGET_SCN_REQUEST_ERROR, StateId.PICK_SERVER, cp);
			   bsPuller.getMessageQueue().clear();
			   connState.switchToTargetScnResponseError();
			   testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_ERROR, StateId.PICK_SERVER, cp);
			   bsPuller.getMessageQueue().clear();
			   connState.switchToStreamRequestError();
			   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_ERROR, StateId.PICK_SERVER, cp);
			   bsPuller.getMessageQueue().clear();
			   connState.switchToStreamResponseError();
			   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_ERROR, StateId.PICK_SERVER, cp);
		   }
	   }
  }

  @Test
  public void testServerSetChanges()
  	throws Exception
  {
	   Set<ServerInfo> expServerInfo = new TreeSet<ServerInfo>();
	   expServerInfo.add(new ServerInfo("bs1", "ONLINE", new InetSocketAddress("localhost",10001),"source1"));
	   expServerInfo.add(new ServerInfo("bs2","ONLINE",  new InetSocketAddress("localhost",10002),"source1"));
	   expServerInfo.add(new ServerInfo("bs3","ONLINE", new InetSocketAddress("localhost",10003),"source1"));

	   Set<ServerInfo> expServerInfo2 = new TreeSet<ServerInfo>(expServerInfo);
	   expServerInfo2.add(new ServerInfo("bs4","ONLINE", new InetSocketAddress("localhost",10000),"source1"));

	   Set<ServerInfo> expServerInfo3 = new TreeSet<ServerInfo>();
	   expServerInfo3.add(new ServerInfo("bs4","ONLINE", new InetSocketAddress("localhost",10000),"source1"));


	   // when on PICK_SERVER
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToPickServer();
		   bsPuller.enqueueMessage(connState);

		   // ServerSetChange
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), -1, "Current Server Index");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), -1, "Current Server Index");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while Pick_Server");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while Pick_Server");
		   }
	   }

	   // When on Request_start_Scn
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);

		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);


		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_START_SCN, "ServerSetChange while REQUEST_START_SCN");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [REQUEST_START_SCN]", "Queue :ServerSetChange while REQUEST_START_SCN");
		   }

		   // ServerSetChange when New Set excludes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_START_SCN");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_START_SCN");
		   }
	   }

	   // When on Start_Scn_Request_Sent
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, true);

		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_REQUEST_SENT, "", null);

		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");

			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.START_SCN_REQUEST_SENT, "ServerSetChange while START_SCN_REQUEST_SENT");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while START_SCN_REQUEST_SENT");
		   }

		   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
		   {
			   int oldServerIndex = bsPuller.getCurrentServerIdx();
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");

			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.START_SCN_REQUEST_SENT, "ServerSetChange while START_SCN_REQUEST_SENT");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while START_SCN_REQUEST_SENT");

			   // Now Response arrives
			   connState.switchToStartScnSuccess(cp, null, null);
			   testTransitionCase(bsPuller, StateId.START_SCN_REQUEST_SENT, StateId.PICK_SERVER, null);
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
		   }
	   }


	   // When on Start_Scn_Response_ Success
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);

		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);

		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");

			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.START_SCN_RESPONSE_SUCCESS, "ServerSetChange while START_SCN_RESPONSE_SUCCESS");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [START_SCN_RESPONSE_SUCCESS]", "Queue :ServerSetChange while START_SCN_RESPONSE_SUCCESS");
		   }

		   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
		   {
			   int oldServerIndex = bsPuller.getCurrentServerIdx();
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");

			   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.START_SCN_RESPONSE_SUCCESS, "ServerSetChange while START_SCN_RESPONSE_SUCCESS");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [START_SCN_RESPONSE_SUCCESS]", "Queue :ServerSetChange while START_SCN_RESPONSE_SUCCESS");

			   // Now Response arrives
			   connState.switchToStartScnSuccess(cp, null, null);
			   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.PICK_SERVER, null);
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
		   }
	   }

	   // When on Request_stream
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);

		   bsPuller.getMessageQueue().clear();
		   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

		   entries.put(1L, new ArrayList<RegisterResponseEntry>());
		   connState.setSourcesSchemas(entries);
		   connState.setCurrentBSServerInfo(bsPuller.getCurentServer());

		   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);


		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_STREAM, "ServerSetChange while REQUEST_STREAM");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [REQUEST_STREAM]", "Queue :ServerSetChange while REQUEST_STREAM");
		   }

		   // ServerSetChange when New Set excludes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_STREAM");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_STREAM");
		   }
	   }

	   // When on Stream_Request_Sent
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
		   bsPuller.getMessageQueue().clear();
		   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

		   entries.put(1L, new ArrayList<RegisterResponseEntry>());
		   connState.setSourcesSchemas(entries);
		   connState.setCurrentBSServerInfo(bsPuller.getCurentServer());

		   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   bsPuller.getMessageQueue().clear();

		   cp.setSnapshotSource("source1");
		   cp.setCatchupSource("source1");
		   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
		   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
		   MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
		   mockConn.setMuteTransition(true);

		   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "", null);

		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while STREAM_REQUEST_SENT");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while STREAM_REQUEST_SENT");
		   }

		   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
		   {
			   int oldServerIndex = bsPuller.getCurrentServerIdx();
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while STREAM_REQUEST_SENT");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while STREAM_REQUEST_SENT");

			   // Now Response arrives
			   connState.switchToStartScnSuccess(cp, null, null);
			   testTransitionCase(bsPuller, StateId.START_SCN_REQUEST_SENT, StateId.PICK_SERVER, null);
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
		   }
	   }

	   // When on Stream_Response_Success
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
		   bsPuller.getMessageQueue().clear();
		   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

		   entries.put(1L, new ArrayList<RegisterResponseEntry>());
		   connState.setSourcesSchemas(entries);
		   connState.setCurrentBSServerInfo(bsPuller.getCurentServer());

		   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   bsPuller.getMessageQueue().clear();

		   cp.setSnapshotSource("source1");
		   cp.setCatchupSource("source1");
		   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
		   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));

		   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SUCCESS, "ServerSetChange while STREAM_REQUEST_SUCCESS");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [STREAM_REQUEST_SUCCESS]", "Queue :ServerSetChange while STREAM_REQUEST_SUCCESS");
		   }

		   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
		   {
			   int oldServerIndex = bsPuller.getCurrentServerIdx();
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SUCCESS, "ServerSetChange while STREAM_REQUEST_SUCCESS");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [STREAM_REQUEST_SUCCESS]", "Queue :ServerSetChange while STREAM_REQUEST_SUCCESS");

			   // Now Response arrives
			   connState.switchToStartScnSuccess(cp, null, null);
			   testTransitionCase(bsPuller, StateId.START_SCN_REQUEST_SENT, StateId.PICK_SERVER, null);
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
		   }
	   }


	   // When on Request_target_Scn
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
		   bsPuller.getMessageQueue().clear();
		   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

		   entries.put(1L, new ArrayList<RegisterResponseEntry>());
		   connState.setSourcesSchemas(entries);
		   connState.setCurrentBSServerInfo(bsPuller.getCurentServer());

		   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   bsPuller.getMessageQueue().clear();

		   cp.setSnapshotSource("source1");
		   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
		   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
		   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

		   bsPuller.getMessageQueue().clear();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		   cp.setSnapshotOffset(-1);
		   cp.setBootstrapStartScn(100L);
		   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
		   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
		   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
		   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");


		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.REQUEST_TARGET_SCN, "ServerSetChange while REQUEST_TARGET_SCN");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [REQUEST_TARGET_SCN]", "Queue :ServerSetChange while REQUEST_TARGET_SCN");
		   }

		   // ServerSetChange when New Set excludes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_TARGET_SCN");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_TARGET_SCN");
		   }
	   }

	   // When on Target_Scn_Request_Sent
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
		   bsPuller.getMessageQueue().clear();
		   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

		   entries.put(1L, new ArrayList<RegisterResponseEntry>());
		   connState.setSourcesSchemas(entries);
		   connState.setCurrentBSServerInfo(bsPuller.getCurentServer());

		   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   bsPuller.getMessageQueue().clear();

		   cp.setSnapshotSource("source1");
		   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
		   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
		   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

		   bsPuller.getMessageQueue().clear();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		   cp.setSnapshotOffset(-1);
		   cp.setBootstrapStartScn(100L);
		   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
		   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
		   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
		   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

		   bsPuller.getMessageQueue().clear();
		   MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
		   mockConn.setMuteTransition(true);
		   testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_REQUEST_SENT, "", null);

		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_REQUEST_SENT, "ServerSetChange while TARGET_SCN_REQUEST_SENT");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while TARGET_SCN_REQUEST_SENT");
		   }

		   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
		   {
			   int oldServerIndex = bsPuller.getCurrentServerIdx();
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_REQUEST_SENT, "ServerSetChange while TARGET_SCN_REQUEST_SENT");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while TARGET_SCN_REQUEST_SENT");

			   // Now Response arrives
			   connState.switchToStartScnSuccess(cp, null, null);
			   testTransitionCase(bsPuller, StateId.TARGET_SCN_REQUEST_SENT, StateId.PICK_SERVER, null);
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
		   }
	   }

	   // When on Target_Scn_Response_ Success
	   {
		   BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

		   Checkpoint cp = new Checkpoint();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

		   bsPuller.getComponentStatus().start();
		   ConnectionState connState = bsPuller.getConnectionState();
		   connState.switchToBootstrap(cp);
		   testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
		   bsPuller.getMessageQueue().clear();
		   Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

		   entries.put(1L, new ArrayList<RegisterResponseEntry>());
		   connState.setSourcesSchemas(entries);
		   connState.setCurrentBSServerInfo(bsPuller.getCurentServer());

		   testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
		   bsPuller.getMessageQueue().clear();

		   cp.setSnapshotSource("source1");
		   connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
		   connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
		   testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

		   bsPuller.getMessageQueue().clear();
		   cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		   cp.setSnapshotOffset(-1);
		   cp.setBootstrapStartScn(100L);
		   testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
		   Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
		   Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
		   Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

		   bsPuller.getMessageQueue().clear();
		   testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

		   // ServerSetChange when New Set includes CurrentServer
		   {
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_RESPONSE_SUCCESS, "ServerSetChange while TARGET_SCN_RESPONSE_SUCCESS");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [TARGET_SCN_RESPONSE_SUCCESS]", "Queue :ServerSetChange while TARGET_SCN_RESPONSE_SUCCESS");
		   }

		   // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
		   {
			   int oldServerIndex = bsPuller.getCurrentServerIdx();
			   ServerInfo oldServer = bsPuller.getCurentServer();
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo2,"Server Set");

			   entries = new HashMap<Long, List<RegisterResponseEntry>>();
			   entries.put(1L, new ArrayList<RegisterResponseEntry>());
			   connState.setSourcesSchemas(entries);

			   doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
			   Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
			   Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
			   Assert.assertEquals(bsPuller.getServers(),expServerInfo3,"Server Set");
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
			   Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_RESPONSE_SUCCESS, "ServerSetChange while TARGET_SCN_RESPONSE_SUCCESS");
			   Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [TARGET_SCN_RESPONSE_SUCCESS]", "Queue :ServerSetChange while TARGET_SCN_RESPONSE_SUCCESS");

			   // Now Response arrives
			   connState.switchToStartScnSuccess(cp, null, null);
			   testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.PICK_SERVER, null);
			   Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
			   Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
			   Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
		   }
	   }
  }

  private ServerSetChangeMessage createSetServerMessage(boolean keepCurrent, BasePullThread puller)
  {
    Set<ServerInfo> serverInfoSet = new HashSet<ServerInfo>();
	InetSocketAddress inetAddr1 = new InetSocketAddress("localhost",10000);

	ServerInfo s = new ServerInfo("newBs1", "ONLINE", inetAddr1, Arrays.asList("source1"));
	serverInfoSet.add(s);

	if ( keepCurrent )
	{
	  serverInfoSet.addAll(puller.getServers());
	}

	ServerSetChangeMessage msg = ServerSetChangeMessage.createSetServersMessage(serverInfoSet);

	return msg;
  }


  private void testTransitionCase(BootstrapPullThread bsPuller, StateId currState, StateId finalState, Checkpoint cp)
  {
	  testTransitionCase(bsPuller, currState, finalState, finalState.toString(), cp);
  }


  private void testTransitionCase(BootstrapPullThread bsPuller, StateId currState, StateId finalState, String msgQueueState, Checkpoint cp)
  {
    ConnectionState connState = bsPuller.getConnectionState();

    if ( null != cp )
    {
    	CheckpointMessage cpM = CheckpointMessage.createSetCheckpointMessage(cp);
    	doExecuteAndChangeState(bsPuller,cpM);
    }

    doExecuteAndChangeState(bsPuller,connState);
	Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [" + msgQueueState + "]", "Queue :" + currState + " to " + finalState);
	Assert.assertEquals(connState.getStateId(),finalState, "" + currState + " to " + finalState );
  }


  private BootstrapPullThread createBootstrapPullThread(boolean failBsConnection, boolean throwBsConnException, boolean muteTransition)
		  	throws Exception
  {
	  return createBootstrapPullThread(failBsConnection, throwBsConnException, muteTransition, false, false, null, 12000, 1, false);
  }

  private BootstrapPullThread createBootstrapPullThread(boolean failBsConnection,
		                                                boolean throwBsConnException,
		                                                boolean muteTransition,
		                                                int freeReadSpace,
		                                                int numBytesRead)
		  	throws Exception
  {
	  return createBootstrapPullThread(failBsConnection, throwBsConnException, muteTransition, false, false, null, freeReadSpace, numBytesRead, false);
  }

  private BootstrapPullThread createBootstrapPullThread(boolean failBsConnection,
		  												boolean throwBSConnException,
				  										boolean muteTransition,
				  										boolean readDataThrowException,
				  										boolean readDataException,
				  										String exceptionName,
				  										int freeReadSpace,
				  										int numBytesRead,
				  										boolean phaseCompleted)
		throws Exception
  {
	  List<String> sources = Arrays.asList("source1");

	  Properties clientProps = new Properties();

	  clientProps.setProperty("client.runtime.bootstrap.enabled", "true");
	  clientProps.setProperty("client.runtime.bootstrap.service(1).name", "bs1");
	  clientProps.setProperty("client.runtime.bootstrap.service(1).host", "localhost");
	  clientProps.setProperty("client.runtime.bootstrap.service(1).port", "10001");
	  clientProps.setProperty("client.runtime.bootstrap.service(1).sources", "source1");
	  clientProps.setProperty("client.runtime.bootstrap.service(2).name", "bs2");
	  clientProps.setProperty("client.runtime.bootstrap.service(2).host", "localhost");
	  clientProps.setProperty("client.runtime.bootstrap.service(2).port", "10002");
	  clientProps.setProperty("client.runtime.bootstrap.service(2).sources", "source1");
	  clientProps.setProperty("client.runtime.bootstrap.service(3).name", "bs3");
	  clientProps.setProperty("client.runtime.bootstrap.service(3).host", "localhost");
	  clientProps.setProperty("client.runtime.bootstrap.service(3).port", "10003");
	  clientProps.setProperty("client.runtime.bootstrap.service(3).sources", "source1");

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
		  org.easymock.EasyMock.expect(channel.getMetadata("x-dbus-error-cause")).andReturn(null).anyTimes();
		  org.easymock.EasyMock.expect(channel.getMetadata("x-dbus-req-id")).andReturn(null).anyTimes();
	  } else {
		  org.easymock.EasyMock.expect(channel.getMetadata("x-dbus-error-cause")).andReturn(exceptionName).anyTimes();
		  org.easymock.EasyMock.expect(channel.getMetadata("x-dbus-req-id")).andReturn(exceptionName).anyTimes();
	  }

	  if ( phaseCompleted)
	    org.easymock.EasyMock.expect(channel.getMetadata("PhaseCompleted")).andReturn("true").anyTimes();
	  else
		org.easymock.EasyMock.expect(channel.getMetadata("PhaseCompleted")).andReturn(null).anyTimes();

	  EasyMock.replay(channel);


	  DbusEventBuffer dbusBuffer = EasyMock.createMock(DbusEventBuffer.class);
	  dbusBuffer.endEvents(false, -1, false, false, null);
	  org.easymock.EasyMock.expectLastCall().anyTimes();

	  org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull(), org.easymock.EasyMock.<List<InternalDatabusEventsListener>>notNull(), org.easymock.EasyMock.<DbusEventsStatisticsCollector>isNull())).andReturn(numBytesRead).anyTimes();

	  if ( readDataThrowException)
	  {
		  org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull())).andThrow(new RuntimeException("dummy")).anyTimes();
	  } else {
		  org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull())).andReturn(numBytesRead).anyTimes();
	  }

	  org.easymock.EasyMock.expect(dbusBuffer.acquireIterator(org.easymock.EasyMock.<String>notNull())).andReturn(null).anyTimes();
	  dbusBuffer.waitForFreeSpace((int)(10000 * 100.0 / clientConf.getPullerBufferUtilizationPct()));
	  org.easymock.EasyMock.expectLastCall().anyTimes();
	  org.easymock.EasyMock.expect(dbusBuffer.getBufferFreeReadSpace()).andReturn(freeReadSpace).anyTimes();

	  EasyMock.replay(dbusBuffer);

	  //This guy succeeds on /sources but fails on /register
	  MockBootstrapConnection mockSuccessConn = new MockBootstrapConnection(10,10, channel,
			  serverIdx, muteTransition);

	  DatabusBootstrapConnectionFactory mockConnFactory =
			  EasyMock.createMock("mockRelayFactory", DatabusBootstrapConnectionFactory.class);

	  //each server should be tried MAX_RETRIES time until all retries are exhausted

	  if ( throwBSConnException )
	  {
		  org.easymock.EasyMock.expect(mockConnFactory.createConnection(
				  org.easymock.EasyMock.<ServerInfo>notNull(),
				  org.easymock.EasyMock.<ActorMessageQueue>notNull(),
				  org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andThrow(new RuntimeException("Mock Error")).anyTimes();
	  } else if ( failBsConnection) {
		  org.easymock.EasyMock.expect(mockConnFactory.createConnection(
				  org.easymock.EasyMock.<ServerInfo>notNull(),
				  org.easymock.EasyMock.<ActorMessageQueue>notNull(),
				  org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andReturn(null).anyTimes();
      } else {
		  org.easymock.EasyMock.expect(mockConnFactory.createConnection(
				  org.easymock.EasyMock.<ServerInfo>notNull(),
				  org.easymock.EasyMock.<ActorMessageQueue>notNull(),
				  org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockSuccessConn).anyTimes();
	  }


	  List<DatabusSubscription> sourcesSubList = DatabusSubscription.createSubscriptionList(sources);

	  // Mock Bootstrap Puller
	  RelayPullThread mockRelayPuller = EasyMock.createMock("rpt",RelayPullThread.class);
	  mockRelayPuller.enqueueMessage(org.easymock.EasyMock.notNull());
	  org.easymock.EasyMock.expectLastCall().anyTimes();

	  // Mock Relay Dispatcher
	  BootstrapDispatcher mockDispatcher = EasyMock.createMock("rd", BootstrapDispatcher.class);
	  mockDispatcher.enqueueMessage(org.easymock.EasyMock.notNull());
	  org.easymock.EasyMock.expectLastCall().anyTimes();

	  DatabusSourcesConnection sourcesConn2 = EasyMock.createMock(DatabusSourcesConnection.class);
	  org.easymock.EasyMock.expect(sourcesConn2.getSourcesNames()).andReturn(Arrays.asList("source1")).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getSubscriptions()).andReturn(sourcesSubList).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getConnectionConfig()).andReturn(srcConnConf).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getConnectionStatus()).andReturn(new DatabusComponentStatus("dummy")).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getLocalRelayCallsStatsCollector()).andReturn(null).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getRelayCallsStatsCollector()).andReturn(null).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getBootstrapConnFactory()).andReturn(mockConnFactory).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.loadPersistentCheckpoint()).andReturn(null).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getDataEventsBuffer()).andReturn(dbusBuffer).anyTimes();

	  org.easymock.EasyMock.expect(sourcesConn2.isBootstrapEnabled()).andReturn(true).anyTimes();

	  org.easymock.EasyMock.expect(sourcesConn2.getBootstrapConsumers()).andReturn(null).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getBootstrapServices()).andReturn(null).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getBootstrapEventsStatsCollector()).andReturn(null).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getRelayPullThread()).andReturn(mockRelayPuller).anyTimes();
	  org.easymock.EasyMock.expect(sourcesConn2.getBootstrapDispatcher()).andReturn(mockDispatcher).anyTimes();


	  EasyMock.makeThreadSafe(mockConnFactory, true);
	  EasyMock.makeThreadSafe(mockDispatcher, true);
	  EasyMock.makeThreadSafe(mockRelayPuller, true);
	  EasyMock.makeThreadSafe(sourcesConn2, true);

	  EasyMock.replay(mockConnFactory);
	  EasyMock.replay(sourcesConn2);
	  EasyMock.replay(mockDispatcher);
	  EasyMock.replay(mockRelayPuller);
	  BootstrapPullThread bsPuller = new BootstrapPullThread("RelayPuller", sourcesConn2, dbusBuffer, clientRtConf.getBootstrap().getServicesSet(),
			  new ArrayList<DbusKeyCompositeFilterConfig>(),
			  clientConf.getPullerBufferUtilizationPct(),
			  ManagementFactory.getPlatformMBeanServer());
	  mockSuccessConn.setCallback(bsPuller);

	  return bsPuller;
  }
}

class MockBootstrapConnection implements DatabusBootstrapConnection
{
  private final ChunkedBodyReadableByteChannel _streamResponse;
  private final AtomicInteger _sharedServerIdx;

  private AbstractActorMessageQueue _callback = null;
  private int _startScnCallCounter = 0;
  private int _targetScnCallCounter = 0;
  private int _closeCallCounter = 0;
  private int _streamCallCounter = 0;
  private boolean _muteTransition = false;
  private final long _startScn;
  private final long _targetScn;
  private Checkpoint _cp = null;

  public void setMuteTransition(boolean _muteTransition) {
	this._muteTransition = _muteTransition;
  }

public MockBootstrapConnection(
                             long startScn,
                             long targetScn,
                             ChunkedBodyReadableByteChannel streamResponse,
                             AtomicInteger sharedServerIdx)
  {
	  this(startScn, targetScn, streamResponse, sharedServerIdx, false);
  }

  public MockBootstrapConnection(
          long startScn,
          long targetScn,
          ChunkedBodyReadableByteChannel streamResponse,
          AtomicInteger sharedServerIdx,
          boolean muteTransition)
  {
    super();
    _streamResponse = streamResponse;
    _sharedServerIdx = sharedServerIdx;
    _muteTransition = muteTransition;
    _startScn = startScn;
    _targetScn = targetScn;
  }

  @Override
  public void close()
  {
	  _closeCallCounter++;
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
  public int getVersion()  {
    return 0;
  }

  public ChunkedBodyReadableByteChannel getStreamResponse() {
	return _streamResponse;
  }


  @Override
  public void requestTargetScn(Checkpoint checkpoint,
		  					   DatabusBootstrapConnectionStateMessage stateReuse)
  {
	  _cp = checkpoint;
	  _targetScnCallCounter++;

	  if ( -1 == _targetScn)
	  {
	    	if ( !_muteTransition) stateReuse.switchToTargetScnResponseError();
	  }  else {
		  _cp.setBootstrapTargetScn(_targetScn);
		  if ( !_muteTransition) stateReuse.switchToTargetScnSuccess();
	  }

	  if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
  }

  @Override
  public void requestStartScn(Checkpoint checkpoint,
		  					DatabusBootstrapConnectionStateMessage stateReuse,
		  					String sourceNames)
  {
	  _cp = checkpoint;
	  _startScnCallCounter++;

	  if ( -1 == _startScn)
	  {
	    	if ( !_muteTransition) stateReuse.switchToStartScnResponseError();
	  }  else {
		  _cp.setBootstrapStartScn(_startScn);
		  if ( !_muteTransition)  stateReuse.switchToStartScnSuccess(_cp, null, TestBootstrapPullThread._serverInfo);
	  }

	  if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
  }


  @Override
  public void requestStream(String sourcesIdList,
		                    DbusKeyFilter filter,
		                    int freeBufferSpace, Checkpoint cp,
		                    DatabusBootstrapConnectionStateMessage stateReuse)
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

  public int getStartScnCallCounter()
  {
    return _startScnCallCounter;
  }

  public int getTargetScnCallCounter()
  {
    return _targetScnCallCounter;
  }

  public int getCloseCallCounter()
  {
    return _closeCallCounter;
  }

}
