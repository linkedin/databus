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
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ConnectionState.StateId;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.test.CheckpointForTesting;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
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

  private static final BootstrapCheckpointHandler _ckptHandlerSource1 =
      new BootstrapCheckpointHandler("source1");
  private static final BootstrapCheckpointHandler _ckptHandlerTwoSources =
      new BootstrapCheckpointHandler("source1", "source2");
  private static final Set<ServerInfo> EXP_SERVERINFO_1 = new TreeSet<ServerInfo>(Arrays.asList(
          new ServerInfo("bs1", "ONLINE", new InetSocketAddress("localhost",10001),"source1"),
          new ServerInfo("bs2","ONLINE",  new InetSocketAddress("localhost",10002),"source1"),
          new ServerInfo("bs3","ONLINE", new InetSocketAddress("localhost",10003),"source1")
          ));
  private static final Set<ServerInfo> EXP_SERVERINFO_2 = new TreeSet<ServerInfo>(EXP_SERVERINFO_1);
  private static final Set<ServerInfo> EXP_SERVERINFO_3 = new TreeSet<ServerInfo>(Arrays.asList(
          new ServerInfo("bs4","ONLINE", new InetSocketAddress("localhost",10000),"source1")
          ));


  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
    EXP_SERVERINFO_2.add(new ServerInfo("bs4","ONLINE", new InetSocketAddress("localhost",10000),"source1"));

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
  /** Test BOOTSTRAP transitions - Happy Path without startScn */
  public void testTransition_HappyPathWithoutStartScn() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
  }

  @Test
  /** Test BOOTSTRAP transitions - Happy Path with startScn */
  public void testTransition_HappyPathWithStartScn() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 50L);
    //TODO remove
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    cp.setBootstrapServerInfo(_serverInfoName);
    cp.setBootstrapStartScn(100L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    //cp.setBootstrapSinceScn(50L);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_STREAM, cp);
    ServerInfo bstServerInfo = ServerInfo.buildServerInfoFromHostPort(cp.getBootstrapServerInfo(),
                                                                      DbusConstants.HOSTPORT_DELIMITER);
    Assert.assertEquals(bsPuller.getConnectionState().getCurrentBSServerInfo(), bstServerInfo);
    Assert.assertEquals(cp.getBootstrapStartScn().longValue(), 100L, "Cleared Bootstrap StartSCN");
    Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
    String actualHost = bsPuller.getCurentServer().getAddress().getHostName();
    int actualPort = bsPuller.getCurentServer().getAddress().getPort();
    Assert.assertEquals(actualHost, _host, "Current Server Host Check");
    Assert.assertEquals(actualPort, _port, "Server Port Check");
    int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
    Assert.assertEquals(numRetries, 1000, "NumRetries Check");
  }

  @Test
  /** Test BOOTSTRAP transitions - Bootstrap Restart since no serverInfo */
  public void testTransition_RestartWithNoServerInfo() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 50L);
    cp.setBootstrapStartScn(1111L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
    Assert.assertEquals(cp.getBootstrapStartScn().longValue(),
                        Checkpoint.UNSET_BOOTSTRAP_START_SCN, "Cleared Bootstrap StartSCN");
    Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
    int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
    Assert.assertEquals(numRetries, 1000, "NumRetries Check");
  }

  @Test
  /** Test BOOTSTRAP transition - Bootstrap Restart since current errors in current serverInfo */
  public void testTransition_RestartDueToServerInfoErrors() throws Exception
  {
    final String dummyHost = "NonExistantHost";
    final String dummyServerInfoName = dummyHost + ":" + _port;

    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 50L);
    //TODO remove
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    //cp.setBootstrapSinceScn(900L);
    cp.setBootstrapStartScn(1111L);
    cp.setBootstrapServerInfo(dummyServerInfoName);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
    Assert.assertEquals(cp.getBootstrapStartScn().longValue(),
                        Checkpoint.UNSET_BOOTSTRAP_START_SCN, "Cleared Bootstrap StartSCN");
    Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
    int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
    Assert.assertEquals(numRetries, 1000, "NumRetries Check");
  }

  @Test
  /** Test BOOTSTRAP transition - Bootstrap Restart since malformed serverInfo */
  public void testTransition_RestartDueToMalformedServerInfo() throws Exception
  {
    final String dummyHost = "NonExistantHost";
    final String malformedServerInfoName = dummyHost + _port; // no delim

    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 50L);
    //TODO remove
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    //cp.setBootstrapSinceScn(900L);
    cp.setBootstrapStartScn(1111L);
    cp.setBootstrapServerInfo(malformedServerInfoName);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    //cp.setBootstrapSinceScn(50L);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);
    Assert.assertEquals(cp.getBootstrapStartScn().longValue(),
                        Checkpoint.UNSET_BOOTSTRAP_START_SCN, "Cleared Bootstrap StartSCN");
    Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), 50L, "Cleared Bootstrap SinceSCN");
    int numRetries = bsPuller.getRetriesBeforeCkptCleanup().getRemainingRetriesNum();
    Assert.assertEquals(numRetries, 1000, "NumRetries Check");
  }

  @Test
  /** Test BOOTSTRAP transition - Servers exhausted */
  public void testTransition_ServersExhausted() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, true, false);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 1L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.BOOTSTRAP, "SUSPEND_ON_ERROR", cp);
  }

  @Test
  /** Test Bootstrap transition: Connection Factory returned null with resumeCkpt startScn not set*/
  public void testTransition_ResumeCkptMissingStartScn() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(true, false, false);

    Checkpoint cp = _ckptHandlerTwoSources.createInitialBootstrapCheckpoint(null, 100L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.BOOTSTRAP, "SUSPEND_ON_ERROR", cp);
  }

  @Test
  /** Test Bootstrap transition: Connection Factory returned null with resumeCkpt startScn set*/
  public void testTransition_NullResumeCkptWithStartScn() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(true, false, false);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 50L);
    //TODO remove
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    //cp.setBootstrapSinceScn(50L);
    cp.setBootstrapStartScn(100L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.BOOTSTRAP, "SUSPEND_ON_ERROR", cp);
  }

  @Test
  /** Test bootstrap transition: Request_Start_Scn to Start_Scn_Sent */
  public void testTransition_RequestStartScnToStartScnSent() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, true);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_REQUEST_SENT, "", null);
  }

  @Test
  /** Test bootstrap transition: Request_StartSCN to StarSCN_Response_Success*/
  public void testTransition_RequestStartSCNToStartSCNResponseSuccess() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
  }

  @Test
  /** Test bootstrap transition: StartSCN_Response_Success : Happy path */
  public void testTransition_StartScnResponseSuccessHappyPath() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerTwoSources.createInitialBootstrapCheckpoint(null, 1000L);
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

  @Test
  /** StartSCN_Response_Success : when ServerInfo does not match */
  public void testTransition_StartScnResponseSuccessSIMismatch() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
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

  @Test
  /** Test bootstrap transition: StartSCN_Response_Success : when no ServerInfo */
  public void testTransition_StartSCNResponseSuccessNoSI() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerTwoSources.createInitialBootstrapCheckpoint(null, 0L);

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

  @Test
  /** Test Bootstrap transition: StartSCN_Response_Success : Error Case */
  public void testTransition_StartSCNResponseSuccessErrorCase() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true,
                                                             -10, 50);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    cp.setBootstrapServerInfo(_serverInfoName);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_ERROR, null);
    Assert.assertFalse(cp.isSnapShotSourceCompleted());
    Assert.assertEquals(cp.getBootstrapStartScn().longValue(), Checkpoint.UNSET_BOOTSTRAP_START_SCN);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
  }

  @Test
  /** Test bootstrap transition: Request_Stream when not enough space in the buffer */
  public void testTransition_RequestStreamFullBuffer() throws Exception
  {
    // available space is 10 which is less than the threshold of 10000
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 10, 1);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.REQUEST_STREAM, null);
  }

  @Test
  /** Test bootstrap transition: Request_Stream to Stream_request_success */
  public void testTransition_RequestStreamToStreamRequestSuccess() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);
  }

  @Test
  /** Test bootstrap transition: Request_Stream to Stream_request_sent */
  public void testTransition_RequestStreamToStreamRequestSet() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
    mockConn.setMuteTransition(true);

    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SENT, "", null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Success - Happy Path */
  public void testTransition_StreamResponseSuccessHappyPath() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Success - readEvents returned 0 bytes */
  public void testTransition_StreamResponseSuccessEmptyResponse() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 0);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER, null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Success - readEvents threw Exception */
  public void testTransition_StreamResponseReadEventsException() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, true, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.PICK_SERVER, null);
  }

  @Test
  /**
   * Test bootstrap transition: Stream_Response_Success -
   * Server returned Bootstrap_Too_Old_Exception
   */
  public void testTransition_StreamResponseBootstrapTooOld() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, true,
                                  BootstrapDatabaseTooOldException.class.getName(), 12000, 1,
                                  false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_ERROR,
                       null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Success - Server returned other exception */
  public void testTransition_StreamResponseOtherException() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, true, "Dummy Exception", 12000, 1,
                                  false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_ERROR, null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Success - Happy Path: Phase Completed */
  public void testTransition_StreamResponsePhaseCompleted() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Done - when phase not yet completed */
  public void testTransition_StreamResponsePhaseNotCompleted() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, false);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

  @Test
  /** Test bootstrap transition: Stream_Response_Done - when snapshot phase  completed */
  public void testTransition_StreamResponseSnapshotCompleted() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertTrue(cp.isSnapShotSourceCompleted(), "WindowSCN Check");
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT,"Consumption Mode check");
  }

  @Test
  /**
   * Test bootstrap transitions: Stream_Response_Done - when catchup phase  completed and going to
   * catchup next table
   */
  public void testTransition_StreamResponseCatchupNextTable() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true,
                                  50L, 100L, "source1", "source2");

    Checkpoint cp = _ckptHandlerTwoSources.createInitialBootstrapCheckpoint(null, 0L);
    cp.setBootstrapServerInfo(_serverInfoName);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourcesNameMap().put("source2", new IdNamePair(2L, "source2"));
    connState.getSourceIdMap().put(2L, new IdNamePair(2L, "source1"));
    connState.getSourceIdMap().put(2L, new IdNamePair(2L, "source1"));

    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
    bsPuller.getMessageQueue().clear();
    Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

    entries.put(1L, new ArrayList<RegisterResponseEntry>());
    connState.setSourcesSchemas(entries);

    //set the startSCN
    testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertEquals(cp.getSnapshotSource(), "source1");
    Assert.assertFalse(cp.isSnapShotSourceCompleted());

    //start snapshot for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    //finish the snapshot phase for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);

    //set targetSCN after the snapshot phase for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP);
    Assert.assertFalse(cp.isCatchupSourceCompleted());
    Assert.assertEquals(cp.getWindowScn(), 50L, "WindowSCN Check");
    Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

    //start catch-up for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    //finish the catch-up phase for source1
    cp.setWindowOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, cp);

    //start the snapshot phase for source2
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertEquals(cp.getSnapshotSource(), "source2");
    Assert.assertFalse(cp.isSnapShotSourceCompleted());
    Assert.assertTrue(cp.isCatchupSourceCompleted());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    //finish the snapshot phase for source2
    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    //set targetSCN after the snapshot phase for source2
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);

    //start catch-up for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP);
    Assert.assertFalse(cp.isCatchupSourceCompleted());
    Assert.assertEquals(cp.getWindowScn(), 50L, "WindowSCN Check");
    Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

    cp.setWindowOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, cp);

    //finish the catch-up phase for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_STREAM, cp);

    //start catch-up for source2
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP);
    Assert.assertFalse(cp.isCatchupSourceCompleted());
    Assert.assertEquals(cp.getWindowScn(), 50L, "WindowSCN Check");
    Assert.assertEquals(cp.getCatchupSource(), "source2", "Catchup Source check");

    //finish the catch-up phase for source2
    cp.setWindowOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, cp);



    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP,
                        "Consumption Mode check");
  }

  @Test
  /**
   * Test bootstrap transition: Stream_Response_Done - when catchup phase  completed and going to
   * snapshot next table
   */
  public void testTransition_StreamResponseCatchupToSnapshot() throws Exception
  {
    BootstrapPullThread bsPuller =
        createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true,
                                  50L, 100L, "source1", "source2");

    Checkpoint cp = _ckptHandlerTwoSources.createInitialBootstrapCheckpoint(null, 0L);
    cp.setBootstrapServerInfo(_serverInfoName);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourcesNameMap().put("source2", new IdNamePair(2L, "source2"));
    connState.getSourceIdMap().put(2L, new IdNamePair(2L, "source1"));
    connState.getSourceIdMap().put(2L, new IdNamePair(2L, "source1"));

    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_START_SCN, StateId.START_SCN_RESPONSE_SUCCESS, null);
    bsPuller.getMessageQueue().clear();
    Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();

    entries.put(1L, new ArrayList<RegisterResponseEntry>());
    connState.setSourcesSchemas(entries);

    //set the startSCN
    testTransitionCase(bsPuller, StateId.START_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertEquals(cp.getSnapshotSource(), "source1");
    Assert.assertFalse(cp.isSnapShotSourceCompleted());

    //start snapshot for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    //finish the snapshot phase for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);

    //set targetSCN after the snapshot phase for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP);
    Assert.assertFalse(cp.isCatchupSourceCompleted());
    Assert.assertEquals(cp.getWindowScn(), 50L, "WindowSCN Check");
    Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

    //start catch-up for source1
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    //finish the catch-up phase for source1
    cp.setWindowOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, cp);

    //start the snapshot phase for source2
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertEquals(cp.getSnapshotSource(), "source2");
    Assert.assertFalse(cp.isSnapShotSourceCompleted());
    Assert.assertTrue(cp.isCatchupSourceCompleted());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    //finish the snapshot phase for source2
    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);
  }

  @Test
  /** Test bootstrap transition: Stream_Response_Done - Bootstrap Done */
  public void testTransition_StreamResponseBootstrapDone() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    CheckpointForTesting cp = new CheckpointForTesting();
    _ckptHandlerSource1.createInitialBootstrapCheckpoint(cp, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    cp.setSnapshotOffset(-1);
    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);
    Assert.assertTrue(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");

    cp.setWindowOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.BOOTSTRAP_DONE, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.ONLINE_CONSUMPTION, "Consumption Mode check");
  }

  @Test
  /** Test bootstrap transition: Request_target_Scn to Target_Scn_Sent */
  public void testTransition_RequestTargetScnToTargetScnSent() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
    mockConn.setMuteTransition(true);
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_REQUEST_SENT, "", null);
  }

  @Test
  /** Test bootstap transition: Request_target_Scn to Target_Scn_Success */
  public void testTransition_RequestTargetScnSuccess() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);
  }

  @Test
  /** Test bootstrap transition: Target_Scn_Success : Happy Path */
  public void testTransition_TargetScnSuccessHappyPath() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    entries = new HashMap<Long, List<RegisterResponseEntry>>();

    entries.put(1L, new ArrayList<RegisterResponseEntry>());
    connState.setSourcesSchemas(entries);

    testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_SUCCESS, StateId.REQUEST_STREAM, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
    Assert.assertTrue(cp.isBootstrapTargetScnSet());
    Assert.assertEquals(cp.getBootstrapTargetScn().longValue(), 10L);
    Assert.assertEquals(cp.getWindowScn(), cp.getBootstrapStartScn().longValue());
    Assert.assertEquals(cp.getWindowOffset().longValue(), 0L);
  }

  @Test
  /** Test bootstrap transition: Target_Scn_Success to Error */
  public void testTransition_TargetScnSuccessError() throws Exception
  {
    // targetScn = 50 < 100 = startScn
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true,
                                                             100, 50);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
//    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_ERROR, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.TARGET_SCN_RESPONSE_ERROR, StateId.PICK_SERVER, null);
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());
    Assert.assertEquals(cp.getBootstrapTargetScn().longValue(), Checkpoint.UNSET_BOOTSTRAP_TARGET_SCN);

  }

  @Test
  public void testTransition_ErrorStates() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
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

  // Make sure that we suspend on error when we get the x-dbus-pending-size header with a size that is
  // larger than our dbusevent size.
  @Test
  public void testBootstrapPendingEvent() throws Exception
  {
    List<String> sources = Arrays.asList("source1");

    Properties clientProps = new Properties();

    clientProps.setProperty("client.container.httpPort", "0");
    clientProps.setProperty("client.container.jmx.rmiEnabled", "false");
    clientProps.setProperty("client.runtime.bootstrap.enabled", "true");
    clientProps.setProperty("client.runtime.bootstrap.service(1).name", "bs1");
    clientProps.setProperty("client.runtime.bootstrap.service(1).host", "localhost");
    clientProps.setProperty("client.runtime.bootstrap.service(1).port", "10001");
    clientProps.setProperty("client.runtime.bootstrap.service(1).sources", "source1");

    clientProps.setProperty("client.runtime.relay(1).name", "relay1");
    clientProps.setProperty("client.runtime.relay(1).port", "10001");
    clientProps.setProperty("client.runtime.relay(1).sources", "source1");

    clientProps.setProperty("client.connectionDefaults.eventBuffer.maxSize", "100000");
    clientProps.setProperty("client.connectionDefaults.pullerRetries.maxRetryNum", "3");

    DatabusHttpClientImpl.Config clientConfBuilder = new DatabusHttpClientImpl.Config();
    ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>("client.", clientConfBuilder);
    configLoader.loadConfig(clientProps);

    DatabusHttpClientImpl.StaticConfig clientConf = clientConfBuilder.build();
    DatabusSourcesConnection.StaticConfig srcConnConf = clientConf.getConnectionDefaults();

    DatabusHttpClientImpl client = new DatabusHttpClientImpl(clientConf);

    client.registerDatabusBootstrapListener(new LoggingConsumer(), null, "source1");

    Assert.assertNotNull(client, "client instantiation failed");

    DatabusHttpClientImpl.RuntimeConfig clientRtConf = clientConf.getRuntime().build();

    //we keep the index of the next server we expect to see
    AtomicInteger serverIdx = new AtomicInteger(-1);

    List<IdNamePair> sourcesResponse = new ArrayList<IdNamePair>();
    sourcesResponse.add(new IdNamePair(1L, "source1"));

    Map<Long, List<RegisterResponseEntry>> registerResponse = new HashMap<Long, List<RegisterResponseEntry>>();

    List<RegisterResponseEntry> regResponse = new ArrayList<RegisterResponseEntry>();
    regResponse.add(new RegisterResponseEntry(1L, (short)1, SCHEMA$.toString()));
    registerResponse.put(1L, regResponse);

    ChunkedBodyReadableByteChannel channel = EasyMock.createMock(ChunkedBodyReadableByteChannel.class);

    // getting the pending-event-size header is called twice, once for checking and once for logging.
    EasyMock.expect(channel.getMetadata(DatabusHttpHeaders.DATABUS_PENDING_EVENT_SIZE)).andReturn("1000000").times(2);
    EasyMock.expect(channel.getMetadata("x-dbus-error-cause")).andReturn(null).times(2);
    EasyMock.expect(channel.getMetadata("x-dbus-error")).andReturn(null).times(2);


    EasyMock.replay(channel);

    DbusEventBuffer dbusBuffer = EasyMock.createMock(DbusEventBuffer.class);
    dbusBuffer.endEvents(false, -1, false, false, null);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(
        dbusBuffer.injectEvent(
            EasyMock.<DbusEventInternalReadable>notNull())).andReturn(true).anyTimes();
    EasyMock.expect(dbusBuffer.getEventSerializationVersion()).andReturn(DbusEventFactory.DBUS_EVENT_V1).anyTimes();

    EasyMock.expect(dbusBuffer.getMaxReadBufferCapacity()).andReturn(600).times(2);
    EasyMock.expect(dbusBuffer.getBufferFreeReadSpace()).andReturn(600000).times(2);
    EasyMock.expect(dbusBuffer.readEvents(EasyMock.<ReadableByteChannel>notNull())).andReturn(1).times(1);
    EasyMock.expect(dbusBuffer.readEvents(EasyMock.<ReadableByteChannel>notNull(),
                                          EasyMock.<List<InternalDatabusEventsListener>>notNull(),
                                          EasyMock.<DbusEventsStatisticsCollector>isNull()))
        .andReturn(0).times(1);

    EasyMock.replay(dbusBuffer);
    ConnectionStateFactory connStateFactory = new ConnectionStateFactory(sources);

    //This guy succeeds on /sources but fails on /register
    MockBootstrapConnection mockSuccessConn = new MockBootstrapConnection(10,10, channel,
                                                                          serverIdx, false);

    DatabusBootstrapConnectionFactory mockConnFactory =
        org.easymock.EasyMock.createMock("mockRelayFactory", DatabusBootstrapConnectionFactory.class);

    //each server should be tried MAX_RETRIES time until all retries are exhausted

    EasyMock.expect(mockConnFactory.createConnection(
        EasyMock.<ServerInfo>notNull(),
        EasyMock.<ActorMessageQueue>notNull(),
        EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockSuccessConn).anyTimes();

    List<DatabusSubscription> sourcesSubList = DatabusSubscription.createSubscriptionList(sources);

    DatabusSourcesConnection sourcesConn2 = EasyMock.createMock(DatabusSourcesConnection.class);
    EasyMock.expect(sourcesConn2.getSourcesNames()).andReturn(Arrays.asList("source1")).anyTimes();
    EasyMock.expect(sourcesConn2.getSubscriptions()).andReturn(sourcesSubList).anyTimes();
    EasyMock.expect(sourcesConn2.getConnectionConfig()).andReturn(srcConnConf).anyTimes();
    EasyMock.expect(sourcesConn2.getConnectionStatus()).andReturn(new DatabusComponentStatus("dummy")).anyTimes();
    EasyMock.expect(sourcesConn2.getLocalRelayCallsStatsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getRelayCallsStatsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getUnifiedClientStats()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapConnFactory()).andReturn(mockConnFactory).anyTimes();
    EasyMock.expect(sourcesConn2.loadPersistentCheckpoint()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getDataEventsBuffer()).andReturn(dbusBuffer).anyTimes();

    EasyMock.expect(sourcesConn2.isBootstrapEnabled()).andReturn(true).anyTimes();

    EasyMock.expect(sourcesConn2.getBootstrapRegistrations()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapServices()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapEventsStatsCollector()).andReturn(null).anyTimes();


    EasyMock.makeThreadSafe(mockConnFactory, true);
    EasyMock.makeThreadSafe(sourcesConn2, true);

    EasyMock.replay(mockConnFactory);
    EasyMock.replay(sourcesConn2);
    BootstrapPullThread bsPuller = new BootstrapPullThread("RelayPuller", sourcesConn2, dbusBuffer, connStateFactory, clientRtConf.getBootstrap().getServicesSet(),
                                                           new ArrayList<DbusKeyCompositeFilterConfig>(),
                                                           clientConf.getPullerBufferUtilizationPct(),
                                                           ManagementFactory.getPlatformMBeanServer(),
                                                           new DbusEventV2Factory(),
                                                           null,
                                                           null);
    mockSuccessConn.setCallback(bsPuller);

    bsPuller.getComponentStatus().start();
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
    //cp.setSnapshotSource("source1");
    //cp.setCatchupSource("source1");
    //cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_REQUEST_SUCCESS, "SUSPEND_ON_ERROR", null);
    EasyMock.verify(channel);
    EasyMock.verify(sourcesConn2);
    EasyMock.verify(dbusBuffer);
    EasyMock.verify(channel);
    EasyMock.verify(mockConnFactory);
  }

  @Test
  /** Test ServerSet change when in PICK_SERVER state */
  public void testServerSetChange_PickServer() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToPickServer();
    bsPuller.enqueueMessage(connState);

    // ServerSetChange
    Assert.assertEquals(bsPuller.getCurrentServerIdx(), -1, "Current Server Index");
    Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
    Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");
    doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
    Assert.assertEquals(bsPuller.getCurrentServerIdx(), -1, "Current Server Index");
    Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
    Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
    Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
    Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while Pick_Server");
    Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while Pick_Server");
  }

  @Test
  /** Test ServerSet change when in Request_start_Scn state */
  public void testServerSetChange_RequestStartScn() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);

    bsPuller.getComponentStatus().start();
    ConnectionState connState = bsPuller.getConnectionState();
    connState.switchToBootstrap(cp);
    testTransitionCase(bsPuller, StateId.BOOTSTRAP, StateId.REQUEST_START_SCN, cp);


    // ServerSetChange when New Set includes CurrentServer
    {
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");
      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.REQUEST_START_SCN, "ServerSetChange while REQUEST_START_SCN");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [REQUEST_START_SCN]", "Queue :ServerSetChange while REQUEST_START_SCN");
    }

    // ServerSetChange when New Set excludes CurrentServer
    {
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
      Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_START_SCN");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_START_SCN");
    }
  }

  @Test
  /** Test ServerSet change when in Start_Scn_Request_Sent state */
  public void testServerSetChange_StartScnRequestSent() throws Exception
  {
    //Logger.getRootLogger().setLevel(Level.INFO);
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 1L);

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
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");

      Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.START_SCN_REQUEST_SENT, "ServerSetChange while START_SCN_REQUEST_SENT");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while START_SCN_REQUEST_SENT");
    }

    // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
    {
      int oldServerIndex = bsPuller.getCurrentServerIdx();
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");

      Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
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

  @Test
  /** Test ServerSet change when in Start_Scn_Response_Success */
  public void testServerSetChange_StartScnResponseSuccess() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);

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
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");

      Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.START_SCN_RESPONSE_SUCCESS, "ServerSetChange while START_SCN_RESPONSE_SUCCESS");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [START_SCN_RESPONSE_SUCCESS]", "Queue :ServerSetChange while START_SCN_RESPONSE_SUCCESS");
    }

    // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
    {
      int oldServerIndex = bsPuller.getCurrentServerIdx();
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");

      Map<Long, List<RegisterResponseEntry>> entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
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

  @Test
  /** Test ServerSet Change when in Request_stream state */
  public void testServerSetChange_RequestStream() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 1L);

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
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");
      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.REQUEST_STREAM, "ServerSetChange while REQUEST_STREAM");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [REQUEST_STREAM]", "Queue :ServerSetChange while REQUEST_STREAM");
    }

    // ServerSetChange when New Set excludes CurrentServer
    {
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
      Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_STREAM");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_STREAM");
    }
  }

  @Test
  /** Tests ServerSet change when in Stream_Request_Sent */
  public void testServerSetChange_StreamRequestSent() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
//    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
//    cp.setSnapshotSource("source1");
//    cp.setCatchupSource("source1");

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
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SENT, "ServerSetChange while STREAM_REQUEST_SENT");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while STREAM_REQUEST_SENT");
    }

    // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
    {
      int oldServerIndex = bsPuller.getCurrentServerIdx();
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
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

  @Test
  /** Test ServerSet change when in Stream_Response_Success state */
  public void testServerSetChange_StreamResponseSuccess() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, 12000, 1);
    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
//    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
//    cp.setSnapshotSource("source1");
//    cp.setCatchupSource("source1");

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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));

    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    // ServerSetChange when New Set includes CurrentServer
    {
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.STREAM_REQUEST_SUCCESS, "ServerSetChange while STREAM_REQUEST_SUCCESS");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [STREAM_REQUEST_SUCCESS]", "Queue :ServerSetChange while STREAM_REQUEST_SUCCESS");
    }

    // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
    {
      int oldServerIndex = bsPuller.getCurrentServerIdx();
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
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

  @Test
  /** Test ServerSet change when in Request_target_Scn state */
  public void testServerSetChange_RequestTargetScn() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);

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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    cp.setSnapshotOffset(-1);
    //cp.setBootstrapStartScn(100L);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    //Assert.assertEquals(cp.getWindowScn(), 100L, "WindowSCN Check");
    //Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP, "Consumption Mode check");
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertEquals(cp.getCatchupSource(), "source1", "Catchup Source check");

    // ServerSetChange when New Set includes CurrentServer
    {
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");
      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.REQUEST_TARGET_SCN, "ServerSetChange while REQUEST_TARGET_SCN");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [REQUEST_TARGET_SCN]", "Queue :ServerSetChange while REQUEST_TARGET_SCN");
    }

    // ServerSetChange when New Set excludes CurrentServer
    {
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
      Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.PICK_SERVER, "ServerSetChange while REQUEST_TARGET_SCN");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [PICK_SERVER]", "Queue :ServerSetChange while REQUEST_TARGET_SCN");
    }
  }

  @Test
  /** Tests ServerSet change when in Target_Scn_Request_Sent */
  public void testServerSetChange_TargetScnRequestSent() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
//    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
//    cp.setSnapshotSource("source1");
//    cp.setCatchupSource("source1");

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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertTrue(cp.isSnapShotSourceCompleted(), "Phase completed");
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    MockBootstrapConnection mockConn = (MockBootstrapConnection) connState.getBootstrapConnection();
    mockConn.setMuteTransition(true);
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_REQUEST_SENT, "", null);

    // ServerSetChange when New Set includes CurrentServer
    {
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_REQUEST_SENT, "ServerSetChange while TARGET_SCN_REQUEST_SENT");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while TARGET_SCN_REQUEST_SENT");
      Assert.assertTrue(cp.isSnapShotSourceCompleted(), "Phase completed");
      Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    }

    // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
    {
      int oldServerIndex = bsPuller.getCurrentServerIdx();
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), true, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_REQUEST_SENT, "ServerSetChange while TARGET_SCN_REQUEST_SENT");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: []", "Queue :ServerSetChange while TARGET_SCN_REQUEST_SENT");

      // Now Response arrives
      connState.switchToStartScnSuccess(cp, null, null);
      testTransitionCase(bsPuller, StateId.TARGET_SCN_REQUEST_SENT, StateId.PICK_SERVER, null);
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(bsPuller.getCurrentServerIdx() == -1, true, "Current Server Index undefined");
      Assert.assertEquals(bsPuller.getCurentServer() == null, true, "Current Server Null");
      Assert.assertTrue(cp.isSnapShotSourceCompleted(), "Phase completed");
      Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    }
  }

  @Test
  /** Test ServerSet change when in Target_Scn_Response_ Success state */
  public void testServerSetChange_TargetScnResponseSuccess() throws Exception
  {
    BootstrapPullThread bsPuller = createBootstrapPullThread(false, false, false, false, false, null, 12000, 1, true);

    Checkpoint cp = _ckptHandlerSource1.createInitialBootstrapCheckpoint(null, 0L);
    //TODO remove
//    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
//    cp.setSnapshotSource("source1");
//    cp.setCatchupSource("source1");

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

    connState.getSourcesNameMap().put("source1", new IdNamePair(1L, "source1"));
    connState.getSourceIdMap().put(1L, new IdNamePair(1L, "source1"));
    testTransitionCase(bsPuller, StateId.REQUEST_STREAM, StateId.STREAM_REQUEST_SUCCESS, null);

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.STREAM_REQUEST_SUCCESS, StateId.STREAM_RESPONSE_DONE, null);

    bsPuller.getMessageQueue().clear();
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    cp.setSnapshotOffset(-1);
    testTransitionCase(bsPuller, StateId.STREAM_RESPONSE_DONE, StateId.REQUEST_TARGET_SCN, null);
    Assert.assertTrue(cp.isSnapShotSourceCompleted(), "Phase completed");
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT, "Consumption Mode check");
    Assert.assertFalse(cp.isBootstrapTargetScnSet());

    bsPuller.getMessageQueue().clear();
    testTransitionCase(bsPuller, StateId.REQUEST_TARGET_SCN, StateId.TARGET_SCN_RESPONSE_SUCCESS, null);
    Assert.assertTrue(cp.isBootstrapTargetScnSet());
    Assert.assertEquals(cp.getBootstrapTargetScn().longValue(), 10L);

    // ServerSetChange when New Set includes CurrentServer
    {
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer() != null, true, "Current Server not Null");
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_1,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(true, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx() != -1, true, "Current Server Index defined");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");
      Assert.assertEquals(bsPuller.toTearConnAfterHandlingResponse(), false, "Tear Conn After Handling Response");
      Assert.assertEquals(connState.getStateId(),StateId.TARGET_SCN_RESPONSE_SUCCESS, "ServerSetChange while TARGET_SCN_RESPONSE_SUCCESS");
      Assert.assertEquals(bsPuller.getQueueListString(), "RelayPuller queue: [TARGET_SCN_RESPONSE_SUCCESS]", "Queue :ServerSetChange while TARGET_SCN_RESPONSE_SUCCESS");
    }

    // ServerSetChange when New Set excludes CurrentServer and SuccessFul Response
    {
      int oldServerIndex = bsPuller.getCurrentServerIdx();
      ServerInfo oldServer = bsPuller.getCurentServer();
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_2,"Server Set");

      entries = new HashMap<Long, List<RegisterResponseEntry>>();
      entries.put(1L, new ArrayList<RegisterResponseEntry>());
      connState.setSourcesSchemas(entries);

      doExecuteAndChangeState(bsPuller,createSetServerMessage(false, bsPuller));
      Assert.assertEquals(bsPuller.getCurrentServerIdx(), oldServerIndex, "Current Server Index unchanged");
      Assert.assertEquals(bsPuller.getCurentServer(), oldServer, "Current Server unchanged");
      Assert.assertEquals(bsPuller.getServers(),EXP_SERVERINFO_3,"Server Set");
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
    return createBootstrapPullThread(failBsConnection, throwBSConnException, muteTransition, readDataThrowException,
                                     readDataException, exceptionName, freeReadSpace, numBytesRead, phaseCompleted,
                                     10, 10);
  }

  private BootstrapPullThread createBootstrapPullThread(boolean failBsConnection,
                                                        boolean throwBSConnException,
                                                        boolean muteTransition,
                                                        boolean readDataThrowException,
                                                        boolean readDataException,
                                                        String exceptionName,
                                                        int freeReadSpace,
                                                        int numBytesRead,
                                                        boolean phaseCompleted,
                                                        long startScn,
                                                        long targetScn)
  throws Exception
  {
    return createBootstrapPullThread(failBsConnection, throwBSConnException, muteTransition, readDataThrowException,
                                     readDataException, exceptionName, freeReadSpace, numBytesRead, phaseCompleted,
                                     startScn, targetScn, "source1");
  }

  private BootstrapPullThread createBootstrapPullThread(boolean failBsConnection,
                              boolean throwBSConnException,
                              boolean muteTransition,
                              boolean readDataThrowException,
                              boolean readDataException,
                              String exceptionName,
                              int freeReadSpace,
                              int numBytesRead,
                              boolean phaseCompleted,
                              long startScn,
                              long targetScn,
                              String... sourceNames)
  throws Exception
  {
    List<String> sources = Arrays.asList(sourceNames);

    Properties clientProps = new Properties();

    clientProps.setProperty("client.container.httpPort", "0");
    clientProps.setProperty("client.container.jmx.rmiEnabled", "false");

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
      EasyMock.expect(channel.getMetadata("x-dbus-error-cause")).andReturn(null).anyTimes();
      EasyMock.expect(channel.getMetadata("x-dbus-req-id")).andReturn(null).anyTimes();
      EasyMock.expect(channel.getMetadata("x-dbus-error")).andReturn(null).anyTimes();
    } else {
      EasyMock.expect(channel.getMetadata("x-dbus-error-cause")).andReturn(exceptionName).anyTimes();
      EasyMock.expect(channel.getMetadata("x-dbus-req-id")).andReturn(exceptionName).anyTimes();
      EasyMock.expect(channel.getMetadata("x-dbus-error")).andReturn(exceptionName).anyTimes();
    }

    if ( phaseCompleted)
      EasyMock.expect(channel.getMetadata("PhaseCompleted")).andReturn("true").anyTimes();
    else
    EasyMock.expect(channel.getMetadata("PhaseCompleted")).andReturn(null).anyTimes();

    EasyMock.replay(channel);

    DbusEventBuffer dbusBuffer = EasyMock.createMock(DbusEventBuffer.class);
    dbusBuffer.endEvents(false, -1, false, false, null);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(dbusBuffer.injectEvent(EasyMock.<DbusEventInternalReadable>notNull())).andReturn(true).anyTimes();
    EasyMock.expect(dbusBuffer.getEventSerializationVersion()).andReturn(DbusEventFactory.DBUS_EVENT_V1).anyTimes();

    EasyMock.expect(dbusBuffer.readEvents(EasyMock.<ReadableByteChannel>notNull(),
                                          org.easymock.EasyMock.<List<InternalDatabusEventsListener>>notNull(),
                                          org.easymock.EasyMock.<DbusEventsStatisticsCollector>isNull()))
            .andReturn(numBytesRead).anyTimes();

    if ( readDataThrowException)
    {
      EasyMock.expect(dbusBuffer.readEvents(EasyMock.<ReadableByteChannel>notNull()))
              .andThrow(new RuntimeException("dummy")).anyTimes();
    } else {
      EasyMock.expect(dbusBuffer.readEvents(EasyMock.<ReadableByteChannel>notNull()))
              .andReturn(numBytesRead).anyTimes();
    }

    EasyMock.expect(dbusBuffer.acquireIterator(EasyMock.<String>notNull())).andReturn(null).anyTimes();
    dbusBuffer.waitForFreeSpace((int)(10000 * 100.0 / clientConf.getPullerBufferUtilizationPct()));
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(dbusBuffer.getBufferFreeReadSpace()).andReturn(freeReadSpace).anyTimes();

    EasyMock.replay(dbusBuffer);

    //This guy succeeds on /sources but fails on /register
    MockBootstrapConnection mockSuccessConn = new MockBootstrapConnection(startScn, targetScn, channel, serverIdx,
                                                                          muteTransition);

    DatabusBootstrapConnectionFactory mockConnFactory =
        org.easymock.EasyMock.createMock("mockRelayFactory", DatabusBootstrapConnectionFactory.class);

    //each server should be tried MAX_RETRIES time until all retries are exhausted

    if ( throwBSConnException )
    {
      EasyMock.expect(mockConnFactory.createConnection(
          EasyMock.<ServerInfo>notNull(),
          EasyMock.<ActorMessageQueue>notNull(),
          EasyMock.<RemoteExceptionHandler>notNull())).andThrow(new RuntimeException("Mock Error")).anyTimes();
    } else if ( failBsConnection) {
      EasyMock.expect(mockConnFactory.createConnection(
          EasyMock.<ServerInfo>notNull(),
          EasyMock.<ActorMessageQueue>notNull(),
          EasyMock.<RemoteExceptionHandler>notNull())).andReturn(null).anyTimes();
      } else {
      EasyMock.expect(mockConnFactory.createConnection(
          EasyMock.<ServerInfo>notNull(),
          EasyMock.<ActorMessageQueue>notNull(),
          EasyMock.<RemoteExceptionHandler>notNull())).andReturn(mockSuccessConn).anyTimes();
    }


    List<DatabusSubscription> sourcesSubList = DatabusSubscription.createSubscriptionList(sources);
    // Create ConnectionState
    ConnectionStateFactory connStateFactory = new ConnectionStateFactory(sources);
    // Mock Bootstrap Puller
    RelayPullThread mockRelayPuller = EasyMock.createMock("rpt",RelayPullThread.class);
    mockRelayPuller.enqueueMessage(EasyMock.notNull());
    EasyMock.expectLastCall().anyTimes();

    // Mock Relay Dispatcher
    BootstrapDispatcher mockDispatcher = EasyMock.createMock("rd", BootstrapDispatcher.class);
    mockDispatcher.enqueueMessage(EasyMock.notNull());
    EasyMock.expectLastCall().anyTimes();

    //Set up mock for sources connection
    DatabusSourcesConnection sourcesConn2 = EasyMock.createMock(DatabusSourcesConnection.class);
    EasyMock.expect(sourcesConn2.getSourcesNames()).andReturn(Arrays.asList("source1")).anyTimes();
    EasyMock.expect(sourcesConn2.getSubscriptions()).andReturn(sourcesSubList).anyTimes();
    EasyMock.expect(sourcesConn2.getConnectionConfig()).andReturn(srcConnConf).anyTimes();
    EasyMock.expect(sourcesConn2.getConnectionStatus()).andReturn(new DatabusComponentStatus("dummy")).anyTimes();
    EasyMock.expect(sourcesConn2.getLocalRelayCallsStatsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getRelayCallsStatsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getUnifiedClientStats()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapConnFactory()).andReturn(mockConnFactory).anyTimes();
    EasyMock.expect(sourcesConn2.loadPersistentCheckpoint()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getDataEventsBuffer()).andReturn(dbusBuffer).anyTimes();
    EasyMock.expect(sourcesConn2.isBootstrapEnabled()).andReturn(true).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapRegistrations()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapServices()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapEventsStatsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(sourcesConn2.getRelayPullThread()).andReturn(mockRelayPuller).anyTimes();
    EasyMock.expect(sourcesConn2.getBootstrapDispatcher()).andReturn(mockDispatcher).anyTimes();


    EasyMock.makeThreadSafe(mockConnFactory, true);
    EasyMock.makeThreadSafe(mockDispatcher, true);
    EasyMock.makeThreadSafe(mockRelayPuller, true);
    EasyMock.makeThreadSafe(sourcesConn2, true);

    EasyMock.replay(mockConnFactory);
    EasyMock.replay(sourcesConn2);
    EasyMock.replay(mockDispatcher);
    EasyMock.replay(mockRelayPuller);
    BootstrapPullThread bsPuller = new BootstrapPullThread("RelayPuller", sourcesConn2, dbusBuffer, connStateFactory,
                                                           clientRtConf.getBootstrap().getServicesSet(),
                                                           new ArrayList<DbusKeyCompositeFilterConfig>(),
                                                           clientConf.getPullerBufferUtilizationPct(),
                                                           null,
                                                           new DbusEventV2Factory(),
                                                           null,
                                                           null);
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

  public void setMuteTransition(boolean muteTransition)
  {
    _muteTransition = muteTransition;
  }

  public MockBootstrapConnection(long startScn,
                                 long targetScn,
                                 ChunkedBodyReadableByteChannel streamResponse,
                                 AtomicInteger sharedServerIdx)
  {
    this(startScn, targetScn, streamResponse, sharedServerIdx, false);
  }

  public MockBootstrapConnection(long startScn,
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
  public int getProtocolVersion()
  {
    return 0;
  }

  public ChunkedBodyReadableByteChannel getStreamResponse()
  {
    return _streamResponse;
  }


  @Override
  public void requestTargetScn(Checkpoint checkpoint,
                   DatabusBootstrapConnectionStateMessage stateReuse)
  {
    _cp = checkpoint;
    _targetScnCallCounter++;

    try
    {
      if ( -1 == _targetScn)
      {
        if ( !_muteTransition) stateReuse.switchToTargetScnResponseError();
      }  else {
        _cp.setBootstrapTargetScn(_targetScn);
        if ( !_muteTransition) stateReuse.switchToTargetScnSuccess();
      }

      if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
    }
    catch (RuntimeException e)
    {
      TestBootstrapPullThread.LOG.error("requestTargetScn exception:" + e, e);
      stateReuse.switchToTargetScnResponseError();
      _callback.enqueueMessage(stateReuse);
    }
  }

  @Override
  public void requestStartScn(Checkpoint checkpoint,
                DatabusBootstrapConnectionStateMessage stateReuse,
                String sourceNames)
  {
    _cp = checkpoint;
    _startScnCallCounter++;

    try
    {
      if ( -1 == _startScn)
      {
        if ( !_muteTransition) stateReuse.switchToStartScnResponseError();
      }  else {
        _cp.setBootstrapStartScn(_startScn);
        if ( !_muteTransition)  stateReuse.switchToStartScnSuccess(_cp, null, TestBootstrapPullThread._serverInfo);
      }

      if (null != _callback && !_muteTransition) _callback.enqueueMessage(stateReuse);
    }
    catch (RuntimeException e)
    {
      TestBootstrapPullThread.LOG.error("requestStartScn exception: " + e, e);
      stateReuse.switchToStartScnResponseError();
      _callback.enqueueMessage(stateReuse);
    }
  }


  @Override
  public void requestStream(String sourcesIdList,
                            DbusKeyFilter filter,
                            int freeBufferSpace, Checkpoint cp,
                            DatabusBootstrapConnectionStateMessage stateReuse)
  {
    ++_streamCallCounter;
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

  /**
   * @see com.linkedin.databus.client.DatabusBootstrapConnection#getRemoteHost()
   */
  @Override
  public String getRemoteHost()
  {
    return DbusConstants.UNKNOWN_HOST;
  }

  /**
   * @see com.linkedin.databus.client.DatabusBootstrapConnection#getRemoteService()
   */
  @Override
  public String getRemoteService()
  {
    return DbusConstants.UNKNOWN_SERVICE_ID;
  }

  @Override
  public int getMaxEventVersion()
  {
    return 0;
  }

}
