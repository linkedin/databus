package com.linkedin.databus3.espresso.client;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.BootstrapPullThread;
import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.RuntimeConfig;
import com.linkedin.databus.client.DatabusRelayConnectionFactory;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.RelayDispatcher;
import com.linkedin.databus.client.RelayPullThread;
import com.linkedin.databus.client.ServerSetChangeMessage;
import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.DatabusServerCoordinates.StateId;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus3.espresso.client.TestEspressoDatabusHttpClient.DummyEspressoStreamConsumer;

public class TestEspressoExternalViewChange
{
    public static final Logger LOG = Logger.getLogger("TestEspressoExternalViewChange");

    static
    {
      TestUtil.setupLogging(true, null, Level.ERROR);
    }

    @Test
    /** No Registration for a DB */
    public void testNoRegistrations() throws Exception
    {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientCfgBuilder =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
        clientCfgBuilder.getContainer().getJmx().setRmiEnabled(false);
        DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientCfgBuilder);

        DummyRelayPullThread relayPuller = createRelayPullThread(client);
        List<String> dbs = client.getDbList();
        dbs.add("db");

        RegistrationId id = new RegistrationId("1");
        DatabusCombinedConsumer consumer = new DummyEspressoStreamConsumer("dummy");
        List<DatabusSubscription> subs = new ArrayList<DatabusSubscription>();
        subs.add(new DatabusSubscription(new PhysicalSource("abc"), new PhysicalPartition(1, "p1"),new LogicalSourceId(new LogicalSource(1, "p1"), (short)1)));
        CheckpointPersistenceProvider cpProvider = null;
        DatabusV3Registration registration = new DatabusV3ConsumerRegistration(client,"db2", consumer, id,  subs, null, cpProvider, (DatabusComponentStatus)null);

        Map<RegistrationId, DatabusSourcesConnection> regIdToConnMap = client.getRegistrationToSourcesConnectionMap();
        regIdToConnMap.put(id, relayPuller.getSourcesConnection());

        Map<RegistrationId, DatabusV3Registration> regMap = client.getRegistrationIdMap();
        regMap.put(id, registration);

        Map<String, List<RegistrationId>>  dbToIds = client.getDbToRegistrationsMap();
        List<RegistrationId> ids = new ArrayList<RegistrationId>();
        ids.add(id);
        dbToIds.put("db2", ids);

        client.onExternalViewChange("db", null, null, null, null);
        Assert.assertEquals(relayPuller.getMessageList().isEmpty(), true);
        Assert.assertEquals(client.getRelayGroups().size(), 0);
    }

    @Test
    /**  Registration available for this DB but empty old and new view */
    public void testEmptyOldNewView() throws Exception
    {
      DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl();

      DummyRelayPullThread relayPuller = createRelayPullThread(client);
      List<String> dbs = client.getDbList();
      dbs.add("db");

      RegistrationId id = new RegistrationId("1");
      DatabusCombinedConsumer consumer = new DummyEspressoStreamConsumer("dummy");
      List<DatabusSubscription> subs = new ArrayList<DatabusSubscription>();
      subs.add(new DatabusSubscription(new PhysicalSource("abc"), new PhysicalPartition(1, "p1"),new LogicalSourceId(new LogicalSource(1, "p1"), (short)1)));
      CheckpointPersistenceProvider cpProvider = null;
      DatabusV3Registration registration = new DatabusV3ConsumerRegistration(client, "db", consumer, id,  subs, null, cpProvider, (DatabusComponentStatus)null);

      Map<RegistrationId, DatabusSourcesConnection> regIdToConnMap = client.getRegistrationToSourcesConnectionMap();
      regIdToConnMap.put(id, relayPuller.getSourcesConnection());

      Map<RegistrationId, DatabusV3Registration> regMap = client.getRegistrationIdMap();
      regMap.put(id, registration);

      Map<String, List<RegistrationId>>  dbToIds = client.getDbToRegistrationsMap();
      List<RegistrationId> ids = new ArrayList<RegistrationId>();
      ids.add(id);
      dbToIds.put("db", ids);

      client.onExternalViewChange("db", null, null, null, null);
      Assert.assertEquals(relayPuller.getMessageList().isEmpty(), true);
      Assert.assertEquals(client.getRelayGroups().size(), 0);
    }

    @Test
    /**
     * Registration with single subscriber.
     * Two cases :
     * <ol>
     *   <li> When a new external view is received
     *   <li> When old and new external view is same
     *   <li>
     *     <ol>When external view changes. The delta includes
     *       <li> One new Relay
     *       <li> One existing relay removed from the view
     *       <li> One relay becoming offline
     *     </ol>
     * </ol>
     */
    public void testSingleSubscriber() throws Exception
    {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientCfgBuilder =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
        clientCfgBuilder.getContainer().getJmx().setRmiEnabled(false);
        DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientCfgBuilder);

        DummyRelayPullThread relayPuller = createRelayPullThread(client);
        populateRegistrationCase1("1", "db", "db.bizprofile", 1, client, relayPuller);

        Map<ResourceKey, List<DatabusServerCoordinates>> oldexpMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
        Set<ResourceKey> oldrKeySet = new HashSet<ResourceKey>();
        Map<DatabusServerCoordinates, List<ResourceKey>> oldexpRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
        Map<String, StateId> oldserverStateMap = new HashMap<String, StateId>();

        Map<String, Map<String, String>> mp = new HashMap<String, Map<String, String>>();
        Map<ResourceKey, List<DatabusServerCoordinates>> expMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
        Set<ResourceKey> rKeySet = new HashSet<ResourceKey>();
        Map<DatabusServerCoordinates, List<ResourceKey>> expRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
        Map<String, StateId> serverStateMap = new HashMap<String, StateId>();

        String[] oldresourceKeys =
            {
                "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
                "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
                "ela4-db2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
                "ela4-db12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
            };

        String[] newresourceKeys =
              {
                  "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
                  "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
                  "ela4-dbNew2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
                  "ela4-dbNew12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
              };

        String[] oldrelayHosts =
            {
                "ela4-rly1.corp.linkedin.com_1111",
                "ela4-rly2.corp.linkedin.com_2222",
                "ela4-rly3.corp.linkedin.com_3333"
            };

        String[] newrelayHosts = {
                  "ela4-rly21.corp.linkedin.com_1111",
                  "ela4-rly22.corp.linkedin.com_2222",
                  "ela4-rly3.corp.linkedin.com_3333"
          };


        String oldofflineHost = null;

        String offlineHost = "ela4-rly2.corp.linkedin.com_2222";

        // First time when there is no old external view
        {
            populateExternalView(mp, oldexpMap, oldrKeySet, oldexpRMap, oldserverStateMap, oldresourceKeys, oldrelayHosts, oldofflineHost);
            client.onExternalViewChange("db", null, null, oldexpMap, oldexpRMap);

            Assert.assertEquals(relayPuller.getMessageList().size(), 1);
            Assert.assertEquals(client.getRelayGroups().size(), 1);
            Set<ServerInfo> gotRelays = client.getRelayGroups().get(client.getRelayGroups().keySet().iterator().next());
            int numOfflineHost = 0;
            Assert.assertEquals(gotRelays.size(), oldserverStateMap.keySet().size() - numOfflineHost);

            // CHeck if registerRelays set is expected
            for ( ServerInfo s : gotRelays)
            {
                Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
                Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
            }

            // CHeck if serversetChange Message set is expected
            ServerSetChangeMessage msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(0);
            for ( ServerInfo s : msg.getServerSet())
            {
                Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
                Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
            }

            relayPuller.getMessageList().clear();
        }

        // when old and new external view is same
        {
            client.onExternalViewChange("db", oldexpMap, oldexpRMap, oldexpMap, oldexpRMap);

            Assert.assertEquals(relayPuller.getMessageList().size(), 0);
            Assert.assertEquals(client.getRelayGroups().size(), 1);
            Set<ServerInfo> gotRelays = client.getRelayGroups().get(client.getRelayGroups().keySet().iterator().next());
            int numOfflineHost = 0;
            Assert.assertEquals(gotRelays.size(), oldserverStateMap.keySet().size() - numOfflineHost);

            // CHeck if registerRelays set is expected
            for ( ServerInfo s : gotRelays)
            {
                Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
                Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
            }
        }

        /*  When external view changes. The delta includes
         *      a) One new Relay
         *      b) One existing relay removed from the view
         *      c) One relay becoming offline
         */
        {
            populateExternalView(mp, expMap, rKeySet, expRMap, serverStateMap, newresourceKeys, newrelayHosts, offlineHost);
            Logger.getRootLogger().setLevel(Level.DEBUG);

            client.onExternalViewChange("db", oldexpMap, oldexpRMap, expMap, expRMap);

            Assert.assertEquals(relayPuller.getMessageList().size(), 1);
            Assert.assertEquals(client.getRelayGroups().size(), 1);
            Set<ServerInfo> gotRelays = client.getRelayGroups().get(client.getRelayGroups().keySet().iterator().next());
            int numOfflineHost = 1;
            Assert.assertEquals(gotRelays.size(), serverStateMap.keySet().size() - numOfflineHost);

            // CHeck if registerRelays set is expected
            for ( ServerInfo s : gotRelays)
            {
                Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
                Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
            }

            // CHeck if serversetChange Message set is expected
            ServerSetChangeMessage msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(0);
            for ( ServerInfo s : msg.getServerSet())
            {
                Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
                Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
            }
        }
    }

    @Test
    /**
     * Registration with multiple subscribers ( each one subscribing to different logical sources of
     * the same partition). Two cases :
     * <ol>
     *   <li> When a new external view is received
     *   <li> When old and new external view is same
     *   <li> When external view changes. The delta includes
     *     <ol>
     *       <li> One new Relay
     *       <li> One existing relay removed from the view
     *       <li> One relay becoming offline
     *     </ol>
     * </ol>
     */
    public void testMultiSubscribersDifferentLSources() throws Exception
    {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientCfgBuilder =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      clientCfgBuilder.getContainer().getJmx().setRmiEnabled(false);
      DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientCfgBuilder);

      DummyRelayPullThread relayPuller = createRelayPullThread(client);
      populateRegistrationCase2("1", "db", "db.bizprofile1", "db.bizprofile2", "db.bizprofile3", 1, client, relayPuller);

      Map<ResourceKey, List<DatabusServerCoordinates>> oldexpMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
      Set<ResourceKey> oldrKeySet = new HashSet<ResourceKey>();
      Map<DatabusServerCoordinates, List<ResourceKey>> oldexpRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
      Map<String, StateId> oldserverStateMap = new HashMap<String, StateId>();

      Map<String, Map<String, String>> mp = new HashMap<String, Map<String, String>>();
      Map<ResourceKey, List<DatabusServerCoordinates>> expMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
      Set<ResourceKey> rKeySet = new HashSet<ResourceKey>();
      Map<DatabusServerCoordinates, List<ResourceKey>> expRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
      Map<String, StateId> serverStateMap = new HashMap<String, StateId>();

      String[] oldresourceKeys =
          {
              "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
              "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
              "ela4-db2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
              "ela4-db12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
          };

      String[] newresourceKeys =
            {
                "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
                "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
                "ela4-dbNew2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
                "ela4-dbNew12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
            };

      String[] oldrelayHosts =
          {
              "ela4-rly1.corp.linkedin.com_1111",
              "ela4-rly2.corp.linkedin.com_2222",
              "ela4-rly3.corp.linkedin.com_3333"
          };

      String[] newrelayHosts = {
                "ela4-rly21.corp.linkedin.com_1111",
                "ela4-rly22.corp.linkedin.com_2222",
                "ela4-rly3.corp.linkedin.com_3333"
        };


      String oldofflineHost = null;

      String offlineHost = "ela4-rly2.corp.linkedin.com_2222";

      // First time when there is no old external view
      {
          populateExternalView(mp, oldexpMap, oldrKeySet, oldexpRMap, oldserverStateMap, oldresourceKeys, oldrelayHosts, oldofflineHost);
          client.onExternalViewChange("db", null, null, oldexpMap, oldexpRMap);

          Assert.assertEquals(relayPuller.getMessageList().size(), 1);
          Assert.assertEquals(client.getRelayGroups().size(), 1);
          Set<ServerInfo> gotRelays = client.getRelayGroups().get(client.getRelayGroups().keySet().iterator().next());
          int numOfflineHost = 0;
          Assert.assertEquals(gotRelays.size(), oldserverStateMap.keySet().size() - numOfflineHost);

          // CHeck if registerRelays set is expected
          for ( ServerInfo s : gotRelays)
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          // CHeck if serversetChange Message set is expected
          ServerSetChangeMessage msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(0);
          for ( ServerInfo s : msg.getServerSet())
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          relayPuller.getMessageList().clear();
      }

      // when old and new external view is same
      {
          client.onExternalViewChange("db", oldexpMap, oldexpRMap, oldexpMap, oldexpRMap);

          Assert.assertEquals(relayPuller.getMessageList().size(), 0);
          Assert.assertEquals(client.getRelayGroups().size(), 1);
          Set<ServerInfo> gotRelays = client.getRelayGroups().get(client.getRelayGroups().keySet().iterator().next());
          int numOfflineHost = 0;
          Assert.assertEquals(gotRelays.size(), oldserverStateMap.keySet().size() - numOfflineHost);

          // CHeck if registerRelays set is expected
          for ( ServerInfo s : gotRelays)
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }
      }

      /*  When external view changes. The delta includes
       *      a) One new Relay
       *      b) One existing relay removed from the view
       *      c) One relay becoming offline
       */
      {
          populateExternalView(mp, expMap, rKeySet, expRMap, serverStateMap, newresourceKeys, newrelayHosts, offlineHost);
          Logger.getRootLogger().setLevel(Level.DEBUG);

          client.onExternalViewChange("db", oldexpMap, oldexpRMap, expMap, expRMap);

          Assert.assertEquals(relayPuller.getMessageList().size(), 1);
          Assert.assertEquals(client.getRelayGroups().size(), 1);
          Set<ServerInfo> gotRelays = client.getRelayGroups().get(client.getRelayGroups().keySet().iterator().next());
          int numOfflineHost = 1;
          Assert.assertEquals(gotRelays.size(), serverStateMap.keySet().size() - numOfflineHost);

          // CHeck if registerRelays set is expected
          for ( ServerInfo s : gotRelays)
          {
              Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          // CHeck if serversetChange Message set is expected
          ServerSetChangeMessage msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(0);
          for ( ServerInfo s : msg.getServerSet())
          {
              Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }
      }
    }

    @Test
    //
    /**
     * Multiple Registrations with the same logical partition Id.
     * Two cases :
     * <ol>
     *   <li> When a new external view is received
     *   <li> When old and new external view is same
     *   <li> When external view changes. The delta includes
     *     <ol>
     *       <li> One new Relay
     *       <li> One existing relay removed from the view
     *       <li> One relay becoming offline
     *     </ol>
     * </ol>
     */
    public void testMultiSubscribersSameLSource() throws Exception
    {
      DatabusHttpV3ClientImpl.StaticConfigBuilder clientCfgBuilder =
          new DatabusHttpV3ClientImpl.StaticConfigBuilder();
      clientCfgBuilder.getContainer().getJmx().setRmiEnabled(false);
      DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientCfgBuilder);

      DummyRelayPullThread relayPuller = createRelayPullThread(client);
      populateMultipleRegistrations("db", 1, client, relayPuller);

      Map<ResourceKey, List<DatabusServerCoordinates>> oldexpMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
      Set<ResourceKey> oldrKeySet = new HashSet<ResourceKey>();
      Map<DatabusServerCoordinates, List<ResourceKey>> oldexpRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
      Map<String, StateId> oldserverStateMap = new HashMap<String, StateId>();

      Map<String, Map<String, String>> mp = new HashMap<String, Map<String, String>>();
      Map<ResourceKey, List<DatabusServerCoordinates>> expMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
      Set<ResourceKey> rKeySet = new HashSet<ResourceKey>();
      Map<DatabusServerCoordinates, List<ResourceKey>> expRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
      Map<String, StateId> serverStateMap = new HashMap<String, StateId>();

      String[] oldresourceKeys =
          {
              "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
              "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
              "ela4-db2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
              "ela4-db12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
          };

      String[] newresourceKeys =
            {
                "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
                "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
                "ela4-dbNew2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
                "ela4-dbNew12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
            };

      String[] oldrelayHosts =
          {
              "ela4-rly1.corp.linkedin.com_1111",
              "ela4-rly2.corp.linkedin.com_2222",
              "ela4-rly3.corp.linkedin.com_3333"
          };

      String[] newrelayHosts = {
                "ela4-rly21.corp.linkedin.com_1111",
                "ela4-rly22.corp.linkedin.com_2222",
                "ela4-rly3.corp.linkedin.com_3333"
        };


      String oldofflineHost = null;

      String offlineHost = "ela4-rly2.corp.linkedin.com_2222";

      // First time when there is no old external view
      {
          populateExternalView(mp, oldexpMap, oldrKeySet, oldexpRMap, oldserverStateMap, oldresourceKeys, oldrelayHosts, oldofflineHost);
          client.onExternalViewChange("db", null, null, oldexpMap, oldexpRMap);

          // Expect size 2 because, we are using the same Mock RelayPullTHread Object for both registrations
          Assert.assertEquals(relayPuller.getMessageList().size(), 2);
          Assert.assertEquals(client.getRelayGroups().size(), 2);
          Iterator<List<DatabusSubscription>> itr = client.getRelayGroups().keySet().iterator();
          Set<ServerInfo> gotRelays1 = client.getRelayGroups().get(itr.next());
          Set<ServerInfo> gotRelays2 = client.getRelayGroups().get(itr.next());

          int numOfflineHost = 0;
          Assert.assertEquals(gotRelays1.size(), oldserverStateMap.keySet().size() - numOfflineHost);
          Assert.assertEquals(gotRelays2.size(), oldserverStateMap.keySet().size() - numOfflineHost);

          // CHeck if registerRelays set is expected
          for ( ServerInfo s : gotRelays1)
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          for ( ServerInfo s : gotRelays2)
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          // CHeck if serversetChange Message set is expected
          ServerSetChangeMessage msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(0);
          for ( ServerInfo s : msg.getServerSet())
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(1);
          for ( ServerInfo s : msg.getServerSet())
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          relayPuller.getMessageList().clear();
      }

      // When old and new external view is same
      {
          client.onExternalViewChange("db", oldexpMap, oldexpRMap, oldexpMap, oldexpRMap);

          // Expect size 2 because, we are using the same Mock RelayPullTHread Object for both registrations
          Assert.assertEquals(relayPuller.getMessageList().size(), 0);
          Assert.assertEquals(client.getRelayGroups().size(), 2);
          Iterator<List<DatabusSubscription>> itr = client.getRelayGroups().keySet().iterator();
          Set<ServerInfo> gotRelays1 = client.getRelayGroups().get(itr.next());
          Set<ServerInfo> gotRelays2 = client.getRelayGroups().get(itr.next());

          int numOfflineHost = 0;
          Assert.assertEquals(gotRelays1.size(), oldserverStateMap.keySet().size() - numOfflineHost);
          Assert.assertEquals(gotRelays2.size(), oldserverStateMap.keySet().size() - numOfflineHost);

          // CHeck if registerRelays set is expected
          for ( ServerInfo s : gotRelays1)
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          for ( ServerInfo s : gotRelays2)
          {
              Assert.assertEquals(oldserverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(oldserverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }
      }

      /*  When external view changes. The delta includes
       *      a) One new Relay
       *      b) One existing relay removed from the view
       *      c) One relay becoming offline
       */
      {
          populateExternalView(mp, expMap, rKeySet, expRMap, serverStateMap, newresourceKeys, newrelayHosts, offlineHost);
          Logger.getRootLogger().setLevel(Level.DEBUG);

          client.onExternalViewChange("db", oldexpMap, oldexpRMap, expMap, expRMap);

          // Expect size 2 because, we are using the same Mock RelayPullTHread Object for both registrations
          Assert.assertEquals(relayPuller.getMessageList().size(), 2);
          Assert.assertEquals(client.getRelayGroups().size(), 2);
          Iterator<List<DatabusSubscription>> itr = client.getRelayGroups().keySet().iterator();
          Set<ServerInfo> gotRelays1 = client.getRelayGroups().get(itr.next());
          Set<ServerInfo> gotRelays2 = client.getRelayGroups().get(itr.next());

          int numOfflineHost = 1;
          Assert.assertEquals(gotRelays1.size(), serverStateMap.keySet().size() - numOfflineHost);
          Assert.assertEquals(gotRelays2.size(), serverStateMap.keySet().size() - numOfflineHost);

          // CHeck if registerRelays set is expected
          for ( ServerInfo s : gotRelays1)
          {
              Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          for ( ServerInfo s : gotRelays2)
          {
              Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          // CHeck if serversetChange Message set is expected
          ServerSetChangeMessage msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(0);
          for ( ServerInfo s : msg.getServerSet())
          {
              Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }

          msg = (ServerSetChangeMessage) relayPuller.getMessageList().get(1);
          for ( ServerInfo s : msg.getServerSet())
          {
              Assert.assertEquals(serverStateMap.containsKey(s.getAddress().toString()), true);
              Assert.assertEquals(serverStateMap.get(s.getAddress().toString()), StateId.ONLINE);
          }
      }
    }

  @Test
  /**
   * When registration is flushing. Earlier, this was simulated by adding an entry to
   * flushingRegIds table. Now it is based on contention on a lock which is hard to
   * deterministically introduce.
   */
  public void testOnExternalViewChange() throws Exception
  {
    DatabusHttpV3ClientImpl.StaticConfigBuilder clientCfgBuilder =
        new DatabusHttpV3ClientImpl.StaticConfigBuilder();
    clientCfgBuilder.getContainer().getJmx().setRmiEnabled(false);
    DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientCfgBuilder);

    DummyRelayPullThread relayPuller = createRelayPullThread(client);
    populateRegistrationCase1("1", "db", "db.bizprofile", 2, client, relayPuller);


    Map<ResourceKey, List<DatabusServerCoordinates>> oldexpMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
    Set<ResourceKey> oldrKeySet = new HashSet<ResourceKey>();
    Map<DatabusServerCoordinates, List<ResourceKey>> oldexpRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
    Map<String, StateId> oldserverStateMap = new HashMap<String, StateId>();

    Map<String, Map<String, String>> mp = new HashMap<String, Map<String, String>>();

    String[] oldresourceKeys =
        {
            "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
            "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
            "ela4-db2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
            "ela4-db12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
        };

    /*String[] newresourceKeys =
        {
            "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
            "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
            "ela4-dbNew2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
            "ela4-dbNew12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
        };*/

    String[] oldrelayHosts =
        {
            "ela4-rly1.corp.linkedin.com_1111",
            "ela4-rly2.corp.linkedin.com_2222",
            "ela4-rly3.corp.linkedin.com_3333"
        };


    String oldofflineHost = null;

    populateExternalView(mp, oldexpMap, oldrKeySet, oldexpRMap, oldserverStateMap, oldresourceKeys, oldrelayHosts, oldofflineHost);
    client.onExternalViewChange("db", null, null, oldexpMap, oldexpRMap);

    Assert.assertEquals(relayPuller.getMessageList().size(), 1);
    Assert.assertEquals(client.getRelayGroups().size(), 1);
  }


	/**
	 *  Generates 2 registrations : one with single subscription and another with 3 subscriptions
	 *  Used to test multiple registrations case
	 *
	 */
	private void populateMultipleRegistrations(String dbName,
												int partitionId,
												DatabusHttpV3ClientImpl client,
												DummyRelayPullThread relayPuller)
	{
		populateRegistrationCase1("1", dbName, "db.bizProfile1", partitionId, client, relayPuller);
		populateRegistrationCase2("2", dbName, "db.biProfile1", "db.bizProfile2", "db.bizProfile3", partitionId, client, relayPuller);
	}

	/**
	 *  Registration with single subscriber
	 */
	private void populateRegistrationCase1(
								   String regId,
			                       String dbName,
			                       String tableName,
			                       int partitionId,
			                       DatabusHttpV3ClientImpl client,
                                   DummyRelayPullThread relayPuller)
	{
	    RegistrationId id = new RegistrationId(regId);
		DatabusCombinedConsumer consumer = new DummyEspressoStreamConsumer("dummy");

		List<DatabusSubscription> subs = new ArrayList<DatabusSubscription>();
		PhysicalSource ps = PhysicalSource.createMasterSourceWildcard();
		PhysicalPartition pp = new PhysicalPartition(partitionId,dbName);
		LogicalSource ls = new LogicalSource(partitionId, tableName);
		LogicalSourceId lsId = new LogicalSourceId(ls, (short)partitionId);
		DatabusSubscription ds = new DatabusSubscription(ps,pp,lsId);
		subs.add(ds);

		CheckpointPersistenceProvider cpProvider = null;
		DatabusV3Registration registration = new DatabusV3ConsumerRegistration(client,dbName, consumer, id,  subs, null, cpProvider, (DatabusComponentStatus)null);
		Map<RegistrationId, DatabusSourcesConnection> regIdToConnMap = client.getRegistrationToSourcesConnectionMap();
		regIdToConnMap.put(id, relayPuller.getSourcesConnection());

		Map<RegistrationId, DatabusV3Registration> regMap = client.getRegistrationIdMap();
		regMap.put(id, registration);

		Map<String, List<RegistrationId>>  dbToIds = client.getDbToRegistrationsMap();
		List<RegistrationId> ids = dbToIds.get(dbName);

		if ( ids == null)
		{
			ids = new ArrayList<RegistrationId>();
			dbToIds.put(dbName, ids);
		}
		ids.add(id);

		List<String> dbs = client.getDbList();

		if (! dbs.contains(dbName))
			dbs.add(dbName);
	}

	/**
	 *  Registration with multiple subscribers all pointing to the same partition but different tables (logical sources)
	 */
	private void populateRegistrationCase2(
								   String regId,
			                       String dbName,
			                       String tableName1,
			                       String tableName2,
			                       String tableName3,
			                       int partitionId,
			                       DatabusHttpV3ClientImpl client,
                                   DummyRelayPullThread relayPuller)
	{
	    RegistrationId id = new RegistrationId(regId);
		DatabusCombinedConsumer consumer = new DummyEspressoStreamConsumer("dummy");

		List<DatabusSubscription> subs = new ArrayList<DatabusSubscription>();
		PhysicalSource ps = PhysicalSource.createMasterSourceWildcard();
		PhysicalPartition pp = new PhysicalPartition(partitionId,dbName);
		LogicalSource ls1 = new LogicalSource(1, tableName1);
		LogicalSourceId lsId1 = new LogicalSourceId(ls1, (short)partitionId);
		LogicalSource ls2 = new LogicalSource(2, tableName2);
		LogicalSourceId lsId2 = new LogicalSourceId(ls2, (short)partitionId);
		LogicalSource ls3 = new LogicalSource(3, tableName3);
		LogicalSourceId lsId3 = new LogicalSourceId(ls3, (short)partitionId);
		DatabusSubscription ds1 = new DatabusSubscription(ps,pp,lsId1);
		DatabusSubscription ds2 = new DatabusSubscription(ps,pp,lsId2);
		DatabusSubscription ds3 = new DatabusSubscription(ps,pp,lsId3);

		subs.add(ds1);
		subs.add(ds2);
		subs.add(ds3);

		CheckpointPersistenceProvider cpProvider = null;
		DatabusV3Registration registration = new DatabusV3ConsumerRegistration(client,dbName, consumer, id,  subs, null, cpProvider, (DatabusComponentStatus)null);
		Map<RegistrationId, DatabusSourcesConnection> regIdToConnMap = client.getRegistrationToSourcesConnectionMap();
		regIdToConnMap.put(id, relayPuller.getSourcesConnection());

		Map<RegistrationId, DatabusV3Registration> regMap = client.getRegistrationIdMap();
		regMap.put(id, registration);

		Map<String, List<RegistrationId>>  dbToIds = client.getDbToRegistrationsMap();
		List<RegistrationId> ids = dbToIds.get(dbName);

		if ( ids == null)
		{
			ids = new ArrayList<RegistrationId>();
			dbToIds.put(dbName, ids);
		}
		ids.add(id);

		List<String> dbs = client.getDbList();
		dbs.add(dbName);
	}

    private void populateExternalView(Map<String, Map<String, String>> mp,
    		Map<ResourceKey, List<DatabusServerCoordinates>> expMap,
             Set<ResourceKey> rKeySet,
             Map<DatabusServerCoordinates, List<ResourceKey>> expRMap,
             Map<String, StateId> serverStateMap,
             String[] oldResourceKeys,
             String[] oldRelayHosts,
             String oldOfflineHost)
           throws Exception
    {

		 for ( int i = 0 ; i < oldResourceKeys.length; i++)
		 {
			 Map<String, String> mp2 = new HashMap<String,String>();
			 List<DatabusServerCoordinates> c = new ArrayList<DatabusServerCoordinates>();
			 ResourceKey r1 = new ResourceKey(oldResourceKeys[i]);

			 for ( int j = 0; j < oldRelayHosts.length; j++)
			 {
				 mp2.put(oldRelayHosts[j], "ONLINE");

				 String []ip = oldRelayHosts[j].split("_");
				 InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
				 DatabusServerCoordinates rc = new DatabusServerCoordinates(ia,"ONLINE");

				 serverStateMap.put(rc.getAddress().toString(), StateId.ONLINE);
				 c.add(rc);

				 List<ResourceKey> rkeys = expRMap.get(rc);
				 if (rkeys == null)
				 {
					 rkeys = new ArrayList<ResourceKey>();
					 expRMap.put(rc, rkeys);
				 }
				 rkeys.add(r1);
			 }

			 DatabusServerCoordinates rc = null;
			 if (null != oldOfflineHost)
			 {
				 mp2.put(oldOfflineHost, "OFFLINE");
				 String []ip = oldOfflineHost.split("_");
				 InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
				 rc = new DatabusServerCoordinates(ia,"OFFLINE");
				 c.add(rc);
				 serverStateMap.put(rc.getAddress().toString(), StateId.OFFLINE);
		     }

			 mp.put(oldResourceKeys[i], mp2);

			 rKeySet.add(r1);

			 if ( null != rc)
			 {
				 List<ResourceKey> rkeys = expRMap.get(rc);
				 if (rkeys == null)
				 {
					 rkeys = new ArrayList<ResourceKey>();
					 expRMap.put(rc, rkeys);
				 }
				 rkeys.add(r1);
			 }
			 expMap.put(r1,c);
		 }
    }


    private DummyRelayPullThread createRelayPullThread(DatabusHttpClientImpl client)
          throws Exception
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

	    clientConf.getRuntime().setManagedInstance(client);
		DatabusHttpClientImpl.RuntimeConfig clientRtConf = clientConf.getRuntime().build();


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

		ChunkedBodyReadableByteChannel channel = EasyMock.createMock(ChunkedBodyReadableByteChannel.class);

		org.easymock.EasyMock.expect(channel.getMetadata(org.easymock.EasyMock.<String>notNull())).andReturn(null).anyTimes();

		EasyMock.replay(channel);

		DbusEventBuffer dbusBuffer = EasyMock.createMock(DbusEventBuffer.class);
		org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull(), org.easymock.EasyMock.<List<InternalDatabusEventsListener>>notNull(), org.easymock.EasyMock.<DbusEventsStatisticsCollector>isNull())).andReturn(1).anyTimes();
		org.easymock.EasyMock.expect(dbusBuffer.readEvents(org.easymock.EasyMock.<ReadableByteChannel>notNull())).andReturn(1).anyTimes();

		org.easymock.EasyMock.expect(dbusBuffer.acquireIterator(org.easymock.EasyMock.<String>notNull())).andReturn(null).anyTimes();
		dbusBuffer.waitForFreeSpace(10000);
		org.easymock.EasyMock.expectLastCall().anyTimes();
		org.easymock.EasyMock.expect(dbusBuffer.getBufferFreeReadSpace()).andReturn(0).anyTimes();

		EasyMock.replay(dbusBuffer);

		DatabusRelayConnectionFactory mockConnFactory =
				EasyMock.createMock("mockRelayFactory", DatabusRelayConnectionFactory.class);

		org.easymock.EasyMock.expect(mockConnFactory.createRelayConnection(
					org.easymock.EasyMock.<ServerInfo>notNull(),
					org.easymock.EasyMock.<ActorMessageQueue>notNull(),
					org.easymock.EasyMock.<RemoteExceptionHandler>notNull())).andThrow(new RuntimeException("Mock Error")).anyTimes();


		List<DatabusSubscription> sourcesSubList = DatabusSubscription.createSubscriptionList(sources);

		// Mock Bootstrap Puller
		BootstrapPullThread mockBsPuller = EasyMock.createMock("bpt",BootstrapPullThread.class);
		mockBsPuller.enqueueMessage(org.easymock.EasyMock.notNull());
		org.easymock.EasyMock.expectLastCall().anyTimes();

		// Mock Relay Dispatcher
		RelayDispatcher mockDispatcher = EasyMock.createMock("rd", RelayDispatcher.class);
		mockDispatcher.enqueueMessage(org.easymock.EasyMock.notNull());
		org.easymock.EasyMock.expectLastCall().anyTimes();

		DatabusSourcesConnection sourcesConn = EasyMock.createMock(DatabusSourcesConnection.class);
		DatabusSourcesConnection sourcesConn2 = EasyMock.createMock(DatabusSourcesConnection.class);
		org.easymock.EasyMock.expect(sourcesConn2.getSourcesNames()).andReturn(Arrays.asList("source1")).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getSourcesNames()).andReturn(Arrays.asList("source1")).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getSubscriptions()).andReturn(sourcesSubList).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getSubscriptions()).andReturn(sourcesSubList).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getConnectionConfig()).andReturn(srcConnConf).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getConnectionStatus()).andReturn(new DatabusComponentStatus("dummy")).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getLocalRelayCallsStatsCollector()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getRelayCallsStatsCollector()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getRelayConnFactory()).andReturn(mockConnFactory).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.loadPersistentCheckpoint()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getDataEventsBuffer()).andReturn(dbusBuffer).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.isBootstrapEnabled()).andReturn(false).anyTimes();

		org.easymock.EasyMock.expect(sourcesConn.getConnectionConfig()).andReturn(srcConnConf).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getConnectionStatus()).andReturn(new DatabusComponentStatus("dummy")).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getLocalRelayCallsStatsCollector()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getRelayCallsStatsCollector()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getRelayConnFactory()).andReturn(mockConnFactory).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.loadPersistentCheckpoint()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.getDataEventsBuffer()).andReturn(dbusBuffer).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn.isBootstrapEnabled()).andReturn(false).anyTimes();

		org.easymock.EasyMock.expect(sourcesConn2.getBootstrapConsumers()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getBootstrapServices()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getInboundEventsStatsCollector()).andReturn(null).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getBootstrapPuller()).andReturn(mockBsPuller).anyTimes();
		org.easymock.EasyMock.expect(sourcesConn2.getRelayDispatcher()).andReturn(mockDispatcher).anyTimes();


		EasyMock.makeThreadSafe(mockConnFactory, true);
		EasyMock.makeThreadSafe(mockDispatcher, true);
		EasyMock.makeThreadSafe(mockBsPuller, true);
		EasyMock.makeThreadSafe(sourcesConn2, true);
		EasyMock.makeThreadSafe(sourcesConn, true);

		EasyMock.replay(mockConnFactory);
		EasyMock.replay(sourcesConn);
		EasyMock.replay(mockDispatcher);
		EasyMock.replay(mockBsPuller);

		DummyRelayPullThread relayPuller = new DummyRelayPullThread("RelayPuller", sourcesConn, dbusBuffer, clientConf.getConnectionDefaults(),
				clientRtConf,
				new ArrayList<DbusKeyCompositeFilterConfig>(),
				ManagementFactory.getPlatformMBeanServer());


		org.easymock.EasyMock.expect(sourcesConn2.getRelayPullThread()).andReturn(relayPuller).anyTimes();
		EasyMock.replay(sourcesConn2);
		relayPuller.setSourcesConnection(sourcesConn2);

		return relayPuller;
    }

	public static class DummyRelayPullThread
	  extends RelayPullThread
    {
		private final List<Object> msgList = new ArrayList<Object>();

		public DummyRelayPullThread(String name,
				DatabusSourcesConnection sourcesConn,
				DbusEventBuffer dataEventsBuffer,
				DatabusSourcesConnection.StaticConfig sourceConf,
				RuntimeConfig clientRtConfig,
				List<DbusKeyCompositeFilterConfig> relayFilterConfigs,
				MBeanServer mbeanServer) {
			super(name, sourcesConn,
					dataEventsBuffer,
					clientRtConfig.getRelaysSet(),
					relayFilterConfigs,
					sourceConf.getConsumeCurrent(),
					sourceConf.getReadLatestScnOnError(),
					sourceConf.getPullerUtilizationPct(),
					mbeanServer);
		}

		@Override
		public void enqueueMessage(Object msg)
		{
			msgList.add(msg);
		}

		public void clearMessageList()
		{
			msgList.clear();
		}

		public List<Object> getMessageList()
		{
			return msgList;
		}
    }
}
