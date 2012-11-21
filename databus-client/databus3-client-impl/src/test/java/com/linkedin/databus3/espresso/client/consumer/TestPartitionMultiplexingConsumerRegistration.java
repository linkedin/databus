package com.linkedin.databus3.espresso.client.consumer;

import java.util.HashSet;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RegistrationState;
import com.linkedin.databus.core.CompoundDatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus3.espresso.client.DatabusHttpV3ClientImpl;
import com.linkedin.databus3.espresso.client.data_model.EspressoSubscriptionUriCodec;

public class TestPartitionMultiplexingConsumerRegistration
{

  @BeforeClass
  public void setupClass()
  {
    //a work-around for TestNG not running the static initialization of EspressoSubscriptionUriCodec
    EspressoSubscriptionUriCodec.getInstance();
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

  @Test
  public void testCreation() throws Exception
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumerRegistration.testCreation");

    DatabusHttpV3ClientImpl.StaticConfigBuilder clientCfgBuilder =
        new DatabusHttpV3ClientImpl.StaticConfigBuilder();
    clientCfgBuilder.getConnectionDefaults().getPullerRetries().setMaxRetryNum(1);
    DatabusHttpV3ClientImpl.StaticConfig clientCfg = clientCfgBuilder.build();
    DatabusHttpV3ClientImpl client = new DatabusHttpV3ClientImpl(clientCfg);

    PartitionMultiplexingConsumer multi =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, null, null,
                                          100, false, false,
                                          DatabusSubscription.createFromUri("espresso:/testDB/0/TableA"),
                                          DatabusSubscription.createFromUri("espresso:/testDB/1/TableA"),
                                          DatabusSubscription.createFromUri("espresso:/testDB/2/TableA"));

    PartitionMultiplexingConsumerRegistration reg =
        new PartitionMultiplexingConsumerRegistration(
            client, "testDB", null, multi, multi.getRegId(), log);
    Assert.assertNotNull(reg);
    Assert.assertEquals(RegistrationState.CREATED, reg.getState());

    Assert.assertEquals(3, reg.getPartionRegs().size());
    for (Map.Entry<PhysicalPartition, DatabusV3Registration> entry: reg.getPartionRegs().entrySet())
    {
      Assert.assertEquals(1, entry.getValue().getSubscriptions().size());
      Assert.assertEquals(entry.getKey(), entry.getValue().getSubscriptions().get(0).getPhysicalPartition());
      Assert.assertEquals("testDB.TableA", entry.getValue().getSubscriptions().get(0).getLogicalSource().getName());
    }

    Assert.assertTrue(reg.getStatus() instanceof CompoundDatabusComponentStatus);
    CompoundDatabusComponentStatus rstatus = (CompoundDatabusComponentStatus)reg.getStatus();
    Assert.assertEquals(3, rstatus.getChildren().size());
    Assert.assertEquals(3, rstatus.getInitializing().size());
    HashSet<String> initSet = new HashSet<String>(rstatus.getInitializing());
    Assert.assertTrue(initSet.contains("Status_test1_testDB_0"));
    Assert.assertTrue(initSet.contains("Status_test1_testDB_1"));
    Assert.assertTrue(initSet.contains("Status_test1_testDB_2"));

    Assert.assertEquals(0, rstatus.getSuspended().size());
    Assert.assertEquals(0, rstatus.getPaused().size());
    Assert.assertEquals(0, rstatus.getShutdown().size());
  }

}
