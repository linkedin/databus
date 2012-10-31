package com.linkedin.databus3.espresso.client.data_model;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.SubscriptionUriCodec;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.test.TestUtil;

public class TestEspressoSubscriptionUriCodec
{

  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
    //Force class load
    EspressoSubscriptionUriCodec.getInstance();
  }

  @Test
  public void testDecodeHappyPath() throws DatabusException, URISyntaxException
  {
    EspressoSubscriptionUriCodec codec = EspressoSubscriptionUriCodec.getInstance();
    Assert.assertNotNull(codec);

    DatabusSubscription sub = codec.decode(new URI("espresso://MASTER/testDB/1/A"));
    Assert.assertTrue(sub.getPhysicalSource().isMasterSourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "testDB");
    Assert.assertEquals(sub.getPhysicalPartition().getId().intValue(), 1);
    Assert.assertEquals(sub.getLogicalPartition().getId().intValue(), 1);
    Assert.assertEquals(sub.getLogicalSource().getName(), "testDB.A");

    sub = codec.decode(new URI("espresso://SLAVE/myDB/*"));
    Assert.assertTrue(sub.getPhysicalSource().isSlaveSourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "myDB");
    Assert.assertTrue(sub.getPhysicalPartition().isAnyPartitionWildcard());
    Assert.assertTrue(sub.getLogicalSource().isAllSourcesWildcard());

    sub = codec.decode(new URI("espresso://ANY/testDB/*/myTable"));
    Assert.assertTrue(sub.getPhysicalSource().isAnySourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "testDB");
    Assert.assertTrue(sub.getPhysicalPartition().isAnyPartitionWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "testDB");
    Assert.assertEquals(sub.getLogicalSource().getName(), "testDB.myTable");

    sub = codec.decode(new URI("espresso:/espressoDB/1023/*"));
    Assert.assertTrue(sub.getPhysicalSource().isSlaveSourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "espressoDB");
    Assert.assertEquals(sub.getPhysicalPartition().getId().intValue(), 1023);
    Assert.assertTrue(sub.getLogicalSource().isAllSourcesWildcard());

    sub = codec.decode(new URI("espresso:///espressoDB/1023/*"));
    Assert.assertTrue(sub.getPhysicalSource().isSlaveSourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "espressoDB");
    Assert.assertEquals(sub.getPhysicalPartition().getId().intValue(), 1023);
    Assert.assertTrue(sub.getLogicalSource().isAllSourcesWildcard());
  }

  @Test
  public void testInvalidAuthority() throws URISyntaxException
  {
    EspressoSubscriptionUriCodec codec = EspressoSubscriptionUriCodec.getInstance();
    Assert.assertNotNull(codec);

    try
    {
      //invalid authority
      DatabusSubscription sub = codec.decode(new URI("espresso://Something//1/A"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("authority"));
    }

    try
    {
      //invalid authority
      DatabusSubscription sub = codec.decode(new URI("espresso://AnY/"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("authority"));
    }
  }

  @Test
  public void testInvalidDbName() throws URISyntaxException
  {
    EspressoSubscriptionUriCodec codec = EspressoSubscriptionUriCodec.getInstance();
    Assert.assertNotNull(codec);

    try
    {
      //missing DB name
      DatabusSubscription sub = codec.decode(new URI("espresso://ANY//1/A"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("DB"));
    }
  }

  @Test
  public void testInvalidPartition() throws URISyntaxException
  {
    EspressoSubscriptionUriCodec codec = EspressoSubscriptionUriCodec.getInstance();
    Assert.assertNotNull(codec);

    try
    {
      //missing partition
      DatabusSubscription sub = codec.decode(new URI("espresso://MASTER/db//A"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("partition"));
    }

    try
    {
      //invalid partition
      DatabusSubscription sub = codec.decode(new URI("espresso:/db/1a/*"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("partition"));
    }
  }

  @Test
  public void testTooManyComponents() throws URISyntaxException
  {
    EspressoSubscriptionUriCodec codec = EspressoSubscriptionUriCodec.getInstance();
    Assert.assertNotNull(codec);

    try
    {
      DatabusSubscription sub = codec.decode(new URI("espresso://MASTER/db/1/A/2"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("path"));
    }
  }

  @Test
  public void testInvalidScheme() throws URISyntaxException
  {
    EspressoSubscriptionUriCodec codec = EspressoSubscriptionUriCodec.getInstance();
    Assert.assertNotNull(codec);

    try
    {
      //missing scheme
      DatabusSubscription sub = codec.decode(new URI("//MASTER/db/1/A/2"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("scheme"));
    }

    try
    {
      //invalid scheme
      DatabusSubscription sub = codec.decode(new URI("Espresso://MASTER/db/1/A/2"));
      Assert.fail("no exception seen: " + sub);
    }
    catch (DatabusException e)
    {
      Assert.assertTrue(e.getMessage().contains("scheme"));
    }
  }

  @Test
  public void testRegistration() throws DatabusException, URISyntaxException
  {
    SubscriptionUriCodec codec = DatabusSubscription.getUriCodec("espresso");
    Assert.assertNotNull(codec);
    Assert.assertTrue(codec instanceof EspressoSubscriptionUriCodec);

    DatabusSubscription sub = DatabusSubscription.createFromUri("espresso://MASTER/testDB/1/A");
    Assert.assertTrue(sub.getPhysicalSource().isMasterSourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "testDB");
    Assert.assertEquals(sub.getPhysicalPartition().getId().intValue(), 1);
    Assert.assertEquals(sub.getLogicalPartition().getId().intValue(), 1);
    Assert.assertEquals(sub.getLogicalSource().getName(), "testDB.A");

    sub = DatabusSubscription.createFromUri(new URI("espresso://SLAVE/myDB/*/B"));
    Assert.assertTrue(sub.getPhysicalSource().isSlaveSourceWildcard());
    Assert.assertEquals(sub.getPhysicalPartition().getName(), "myDB");
    Assert.assertTrue(sub.getPhysicalPartition().isAnyPartitionWildcard());
    Assert.assertEquals(sub.getLogicalSource().getName(), "myDB.B");

  }
}
