package com.linkedin.databus.core;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.data_model.PhysicalPartition;

public class TestPhysicalPartition
{

  @Test
  public void testSimpleStringSerialization()
  {
    PhysicalPartition p1 = new PhysicalPartition(10, "testdb");
    Assert.assertEquals(p1.toSimpleString(), "testdb:10");

    PhysicalPartition p2 = PhysicalPartition.createAnyPartitionWildcard("somedb");
    Assert.assertEquals(p2.toSimpleString(), "somedb:*");
  }

  @Test
  public void testSimpleStringDeserialization()
  {
    PhysicalPartition p1 = new PhysicalPartition(10, "testdb");
    PhysicalPartition p1test = PhysicalPartition.createFromSimpleString(p1.toSimpleString());
    Assert.assertEquals(p1test, p1);

    PhysicalPartition p2 = PhysicalPartition.createAnyPartitionWildcard("somedb");
    PhysicalPartition p2test = PhysicalPartition.createFromSimpleString(p2.toSimpleString());
    Assert.assertEquals(p2test, p2);

    PhysicalPartition p2test2 = PhysicalPartition.createFromSimpleString("somedb");
    Assert.assertEquals(p2test2, p2);
  }

  @Test(expectedExceptions=IllegalArgumentException.class)
  public void testSimpleStringDeserializationInvalidDbName1()
  {
    PhysicalPartition.createFromSimpleString(":10");
  }

  @Test(expectedExceptions=IllegalArgumentException.class)
  public void testSimpleStringDeserializationInvalidDbName2()
  {
    PhysicalPartition.createFromSimpleString("a:1:2");
  }

  @Test(expectedExceptions=IllegalArgumentException.class)
  public void testSimpleStringDeserializationInvalidPartid1()
  {
    PhysicalPartition.createFromSimpleString("db:ad");
  }

  @Test(expectedExceptions=IllegalArgumentException.class)
  public void testSimpleStringDeserializationInvalidInvalidPartid2()
  {
    PhysicalPartition.createFromSimpleString("db:");
  }

}
