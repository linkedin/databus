package com.linkedin.databus.core;
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
