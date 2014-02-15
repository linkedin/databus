package com.linkedin.databus.core.data_model;
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


import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.TestUtil;

public class TestSubscriptions
{
  static
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

  @Test
  public void testEquals()
  {
    PhysicalPartition db1_1 = new PhysicalPartition(1, "db1");
    DatabusSubscription sub1 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(new LogicalSource(101, "source1"), (short)2));
    Assert.assertTrue(! sub1.equals(null));
    Assert.assertTrue(! sub1.equals(db1_1));

    DatabusSubscription sub2 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(new LogicalSource(101, "source1"), (short)2));

    Assert.assertEquals(sub1, sub2);

    DatabusSubscription sub3 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(new LogicalSource(101, "source1"), (short)3));
    Assert.assertTrue(! sub1.equals(sub3));

    //LogicalSource wildcard
    DatabusSubscription sub4 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(LogicalSource.createAllSourcesWildcard(),
                                                    (short)2));
    Assert.assertEquals(sub4, sub1);

    //different logical partitions
    DatabusSubscription sub5 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(LogicalSource.createAllSourcesWildcard(),
                                                    (short)1));
    Assert.assertNotEquals(sub5, sub1);

    //same logical partition number but different logical source
    DatabusSubscription sub6 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(new LogicalSource(102, "source2"),
                                                    (short)2));
    Assert.assertNotEquals(sub6, sub1);

    //logical source with unknown logical source id
    DatabusSubscription sub7 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                new LogicalSourceId(new LogicalSource("source1"),
                                                    (short)2));
    Assert.assertEquals(sub7, sub1);

    //logical partition wildcard
    DatabusSubscription sub8 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                LogicalSourceId.createAllPartitionsWildcard(new LogicalSource("source1")));
    Assert.assertEquals(sub8, sub1);

    //logical partition wildcard but different logical source
    DatabusSubscription sub9 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                LogicalSourceId.createAllPartitionsWildcard(new LogicalSource("source2")));
    Assert.assertNotEquals(sub9, sub1);

    //logical partition wildcard and logical soruce wildcard
    DatabusSubscription sub10 =
        new DatabusSubscription(new PhysicalSource("uri1"), db1_1,
                                LogicalSourceId.createAllPartitionsWildcard(LogicalSource.createAllSourcesWildcard()));
    Assert.assertEquals(sub10, sub1);
  }

  @Test
  public void testLogicalSourceJsonSerDeser() throws Exception
  {
    LogicalSource lsource1 = new LogicalSource(123, "test.source");
    Assert.assertEquals(lsource1.getId().intValue(), 123);
    Assert.assertEquals(lsource1.getName(), "test.source");
    String lsource1Json = lsource1.toJsonString();
    LogicalSource lsource1_new = LogicalSource.createFromJsonString(lsource1Json);
    Assert.assertEquals(lsource1, lsource1_new);
    Assert.assertEquals(lsource1_new.getId().intValue(), 123);
    Assert.assertEquals(lsource1_new.getName(), "test.source");

    String lsource2Json = "{\"name\":\"mysource\"}";
    LogicalSource lsource2_new = LogicalSource.createFromJsonString(lsource2Json);
    Assert.assertEquals(lsource2_new.getName(), "mysource");
    Assert.assertEquals(lsource2_new.getId(), LogicalSource.UNKNOWN_LOGICAL_SOURCE_ID);

    String lsource3Json = "{\"id\":98765}";
    LogicalSource lsource3_new = LogicalSource.createFromJsonString(lsource3Json);
    Assert.assertEquals(lsource3_new.getName(), LogicalSource.ALL_LOGICAL_SOURCES_NAME);
    Assert.assertEquals(lsource3_new.getId().intValue(), 98765);

    String lsource4Json = "{\"name\":\"*\"}";
    LogicalSource lsource4_new = LogicalSource.createFromJsonString(lsource4Json);
    Assert.assertEquals(lsource4_new.getName(), "*");
    Assert.assertEquals(lsource4_new.getId(), LogicalSource.ALL_LOGICAL_SOURCES_ID);
    Assert.assertTrue(lsource4_new.isAllSourcesWildcard());
  }

  @Test
  public void testPhysicalSourceJsonSerDeser() throws Exception
  {
    PhysicalSource psource1 = new PhysicalSource("test.uri");
    Assert.assertEquals(psource1.getUri(), "test.uri");
    String psource1Json = psource1.toJsonString();
    PhysicalSource psource1_new = PhysicalSource.createFromJsonString(psource1Json);
    Assert.assertEquals(psource1, psource1_new);
    Assert.assertEquals(psource1_new.getUri(), "test.uri");

    String psource2Json = "{\"uri\":\"my.uri\"}";
    PhysicalSource psource2_new = PhysicalSource.createFromJsonString(psource2Json);
    Assert.assertEquals(psource2_new.getUri(), "my.uri");

    PhysicalSource psource3 = PhysicalSource.createAnySourceWildcard();
    String psource3Json = psource3.toJsonString();
    PhysicalSource psource3_new = PhysicalSource.createFromJsonString(psource3Json);
    Assert.assertTrue(psource3_new.isAnySourceWildcard());
    Assert.assertEquals(psource3, psource3_new);

    PhysicalSource psource4 = PhysicalSource.createMasterSourceWildcard();
    String psource4Json = psource4.toJsonString();
    PhysicalSource psource4_new = PhysicalSource.createFromJsonString(psource4Json);
    Assert.assertTrue(psource4_new.isMasterSourceWildcard());
    Assert.assertEquals(psource4, psource4_new);

    PhysicalSource psource5 = PhysicalSource.createSlaveSourceWildcard();
    String psource5Json = psource5.toJsonString();
    PhysicalSource psource5_new = PhysicalSource.createFromJsonString(psource5Json);
    Assert.assertTrue(psource5_new.isSlaveSourceWildcard());
    Assert.assertEquals(psource5, psource5_new);
  }

  @Test
  public void testLogicalSourceIdJsonSerDeser() throws Exception
  {
    LogicalSourceId lsourceId1 = new LogicalSourceId(new LogicalSource(1234, "mytest.source"),
                                                     (short)10);
    Assert.assertEquals(lsourceId1.getSource().getId().intValue(), 1234);
    Assert.assertEquals(lsourceId1.getSource().getName(), "mytest.source");
    Assert.assertEquals(lsourceId1.getId().shortValue(), (short)10);

    String lsourceId1Json = lsourceId1.toJsonString();
    LogicalSourceId lsourceId1_new = LogicalSourceId.createFromJsonString(lsourceId1Json);
    Assert.assertEquals(lsourceId1, lsourceId1_new);

    String lsourceId2Json = "{\"source\":{\"name\":\"mysource2\"}}";
    LogicalSourceId lsourceId2_new = LogicalSourceId.createFromJsonString(lsourceId2Json);
    Assert.assertEquals(lsourceId2_new.getSource().getName(), "mysource2");
    Assert.assertEquals(lsourceId2_new.getSource().getId(), LogicalSource.UNKNOWN_LOGICAL_SOURCE_ID);
    Assert.assertEquals(lsourceId2_new.getId(), LogicalSourceId.ALL_LOGICAL_PARTITIONS_ID);

    String lsourceId3Json = "{\"source\":{\"name\":\"mysource3\",\"id\":666},\"id\":999}";
    LogicalSourceId lsourceId3_new = LogicalSourceId.createFromJsonString(lsourceId3Json);
    Assert.assertEquals(lsourceId3_new.getSource().getName(), "mysource3");
    Assert.assertEquals(lsourceId3_new.getSource().getId().intValue(), 666);
    Assert.assertEquals(lsourceId3_new.getId().intValue(), 999);
  }

  @Test
  public void testPhysicalPartitionJsonSerDeser() throws Exception
  {
    PhysicalPartition ppart1 = new PhysicalPartition(123456, "name");
    Assert.assertEquals(ppart1.getName(), "name");
    Assert.assertEquals(ppart1.getId().intValue(), 123456);
    String ppart1Json = ppart1.toJsonString();
    PhysicalPartition ppart1_new = PhysicalPartition.createFromJsonString(ppart1Json);
    Assert.assertEquals(ppart1, ppart1_new);

    String ppart2Json = "{\"id\":10000}";
    PhysicalPartition ppart2_new = PhysicalPartition.createFromJsonString(ppart2Json);
    Assert.assertEquals(ppart2_new.getId().intValue(), 10000);

    PhysicalPartition ppart3 = PhysicalPartition.createAnyPartitionWildcard();
    String ppart3Json = ppart3.toJsonString();
    PhysicalPartition ppart3_new = PhysicalPartition.createFromJsonString(ppart3Json);
    Assert.assertEquals(ppart3, ppart3_new);
    Assert.assertEquals(ppart3_new.getId(), PhysicalPartition.ANY_PHYSICAL_PARTITION_ID);
  }

  @Test
  public void testSubscriptionJsonSerDeser() throws Exception
  {
    DatabusSubscription.Builder builder = new DatabusSubscription.Builder();
    builder.getPhysicalPartition().setId(2233);
    builder.getPhysicalPartition().setName("psource");
    builder.getPhysicalSource().setUri("http://some.uri.com");
    builder.getLogicalPartition().setId((short)1);
    builder.getLogicalPartition().getSource().setId(2);
    builder.getLogicalPartition().getSource().setName("source1");

    DatabusSubscription sub1 = builder.build();
    Assert.assertEquals(sub1.getPhysicalPartition().getId().intValue(), 2233);
    Assert.assertEquals(sub1.getPhysicalPartition().getName(), "psource");
    Assert.assertEquals(sub1.getPhysicalSource().getUri(), "http://some.uri.com");
    Assert.assertEquals(sub1.getLogicalPartition().getId().intValue(), 1);
    Assert.assertEquals(sub1.getLogicalSource().getId().intValue(), 2);
    Assert.assertEquals(sub1.getLogicalSource().getName(), "source1");

    String sub1Json = sub1.toJsonString();
    DatabusSubscription sub1_new = DatabusSubscription.createFromJsonString(sub1Json);
    Assert.assertEquals(sub1, sub1_new);

    builder.getPhysicalPartition().makeAnyPartitionWildcard();
    DatabusSubscription sub2 = builder.build();
    String sub2Json = sub2.toJsonString();
    DatabusSubscription sub2_new = DatabusSubscription.createFromJsonString(sub2Json);
    Assert.assertTrue(sub2_new.getPhysicalPartition().isAnyPartitionWildcard());
  }

  @Test
  public void testDefaultCodec() throws Exception
  {
    //databus v2 conversion back and forth
    DatabusSubscription sub1 = DatabusSubscription.createSimpleSourceSubscription(new LogicalSource(1, "table1"));
    String sub1Str = DatabusSubscription.getDefaultCodec().encode(sub1).toString();
    Assert.assertEquals(sub1Str, "table1");

    DatabusSubscription sub2 = DatabusSubscription.createFromUri("table1");
    Assert.assertEquals(sub2, sub1);
  }
}
