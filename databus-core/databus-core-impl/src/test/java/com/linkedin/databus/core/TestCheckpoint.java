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


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.databus2.test.TestUtil;

public class TestCheckpoint
{

  @BeforeClass
  public void setUpClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }


  @Test
  public void testCheckpoint()
  {
      try
      {
        Checkpoint cp = new Checkpoint("{\"scn\":1234, \"scnMessageOffset\":34}");
        Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.INIT);
      }
      catch (JsonParseException e)
      {
        fail("Should not throw JSON parse exception");
      }
      catch (JsonMappingException e)
      {
        fail("Should not throw JSON parse exception");
      }
      catch (IOException e)
      {
        fail("Should not throw JSON parse exception");
      }

  }

  //@Test
  public void testSetScn()
  {
    fail("Not yet implemented");
  }

  //@Test
  public void testSetScnMessageOffset()
  {
    fail("Not yet implemented");
  }

  //@Test
  public void testGetScn()
  {
    fail("Not yet implemented");
  }

  //@Test
  public void testGetScnMessageOffset()
  {
    fail("Not yet implemented");
  }

  @Test
  public void testInit() {
    Checkpoint cp = new Checkpoint();
    assertEquals(cp.getInit(), true);
    cp.setWindowScn(1234L);
    cp.setWindowOffset(123);
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    assertEquals(cp.getInit(), false);
    cp.init();
    assertEquals(cp.getInit(), true);
  }

  @Test
  public void testSerialize() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = Checkpoint.createOnlineConsumptionCheckpoint(1234L);
    cp.setWindowOffset(5677);
    cp.setPrevScn(1233L);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    cp.serialize(baos);
    //System.out.println("Serialized String="+ baos.toString());
    Checkpoint newCp = new Checkpoint(baos.toString());
    Assert.assertTrue(newCp.assertCheckpoint());
    assertEquals(cp, newCp);
    Assert.assertEquals(cp.hashCode(), newCp.hashCode());
  }

  @Test
  public void testLargeOffsets() throws JsonParseException, JsonMappingException, IOException
  {
	    Checkpoint cp = new Checkpoint();
	    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
	    cp.setWindowScn(1234L);
	    cp.setWindowOffset(Long.MAX_VALUE-1);
	    cp.onSnapshotEvent(Long.MAX_VALUE-2);
	    cp.setBootstrapStartScn(100L);
	    cp.bootstrapCheckPoint();
	    String cpString = cp.toString();
	    Checkpoint cp2 = new Checkpoint(cpString);

    assertEquals("Checkpoint with large bootstrap offsets",cp, cp2);
  }

  @Test
  public void testCreateOnlineCheckpoints()
  {
    Checkpoint cp = Checkpoint.createFlexibleCheckpoint();
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.ONLINE_CONSUMPTION);
    Assert.assertTrue(cp.getFlexible());
    Assert.assertFalse(cp.equals(null)); //please don't crash
    Assert.assertEquals(cp, cp); //duh

    final long scn = 1L + Integer.MAX_VALUE;
    cp = Checkpoint.createOnlineConsumptionCheckpoint(scn);
    Assert.assertEquals(cp, cp); //duh
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.ONLINE_CONSUMPTION);
    Assert.assertEquals(cp.getWindowOffset(), Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    Assert.assertEquals(cp.getWindowScn(), scn);
    Assert.assertEquals(cp.getPrevScn(), scn);
    Assert.assertFalse(cp.getFlexible());
    Assert.assertEquals(cp.getTsNsecs(), Checkpoint.UNSET_TS_NSECS);
  }



  @Test
  public void testToFromString() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = Checkpoint.createFlexibleCheckpoint();
    Checkpoint newCp = new Checkpoint(cp.toString());
    Assert.assertTrue(newCp.assertCheckpoint());
    Assert.assertEquals(newCp, cp);
    Assert.assertEquals(cp.hashCode(), newCp.hashCode());

    cp = Checkpoint.createOnlineConsumptionCheckpoint(99999999999L);
    newCp = new Checkpoint(cp.toString());
    Assert.assertTrue(newCp.assertCheckpoint());
    Assert.assertEquals(newCp, cp);
    Assert.assertEquals(cp.hashCode(), newCp.hashCode());

    cp = Checkpoint.createOnlineConsumptionCheckpoint(1);
    newCp = new Checkpoint(cp.toString());
    Assert.assertTrue(newCp.assertCheckpoint());
    Assert.assertEquals(newCp, cp);
    Assert.assertEquals(cp.hashCode(), newCp.hashCode());

    cp = new Checkpoint();
    cp.setBootstrapSinceScn(0L);
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    cp.setBootstrapStartScn(1000L);
    cp.setBootstrapSnapshotSourceIndex(2);
    cp.setSnapshotOffset(0xABCDEF0123456L);
    cp.setBootstrapCatchupSourceIndex(0);
    cp.setWindowScn(123L);
    cp.setWindowOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
    Assert.assertEquals(cp, cp); //duh
    newCp = new Checkpoint(cp.toString());
    Assert.assertTrue(newCp.assertCheckpoint());
    Assert.assertEquals(newCp, cp);
    Assert.assertEquals(cp.hashCode(), newCp.hashCode());

    cp = new Checkpoint();
    cp.setBootstrapSinceScn(0L);
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    cp.setBootstrapSnapshotSourceIndex(1);
    cp.setBootstrapStartScn(1000L);
    cp.setBootstrapTargetScn(2000L);
    cp.setSnapshotOffset(-1);
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    cp.setBootstrapCatchupSourceIndex(1);
    cp.setWindowScn(123L);
    cp.setWindowOffset(0xABCDEFL);
    Assert.assertEquals(cp, cp); //duh
    newCp = new Checkpoint(cp.toString());
    Assert.assertTrue(newCp.assertCheckpoint());
    Assert.assertEquals(newCp, cp);
    Assert.assertEquals(cp.hashCode(), newCp.hashCode());
  }

  @Test
  public void testOnlineCheckpointAsserts() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint newCp = null;
    try
    {
       newCp = Checkpoint.createOnlineConsumptionCheckpoint(-1);
       Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      newCp = Checkpoint.createOnlineConsumptionCheckpoint(1000000L);
      newCp.setPrevScn(10L);
      newCp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
       newCp = new Checkpoint("{\"windowScn\":10, \"prevScn\": 5, \"windowOffset\":-1, \"consumption_mode\":\"ONLINE_CONSUMPTION\"}");
       newCp.assertCheckpoint();
       Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      newCp = Checkpoint.createOnlineConsumptionCheckpoint(1000000L);
      newCp.setWindowOffset(1000);
      newCp.setPrevScn(1000001L);
      newCp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      newCp = Checkpoint.createOnlineConsumptionCheckpoint(1000000L);
      newCp.setWindowOffset(-101);
      newCp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      newCp = Checkpoint.createFlexibleCheckpoint();
      newCp.setTsNsecs(1L);
      newCp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      // ok
    }
  }

  @Test
  public void testSnapshotCheckpointAsserts()
  {
    Checkpoint cp = null;

    try
    {
      cp = new Checkpoint();
      cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      cp.setBootstrapSinceScn(0L);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      cp.setBootstrapSnapshotSourceIndex(1);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      cp.setSnapshotOffset(-1);
      cp.setBootstrapCatchupSourceIndex(2);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      cp.setSnapshotOffset(1000);
      cp.setBootstrapCatchupSourceIndex(1);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }

    try
    {
      cp.setSnapshotOffset(1000);
      cp.setBootstrapCatchupSourceIndex(0);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok
    }
  }

  @Test
  public void testCatchupCheckpointAsserts() throws JsonParseException, JsonMappingException, IOException
  {
    final Logger log = Logger.getLogger("TestCheckpoint.testCatchupCheckpointAsserts");
    Checkpoint cp = null;

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\"}");
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok -- bootstrap_since_scn must be set
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0}");
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok -- bootstrap_start_scn must be set
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
                          "\"bootstrap_start_scn\":1000}");
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok -- bootstrap_target_scn must be set
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":900}");
      cp.setBootstrapTargetScn(900L);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok -- bootstrap_target_scn < bootstrap_start_scn
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000}");
      cp.setBootstrapTargetScn(2000L);
      cp.setBootstrapSnapshotSourceIndex(-1);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok - missing bootstrap_catchup_source_index
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1}");
      cp.setBootstrapCatchupSourceIndex(1);
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok -- bootstrap_catchup_snapshot index must be set
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
          "\"bootstrap_snapshot_source_index\":1, \"snapshot_offset\":1}");
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok - bootstrap_offset must be -1
      log.debug("exception thrown: " + e);
    }

    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
          "\"bootstrap_snapshot_source_index\":0}");
      cp.assertCheckpoint();
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //ok - bootstrap_catchup_source_index > bootstrap_catchup_snapshot index
      log.debug("exception thrown: " + e);
    }

    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1}");
    cp.assertCheckpoint();
  }

  @Test
  public void testV3Checkpoint() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = null;

    // Happy path, Add V3 bootstrap parameters for bootstrap SNAPSHOT checkpoint
    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_SNAPSHOT\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":0," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":1000}");
    cp.assertCheckpoint();

    // Happy path, Add V3 bootstrap parameters for bootstrap CATCHUP checkpoint
    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":1000}");
    cp.assertCheckpoint();

    // Unhappy path; V3 bootstrap parameter file_record_offset set, but not cluster name - SNAPSHOT
    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_SNAPSHOT\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":0," +
          "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"\","+
          "\"bootstrap_snapshot_file_record_offset\":1000}");
      cp.assertCheckpoint();
      Assert.fail("The above checkpoint " + cp + " is invalid");
    } catch (InvalidCheckpointException e)
    {
    }

    // Unhappy path; V3 bootstrap parameter file_record_offset set, but not cluster name - CATCHUP
    try
    {
      cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
          "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":2," +
          "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"\","+
          "\"bootstrap_snapshot_file_record_offset\":1000}");
      cp.assertCheckpoint();
      Assert.fail("The above checkpoint " + cp + " is invalid");
    } catch (InvalidCheckpointException e)
    {
    }
  }

  @Test
  public void testCopyBootstrapSnapshotCheckpoint() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = null;

    // Happy path, Add V3 bootstrap parameters for bootstrap SNAPSHOT checkpoint
    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_SNAPSHOT\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":0," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":1000}");
    cp.assertCheckpoint();

    Checkpoint cp2 = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_SNAPSHOT\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":0," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":2000}");

    cp2.copyBootstrapSnapshotCheckpoint(cp);
    Assert.assertEquals(cp2.getSnapshotFileRecordOffset(), cp.getSnapshotFileRecordOffset());
  }

  @Test
  public void testCopyBootstrapCatchupCheckpoint() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = null;

    // Happy path, Add V3 bootstrap parameters for bootstrap SNAPSHOT checkpoint
    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":1000}");
    cp.assertCheckpoint();

    Checkpoint cp2 = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":2000}");

    cp2.copyBootstrapCatchupCheckpoint(cp);
    Assert.assertEquals(cp2.getSnapshotFileRecordOffset(), cp.getSnapshotFileRecordOffset());

  }

  @Test
  public void testEqualsHashCodeForV3Bootstrap() throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = null;

    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":1000}");
    // Reflexive
    Assert.assertEquals(cp, cp);

    Checkpoint cp2 = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"testCluster\","+
        "\"bootstrap_snapshot_file_record_offset\":1000}");
    // Symmetric
    Assert.assertEquals(cp2, cp);

    Checkpoint cp3 = cp2.clone();
    Assert.assertEquals(cp3, cp2);
    Assert.assertEquals(cp, cp3);
  }

  @Test
  public void testEqualsHashCodeForV3BootstrapBackwardCompat()
  throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cp = null;

    cp = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1,"+"\"storage_cluster_name\":\"\","+
        "\"bootstrap_snapshot_file_record_offset\":-1}");

    Checkpoint cp2 = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
        "\"bootstrap_snapshot_source_index\":1}");
    Assert.assertTrue(cp2.equals(cp));

    // The new checkpoint with default values and the old checkpoint without these fields are different.
    // But because we are not using the V3 checkpoints for bootstrap this is ok.
    Assert.assertFalse(cp.equals(cp2));
  }

  // Test the case where a manual checkpoint is created with scn set to 0 but timestamp is non-zero.
  @Test
  public void testTimestampBasedCheckpoint() throws Exception
  {
    final long ts = 23534677L;
    Checkpoint cp = Checkpoint.createOnlineConsumptionCheckpoint(0L);
    cp.setTsNsecs(ts);

    Assert.assertTrue(cp.assertCheckpoint());

    String serCp = cp.toString();

    Checkpoint newCp = new Checkpoint(serCp);
    Assert.assertTrue(newCp.assertCheckpoint());
    Assert.assertEquals(newCp.getTsNsecs(), ts);
  }
}
