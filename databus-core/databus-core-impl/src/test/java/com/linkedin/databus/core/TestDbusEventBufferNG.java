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


import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;

/** Test NG tests for {@link DbusEventBuffer}*/
public class TestDbusEventBufferNG
{

  @Test
  public void testSameBufferTraceOn() throws IOException, InvalidConfigException
  {
    File buf1Trace = null;
    File buf2Trace = null;
    File buf3Trace = null;

    try
    {
      File tmpFile = File.createTempFile("TestDbusEventBufferNG-buffer", ".trace");
      Assert.assertTrue(tmpFile.exists()); //make sure the file was created, i.e. we have write permissions
      Assert.assertTrue(tmpFile.delete()); //we just need the name

      DbusEventBuffer.Config cfgBuilder = new DbusEventBuffer.Config();
      cfgBuilder.setMaxSize(10000);
      cfgBuilder.setScnIndexSize(1024);
      cfgBuilder.setReadBufferSize(1024);
      cfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
      cfgBuilder.getTrace().setOption(RelayEventTraceOption.Option.file.toString());
      cfgBuilder.getTrace().setFilename(tmpFile.getAbsolutePath());
      cfgBuilder.getTrace().setNeedFileSuffix(true);

      //test that the first gets created
      PhysicalPartition pp = new PhysicalPartition(1, "TestPart");
      DbusEventBuffer buf1 = new DbusEventBuffer(cfgBuilder.build(), pp);
      buf1Trace = new File(tmpFile.getAbsolutePath() + "." + pp.getName() + "_" + pp.getId());
      Assert.assertTrue(buf1Trace.exists());

      buf1.start(1);
      for (int i = 1; i <= 10; ++i)
      {
        buf1.startEvents();
        buf1.appendEvent(new DbusEventKey(i), (short)0, (short)0, 0L, (short)0, new byte[16],
                         new byte[10], false, null);
        buf1.endEvents(10 * i);
      }

      long trace1len = buf1Trace.length();
      Assert.assertTrue(trace1len > 0);

      //create the second buffer and check its trace is different
      DbusEventBuffer buf2 = new DbusEventBuffer(cfgBuilder.build(), pp);
      buf2Trace = new File(tmpFile.getAbsolutePath() + "." + pp.getName() + "_" + pp.getId() + ".1");
      Assert.assertTrue(buf2Trace.exists());

      buf2.start(1);
      for (int i = 1; i <= 5; ++i)
      {
        buf2.startEvents();
        buf2.appendEvent(new DbusEventKey(i), (short)0, (short)0, 0L, (short)0, new byte[16],
                         new byte[10], false, null);
        buf2.endEvents(10 * i);
      }

      long trace2len = buf2Trace.length();
      Assert.assertTrue(trace2len > 0);
      Assert.assertTrue(buf1Trace.length() == trace1len);

      //create a third buffer and check its trace is different
      DbusEventBuffer buf3 = new DbusEventBuffer(cfgBuilder.build(), pp);
      buf3Trace = new File(tmpFile.getAbsolutePath() + "." + pp.getName() + "_" + pp.getId() + ".2");
      Assert.assertTrue(buf3Trace.exists());

      buf3.start(1);
      for (int i = 1; i <= 6; ++i)
      {
        buf3.startEvents();
        buf3.appendEvent(new DbusEventKey(i), (short)0, (short)0, 0L, (short)0, new byte[16],
                         new byte[10], false, null);
        buf3.endEvents(10 * i);
      }

      long trace3len = buf3Trace.length();
      Assert.assertTrue(trace3len > 0);
      Assert.assertTrue(buf1Trace.length() == trace1len);
      Assert.assertTrue(buf2Trace.length() == trace2len);
    }
    finally
    {
      Assert.assertTrue(null == buf1Trace || buf1Trace.delete());
      Assert.assertTrue(null == buf2Trace || buf2Trace.delete());
      Assert.assertTrue(null == buf3Trace || buf3Trace.delete());
    }
  }

}
