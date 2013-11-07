package com.linkedin.databus2.relay.config;


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
import java.io.FileWriter;
import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.TestUtil;

public class TestDatabusRelaySourcesInFile
{

  static
  {
     TestUtil.setupLogging(true, "./TestDatabusRelaySourcesInFile.log", Level.WARN);
  }

  PhysicalSourceConfig createPhysicalSourceConfig(String pSourceName, int startLogicalId,int numSources)
  {
    PhysicalSourceConfig pconfig = new PhysicalSourceConfig();
    pconfig.setName(pSourceName);
    pconfig.setUri("http://"+pSourceName);
    ArrayList<LogicalSourceConfig> newSources = new ArrayList<LogicalSourceConfig>();
    for (int i=0; i < numSources;++i)
    {
      short lid = (short) (i+startLogicalId);
      LogicalSourceConfig tab = new LogicalSourceConfig();
      tab.setName("ls"+lid);
      tab.setId(lid);
      tab.setUri("tab_"+  lid);
      newSources.add(tab);
    }
    pconfig.setSources(newSources);
    return pconfig;
  }

  void checkIfEqual(PhysicalSourceConfig p1, PhysicalSourceConfig p2)
  {
      Assert.assertNotNull(p1);
      Assert.assertNotNull(p2);
      String p1String = p1.toString();
      String p2String = p2.toString();
      Assert.assertEquals(p1String,p2String);
  }

  @Test
  public void testSourceInfoSaveLoad() throws Exception
  {
      //create a physcial source config object
      int numConfigs = 3;
      String dir = ".";
      PhysicalSourceConfig[] pConfigs = new PhysicalSourceConfig[numConfigs];
      DatabusRelaySourcesInFiles databusSources = new DatabusRelaySourcesInFiles(dir);
      String[] pNames = new String[numConfigs];
      for (int i=0; i < numConfigs;++i)
      {
        String pName = "physical"+i;
        pNames[i] = pName;
        pConfigs[i] = createPhysicalSourceConfig(pName, 100*(i+1), i+1);
        Assert.assertTrue(databusSources.add(pName,pConfigs[i]));
      }
      Assert.assertTrue(databusSources.save());

      DatabusRelaySourcesInFiles readSources = new DatabusRelaySourcesInFiles(dir);
      Assert.assertTrue(readSources.load());
      PhysicalSourceConfig[] readConfigs = readSources.getAll();
      Assert.assertEquals(numConfigs,readConfigs.length);
      //now compare configs; what was written is what was read
      for (String pSrcName: pNames)
      {
          PhysicalSourceConfig p1 = databusSources.get(pSrcName);
          PhysicalSourceConfig p2 = readSources.get(pSrcName);
          checkIfEqual(p1,p2);
      }

      //remove all entries;
      databusSources.removePersistedEntries();

      //read empty dir
      Assert.assertTrue(readSources.load());
      PhysicalSourceConfig[] pNoConfigs=readSources.getAll();
      Assert.assertEquals(0,pNoConfigs.length);
      Assert.assertEquals(0,readSources.size());
  }

  @Test
  public void testLoadWithBadFiles() throws Exception
  {
    int numConfigs = 1;
    String dir = ".";
    PhysicalSourceConfig[] pConfigs = new PhysicalSourceConfig[numConfigs];
    DatabusRelaySourcesInFiles databusSources = new DatabusRelaySourcesInFiles(dir);
    String[] pNames = new String[numConfigs];
    for (int i=0; i < numConfigs;++i)
    {
      String pName = "physical"+i;
      pNames[i] = pName;
      pConfigs[i] = createPhysicalSourceConfig(pName, 100*(i+1), i+1);
      Assert.assertTrue(databusSources.add(pName,pConfigs[i]));
    }
    //save 1 legal json file
    Assert.assertTrue(databusSources.save());
    //now add bad jsob files
    String badJson = dir+"/bad.json";
    String emptyJson = dir+"/empty.json";

    File fBad = new File(badJson);
    fBad.createNewFile();
    File fEmpty= new File(emptyJson);
    fEmpty.createNewFile();
    FileWriter w1 = new FileWriter(fBad);
    w1.append("This is bad json");
    w1.close();

    DatabusRelaySourcesInFiles readSources = new DatabusRelaySourcesInFiles(dir);

    boolean t = readSources.load();

    //delete old files after reading it
    fBad.delete();
    readSources.removePersistedEntries();
    fEmpty.delete();

    Assert.assertTrue(t);
    PhysicalSourceConfig[] pNoConfigs=readSources.getAll();
    Assert.assertEquals(1,pNoConfigs.length);
    Assert.assertEquals(1,readSources.size());

  }

}
