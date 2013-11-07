package com.linkedin.databus.bootstrap.test;
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


import java.io.IOException;
import java.sql.SQLException;

import com.linkedin.databus.bootstrap.server.BootstrapServerConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.server.BootstrapProcessor;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder;
import com.linkedin.databus2.core.filter.KeyModFilterConfig;
import com.linkedin.databus2.core.filter.KeyRangeFilterConfig;

public class TestFilterToSQL {


  public static final Logger LOG = Logger.getLogger("TestFilterToSQL");

  BootstrapProcessor processor;
  KeyFilterConfigHolder.Config partConf; 
  @BeforeClass
  public void setUp() throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
  {
    Logger.getRootLogger().removeAllAppenders();
    Appender defApp = new ConsoleAppender(new SimpleLayout());
    Logger.getRootLogger().addAppender(defApp);
    Logger.getRootLogger().setLevel(Level.INFO);

    BootstrapServerConfig configBuilder = new BootstrapServerConfig();
    BootstrapServerStaticConfig staticConfig = configBuilder.build();
    processor = new BootstrapProcessor(staticConfig, null);
    partConf =  new KeyFilterConfigHolder.Config();
  } 


  @Test
  public void testModFilter() throws InvalidConfigException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
  {


    partConf =  new KeyFilterConfigHolder.Config();
    partConf.setType("MOD");
    KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
    modConf.setNumBuckets(100);
    modConf.setBuckets("[0,3-4]");
    partConf.setMod(modConf);
    DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    processor.setKeyFilter(filter);
    LOG.info("Testing multiple mod filters");
    LOG.info("CATCHUP TABLE: " + processor.getCatchupSQLString("catchuptab"));
    LOG.info("SNAPSHOT TABLE: " + processor.getSnapshotSQLString("snapshotTable"));
    String catchUpString = processor.getCatchupSQLString("catchuptab").replaceAll("\\s+"," ");
    String snapshotString = processor.getSnapshotSQLString("snapshotTable").replaceAll("\\s+"," ");
    String catchUpExpectedString = "Select id, scn, windowscn, val, CAST(srckey as SIGNED) as srckey from catchuptab where  id > ?  and windowscn >= ? and windowscn <= ?  and windowscn >= ? AND  ( srckey%100 >= 0 AND srckey%100 < 1 OR srckey%100 >= 3 AND srckey%100 < 5 )  order by id limit ?".replaceAll("\\s+"," ");
    String snapshotExpectedString = "Select id, scn,  CAST(srckey as SIGNED) as srckey, val from snapshotTable where  id > ?  and scn < ?  and scn >= ? AND  ( srckey%100 >= 0 AND srckey%100 < 1 OR srckey%100 >= 3 AND srckey%100 < 5 )  order by id limit ?".replaceAll("\\s+"," ");
    Assert.assertEquals(catchUpString, catchUpExpectedString);
    Assert.assertEquals(snapshotString, snapshotExpectedString);

    partConf.setType("MOD");
    modConf = new KeyModFilterConfig.Config();
    modConf.setNumBuckets(100);
    modConf.setBuckets("[0]");
    partConf.setMod(modConf);
    filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    processor.setKeyFilter(filter);
    LOG.info("Testing single mod filter");
    LOG.info("CATCHUP TABLE: " + processor.getCatchupSQLString("catchuptab"));
    LOG.info("SNAPSHOT TABLE: " + processor.getSnapshotSQLString("snapshotTable"));
    catchUpString = processor.getCatchupSQLString("catchuptab").replaceAll("\\s+"," ");
    snapshotString = processor.getSnapshotSQLString("snapshotTable").replaceAll("\\s+"," ");
    catchUpExpectedString = "Select id, scn, windowscn, val, CAST(srckey as SIGNED) as srckey from catchuptab where  id > ?  and windowscn >= ? and windowscn <= ?  and windowscn >= ? AND  ( srckey%100 >= 0 AND srckey%100 < 1 )  order by id limit ?".replaceAll("\\s+"," ");
    snapshotExpectedString =  "Select id, scn,  CAST(srckey as SIGNED) as srckey, val from snapshotTable where  id > ?  and scn < ?  and scn >= ? AND  ( srckey%100 >= 0 AND srckey%100 < 1 )  order by id limit ?".replaceAll("\\s+"," ");
    Assert.assertEquals(catchUpString, catchUpExpectedString);
    Assert.assertEquals(snapshotString, snapshotExpectedString);
  }

  @Test
  public void testRangeFilter() throws InvalidConfigException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException
  {

    partConf =  new KeyFilterConfigHolder.Config();
    partConf.setType("RANGE");
    KeyRangeFilterConfig.Config rangeConf = new KeyRangeFilterConfig.Config();
    rangeConf.setSize(100);
    rangeConf.setPartitions("[0,3-4]");
    partConf.setRange(rangeConf);
    DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    processor.setKeyFilter(filter);
    LOG.info("Testing multiple range filters: ");
    LOG.info("CATCHUP TABLE: " + processor.getCatchupSQLString("catchuptab"));
    LOG.info("SNAPSHOT TABLE: " + processor.getSnapshotSQLString("snapshotTable"));
    String catchUpString = processor.getCatchupSQLString("catchuptab").replaceAll("\\s+"," ");
    String snapshotString = processor.getSnapshotSQLString("snapshotTable").replaceAll("\\s+"," ");
    String catchUpExpectedString = "Select id, scn, windowscn, val, CAST(srckey as SIGNED) as srckey from catchuptab where  id > ?  and windowscn >= ? and windowscn <= ?  and windowscn >= ? AND  ( srckey >= 0 AND srckey < 100 OR srckey >= 300 AND srckey < 500 )  order by id limit ?".replaceAll("\\s+"," ");
    String snapshotExpectedString = "Select id, scn,  CAST(srckey as SIGNED) as srckey, val from snapshotTable where  id > ?  and scn < ?  and scn >= ? AND  ( srckey >= 0 AND srckey < 100 OR srckey >= 300 AND srckey < 500 )  order by id limit ?".replaceAll("\\s+"," ");
    Assert.assertEquals(catchUpString, catchUpExpectedString);
    Assert.assertEquals(snapshotString, snapshotExpectedString);


    partConf =  new KeyFilterConfigHolder.Config();
    partConf.setType("RANGE");
    rangeConf = new KeyRangeFilterConfig.Config();
    rangeConf.setSize(100);
    rangeConf.setPartitions("[0]");
    partConf.setRange(rangeConf);
    filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    processor.setKeyFilter(filter);
    LOG.info("Testing single range filter");
    LOG.info("CATCHUP TABLE: " + processor.getCatchupSQLString("catchuptab"));
    LOG.info("SNAPSHOT TABLE: " + processor.getSnapshotSQLString("snapshotTable"));
    catchUpString = processor.getCatchupSQLString("catchuptab").replaceAll("\\s+"," ");
    snapshotString = processor.getSnapshotSQLString("snapshotTable").replaceAll("\\s+"," ");
    catchUpExpectedString = "Select id, scn, windowscn, val, CAST(srckey as SIGNED) as srckey from catchuptab where  id > ?  and windowscn >= ? and windowscn <= ?  and windowscn >= ? AND  ( srckey >= 0 AND srckey < 100 )  order by id limit ?".replaceAll("\\s+"," ");
    snapshotExpectedString = "Select id, scn,  CAST(srckey as SIGNED) as srckey, val from snapshotTable where  id > ?  and scn < ?  and scn >= ? AND  ( srckey >= 0 AND srckey < 100 )  order by id limit ?".replaceAll("\\s+"," ");
    Assert.assertEquals(catchUpString, catchUpExpectedString);
    Assert.assertEquals(snapshotString, snapshotExpectedString);
  }  

  @Test
  public void testNullFilter() throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
  {
    processor.setKeyFilter(null);
    LOG.info("Testing null filter ");
    LOG.info("CATCHUP TABLE: " + processor.getCatchupSQLString("catchuptab"));
    LOG.info("SNAPSHOT TABLE: " + processor.getSnapshotSQLString("snapshotTable"));
    String catchUpString = processor.getCatchupSQLString("catchuptab").replaceAll("\\s+"," ");
    String snapshotString = processor.getSnapshotSQLString("snapshotTable").replaceAll("\\s+"," ");
    String catchUpExpectedString = "Select id, scn, windowscn, val from catchuptab where  id > ?  and windowscn >= ? and windowscn <= ?  and windowscn >= ?  order by id limit ?".replaceAll("\\s+"," ");
    String snapshotExpectedString = "Select id, scn, srckey, val from snapshotTable where  id > ?  and scn < ?  and scn >= ?  order by id limit ?".replaceAll("\\s+"," ");
    Assert.assertEquals(catchUpString, catchUpExpectedString);
    Assert.assertEquals(snapshotString, snapshotExpectedString);
  }


  @Test
  public void testNonePartition() throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
  {
    partConf.setType("NONE");
    DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    processor.setKeyFilter(filter);
    LOG.info(" Testing no parition filter (None parition");
    LOG.info("CATCHUP TABLE: " + processor.getCatchupSQLString("catchuptab"));
    LOG.info("SNAPSHOT TABLE: " + processor.getSnapshotSQLString("snapshotTable"));
    String catchUpString = processor.getCatchupSQLString("catchuptab").replaceAll("\\s+"," ");
    String snapshotString = processor.getSnapshotSQLString("snapshotTable").replaceAll("\\s+"," ");
    String catchUpExpectedString = "Select id, scn, windowscn, val from catchuptab where  id > ?  and windowscn >= ? and windowscn <= ?  and windowscn >= ?  order by id limit ?".replaceAll("\\s+"," ");
    String snapshotExpectedString = "Select id, scn, srckey, val from snapshotTable where  id > ?  and scn < ?  and scn >= ?  order by id limit ?".replaceAll("\\s+"," ");
    Assert.assertEquals(catchUpString, catchUpExpectedString);
    Assert.assertEquals(snapshotString, snapshotExpectedString);
  }



}
