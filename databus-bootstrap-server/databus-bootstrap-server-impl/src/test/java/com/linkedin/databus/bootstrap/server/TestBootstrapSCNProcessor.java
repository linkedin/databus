package com.linkedin.databus.bootstrap.server;
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


import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;


public class TestBootstrapSCNProcessor
{
  @Test
  public void testShouldBypassSnapshot()
  throws SQLException, BootstrapProcessingException, NoSuchFieldException, IllegalAccessException,
         NoSuchMethodException
  {
    BootstrapSCNProcessor bsp = new BootstrapSCNProcessor();

    Long defaultRowsThresholdForSnapshotBypass = Long.MAX_VALUE;
    Map<String, Long> rowsThresholdForSnapshotBypass = new HashMap<String, Long>();
    Map<String, Boolean> disableSnapshotBypass = new HashMap<String, Boolean>();
    boolean predicatePushDown = false;
    Map<String, Boolean> predicatePushDownBypass = new HashMap<String, Boolean>();
    int queryTimeoutInSec = 10;
    BootstrapReadOnlyConfig db = null;
    boolean enableMinScnCheck=false;

    BootstrapServerStaticConfig bssc = new BootstrapServerStaticConfig(defaultRowsThresholdForSnapshotBypass, rowsThresholdForSnapshotBypass, disableSnapshotBypass,  predicatePushDown, predicatePushDownBypass, queryTimeoutInSec, enableMinScnCheck,db);

    Field field = bsp.getClass().getDeclaredField("_config");
    field.setAccessible(true);
    field.set(bsp, bssc);
    int srcId = 101;
    long sinceScn = 5;
    long startScn = 10;

    BootstrapDBMetaDataDAO bmdd = EasyMock.createMock(BootstrapDBMetaDataDAO.class);
    EasyMock.expect(bmdd.getLogIdToCatchup(srcId, startScn)).andReturn(0).anyTimes();
    EasyMock.expect(bmdd.getLogIdToCatchup(srcId, sinceScn)).andReturn(0).anyTimes();
    EasyMock.replay(bmdd);

    Field dbDaoField = bsp.getClass().getDeclaredField("_dbDao");
    dbDaoField.setAccessible(true);
    dbDaoField.set(bsp, bmdd);

    List<SourceStatusInfo> srcList = new ArrayList<SourceStatusInfo>();
    String name = "foo";
    SourceStatusInfo ssi = new SourceStatusInfo(name, srcId, 4);
    srcList.add(ssi);

    // case 1. Single source, defaultRowsThresholdForSnapshotBypass set to Long.MAX_VALUE,
    // individual overrides not set
    boolean sbs = bsp.shouldBypassSnapshot(sinceScn, startScn, srcList);
    Assert.assertEquals(true, sbs);

    // case 2. Single source, defaultRowsThresholdForSnapshotBypass set to finite value,
    // individual overrides set for the source
    rowsThresholdForSnapshotBypass.put(name, Long.MAX_VALUE);
    BootstrapServerStaticConfig bssc2 = new BootstrapServerStaticConfig(defaultRowsThresholdForSnapshotBypass, rowsThresholdForSnapshotBypass, disableSnapshotBypass,  predicatePushDown, predicatePushDownBypass, queryTimeoutInSec,enableMinScnCheck, db);
    field.set(bsp, bssc2);
    sbs = bsp.shouldBypassSnapshot(sinceScn, startScn, srcList);
    Assert.assertEquals(true, sbs);

    // Case 3:  Detect case when the log is not available on log tables ( so it should NOT bypass snapshot )
    BootstrapDBMetaDataDAO bmdd2 = EasyMock.createMock(BootstrapDBMetaDataDAO.class);
    EasyMock.expect(bmdd2.getLogIdToCatchup(srcId, startScn)).andReturn(2).anyTimes();
    EasyMock.expect(bmdd2.getLogIdToCatchup(srcId, sinceScn)).andThrow(new BootstrapProcessingException(""));
    EasyMock.replay(bmdd2);

    Field dbDaoField2 = bsp.getClass().getDeclaredField("_dbDao");
    dbDaoField2.setAccessible(true);
    dbDaoField2.set(bsp, bmdd2);
    sbs = bsp.shouldBypassSnapshot(sinceScn, startScn, srcList);
    Assert.assertEquals(false, sbs);
  }
}
