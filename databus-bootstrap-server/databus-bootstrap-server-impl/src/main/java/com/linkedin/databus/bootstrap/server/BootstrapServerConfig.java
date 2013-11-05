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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class BootstrapServerConfig implements
    ConfigBuilder<BootstrapServerStaticConfig>
{
  // BypassSnapshot disabled by default
  public static final long DEFAULT_DEFAULT_THRESHOLD_FOR_SNAPSHOT_BYPASS = -1;

  // Predicate push down default
  public static final boolean DEFAULT_PREDICATEPUSHDOWN = true;

  // MinScn check enabled/disabled - See DDSDBUS-1629
  public static final boolean DEFAULT_MINSCNCHECK = true;

  // Default timeout in sec for bootstrap DB query execution;
  public static final int DEFAULT_BOOTSTRAP_DB_QUERY_EXECUTION_TIMEOUT_IN_SEC = 3600;

  // if the number of events between sinceSCN and start SCN is less than this
  // threshold, then snapshot could be disabled.
  private Long defaultRowsThresholdForSnapshotBypass = DEFAULT_DEFAULT_THRESHOLD_FOR_SNAPSHOT_BYPASS;

  private int queryTimeoutInSec = DEFAULT_BOOTSTRAP_DB_QUERY_EXECUTION_TIMEOUT_IN_SEC;

  // Per Source num-rows threshold overrides
  // if the number of events between sinceSCN and start SCN is less than this
  // threshold, then snapshot could be disabled.
  private final Map<String, Long> rowsThresholdForSnapshotBypass;

  // Selectively disable the snapshotBypass feature
  private final Map<String, Boolean> disableSnapshotBypass;
  // Source level override for predicate pushdown
  private final Map<String, Boolean> predicatePushDownBypass;

  // Bootstrap DB Config
  private BootstrapConfig db;

  // Predicate push down to sql level -- disables server side filtering
  private boolean predicatePushDown;

  // MinScn check enabled/disabled - See DDSDBUS-1629
  private boolean enableMinScnCheck = DEFAULT_MINSCNCHECK;

  public boolean getPredicatePushDown()
  {
    return predicatePushDown;
  }

  public void setPredicatePushDown(boolean predicatePushDown)
  {
    this.predicatePushDown = predicatePushDown;
  }

  @Override
  public BootstrapServerStaticConfig build() throws InvalidConfigException
  {
    return new BootstrapServerStaticConfig(
        defaultRowsThresholdForSnapshotBypass, rowsThresholdForSnapshotBypass,
        disableSnapshotBypass, predicatePushDown, predicatePushDownBypass,
        queryTimeoutInSec,enableMinScnCheck, db.build());
  }

  public Long getDefaultRowsThresholdForSnapshotBypass()
  {
    return defaultRowsThresholdForSnapshotBypass;
  }

  public void setDefaultRowsThresholdForSnapshotBypass(
      Long defaultRowsThresholdForSnapshotBypass)
  {
    this.defaultRowsThresholdForSnapshotBypass = defaultRowsThresholdForSnapshotBypass;
  }

  public Long getRowsThresholdForSnapshotBypass(String source)
  {
    Long threshold = rowsThresholdForSnapshotBypass.get(source);

    if (null == threshold)
      return defaultRowsThresholdForSnapshotBypass;

    return threshold;
  }

  public void setRowsThresholdForSnapshotBypass(String source, Long threshold)
  {
    rowsThresholdForSnapshotBypass.put(source, threshold);
  }

  public int getQueryTimeoutInSec()
  {
    return queryTimeoutInSec;
  }

  public void setQueryTimeoutInSec(int queryTimeInSec)
  {
    queryTimeoutInSec = queryTimeInSec;
  }

  public Boolean getDisableSnapshotBypass(String source)
  {
    Boolean disableBypass = disableSnapshotBypass.get(source);

    if (null == disableBypass)
      return false;

    return disableBypass;
  }

  public void setDisableSnapshotBypass(String source, Boolean disableBypass)
  {
    disableSnapshotBypass.put(source, disableBypass);
  }

  public boolean getPredicatePushDownBypass(String source)
  {
    Boolean predicateOverride = predicatePushDownBypass.get(source);
    if (predicateOverride == null)
      return DEFAULT_PREDICATEPUSHDOWN;
    return predicateOverride;
  }

  public void setPredicatePushDownBypass(String source, boolean override)
  {
    predicatePushDownBypass.put(source, override);
  }

  public boolean isEnableMinScnCheck()
  {
    return enableMinScnCheck;
  }

  public void setEnableMinScnCheck(boolean enableMinScnCheck)
  {
    this.enableMinScnCheck = enableMinScnCheck;
  }

  public BootstrapConfig getDb()
  {
    return db;
  }

  public void setDb(BootstrapConfig db)
  {
    this.db = db;
  }

  public BootstrapServerConfig() throws IOException
  {
    super();
    defaultRowsThresholdForSnapshotBypass = DEFAULT_DEFAULT_THRESHOLD_FOR_SNAPSHOT_BYPASS;
    disableSnapshotBypass = new HashMap<String, Boolean>();
    rowsThresholdForSnapshotBypass = new HashMap<String, Long>();
    predicatePushDown = DEFAULT_PREDICATEPUSHDOWN;
    predicatePushDownBypass = new HashMap<String, Boolean>();
    db = new BootstrapConfig();
  }
}
