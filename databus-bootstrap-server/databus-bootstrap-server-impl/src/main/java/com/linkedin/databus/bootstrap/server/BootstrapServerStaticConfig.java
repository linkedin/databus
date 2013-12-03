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


import java.util.Map;

import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;

public class BootstrapServerStaticConfig
{
    // Margin time for longest transaction.
    // When a bootstrap server needs to filter events by time, it has the timestamp T1 of the last window (say, SCN 'S1')
    // received on the client. However, it is possible that there are older events in a newer window, and vice versa. So
    // the bootstrap server streams events starting from some old timestamp T2. The server chooses T2 such that T2 < T1
    // and there are no events that meet the following criteria
    //   a. event has timestamp T < T2
    //   b. event has SCN S > S1
    // T2 is computed as (T1 - _longestDbTxnTime)
    // Default value of _longestDbTxnTimeHrs is 4 hours.
    private final long _longestDbTxnTimeMins;
	  // if the number of events between sinceSCN and start SCN is less than this threshold, then snapshot could be disabled.
	  private final Long defaultRowsThresholdForSnapshotBypass;

	  // Per Source num-rows threshold overrides
	  // if the number of events between sinceSCN and start SCN is less than this threshold, then snapshot could be disabled.
	  private final Map<String, Long> rowsThresholdForSnapshotBypass;

	  //Selectively disable the snapshotBypass feature
	  private final Map<String, Boolean> disableSnapshotBypass;

	  //Bootstrap DB Config
	  private final BootstrapReadOnlyConfig db;

	  //Predicate push down
	  private final boolean predicatePushDown;
    //Source level override for predicate pushdown
    private final Map<String, Boolean> predicatePushDownBypass;

    //Max query timeout in sec
    private final int queryTimeoutInSec;

    //Enable minScn query
    private final boolean enableMinScnCheck;

    public BootstrapServerStaticConfig(Long defaultRowsThresholdForSnapshotBypass,
                                       Map<String, Long> rowsThresholdForSnapshotBypass,
                                       Map<String, Boolean> disableSnapshotBypass,
                                       boolean predicatePushDown,
                                       Map<String, Boolean> predicatePushDownBypass,
                                       int queryTimeoutInSec,
                                       boolean enableMinScnCheck,
                                       BootstrapReadOnlyConfig db,
                                       long longestDbTxnTimeMins)
    {
		  super();
		  this.defaultRowsThresholdForSnapshotBypass = defaultRowsThresholdForSnapshotBypass;
		  this.rowsThresholdForSnapshotBypass = rowsThresholdForSnapshotBypass;
		  this.disableSnapshotBypass = disableSnapshotBypass;
		  this.predicatePushDown = predicatePushDown;
          this.predicatePushDownBypass = predicatePushDownBypass;
          this.queryTimeoutInSec = queryTimeoutInSec;
          this.enableMinScnCheck = enableMinScnCheck;
		  this.db = db;
      this._longestDbTxnTimeMins = longestDbTxnTimeMins;
	  }

	  @Override
	  public String toString() {
		  return "BootstrapStaticConfig [defaultRowsThresholdForSnapshotBypass="
				  + defaultRowsThresholdForSnapshotBypass
				  + ", rowsThresholdForSnapshotBypass="
				  + rowsThresholdForSnapshotBypass + ", disableSnapshotBypass="
				  + disableSnapshotBypass + " , queryTimeoutInSec="
				  + queryTimeoutInSec
				  + " predicatePushDown= " + predicatePushDown
				  + " enableMinScnCheck= " + enableMinScnCheck
				  + ", db=" + db + "]";
	  }


      public boolean getPredicatePushDown()
      {
        return predicatePushDown;
      }

      public Map<String, Boolean> predicatePushDownBypass()
      {
           return predicatePushDownBypass;
      }

	  public Long getDefaultRowsThresholdForSnapshotBypass() {
		  return defaultRowsThresholdForSnapshotBypass;
	  }

    public long getLongestDbTxnTimeMins()
    {
      return _longestDbTxnTimeMins;
    }

	  public int getQueryTimeoutInSec()
	  {
	      return queryTimeoutInSec;
	  }

	  public Map<String, Long> getRowsThresholdForSnapshotBypass() {
		  return rowsThresholdForSnapshotBypass;
	  }

	  public Map<String, Boolean> getDisableSnapshotBypass() {
		  return disableSnapshotBypass;
	  }

	  public BootstrapReadOnlyConfig getDb() {
		  return db;
	  }

	  public boolean isBypassSnapshotDisabled(String source)
	  {
		  Boolean byPass = disableSnapshotBypass.get(source);

		  if ( null == byPass)
			  return false;

		  return byPass;
	  }

      public boolean isPredicatePushDownEnabled(String source)
      {
          if(source == null)
              return getPredicatePushDown();
          Boolean predicateOverride = predicatePushDownBypass.get(source);
          if(predicateOverride == null)
              return getPredicatePushDown();

          return predicateOverride;
      }

    public boolean isEnableMinScnCheck()
    {
      return enableMinScnCheck;
    }

	  public long getRowsThresholdForSnapshotBypass(String source)
	  {
		  long threshold = defaultRowsThresholdForSnapshotBypass;

		  Long t = rowsThresholdForSnapshotBypass.get(source);

		  if ( null != t)
			  threshold = t;

		  return threshold;
	  }

}

