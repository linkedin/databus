package com.linkedin.databus.bootstrap.server;

import java.util.Map;

import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;

public class BootstrapServerStaticConfig
{
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


    public BootstrapServerStaticConfig(Long defaultRowsThresholdForSnapshotBypass,
			  Map<String, Long> rowsThresholdForSnapshotBypass,
			  Map<String, Boolean> disableSnapshotBypass,
			  boolean predicatePushDown,
			  BootstrapReadOnlyConfig db) {
		  super();
		  this.defaultRowsThresholdForSnapshotBypass = defaultRowsThresholdForSnapshotBypass;
		  this.rowsThresholdForSnapshotBypass = rowsThresholdForSnapshotBypass;
		  this.disableSnapshotBypass = disableSnapshotBypass;
		  this.predicatePushDown = predicatePushDown;
		  this.db = db;
	  }

	  @Override
	  public String toString() {
		  return "BootstrapStaticConfig [defaultRowsThresholdForSnapshotBypass="
				  + defaultRowsThresholdForSnapshotBypass
				  + ", rowsThresholdForSnapshotBypass="
				  + rowsThresholdForSnapshotBypass + ", disableSnapshotBypass="
				  + disableSnapshotBypass + ", db=" + db + "]";
	  }


      public boolean getPredicatePushDown()
      {
        return predicatePushDown;
      }
	  
	  public Long getDefaultRowsThresholdForSnapshotBypass() {
		  return defaultRowsThresholdForSnapshotBypass;
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
	  
	  public long getRowsThresholdForSnapshotBypass(String source)
	  {
		  long threshold = defaultRowsThresholdForSnapshotBypass;
		  
		  Long t = rowsThresholdForSnapshotBypass.get(source);
		  
		  if ( null != t)
			  threshold = t;
		  
		  return threshold;
	  }
	  
}

