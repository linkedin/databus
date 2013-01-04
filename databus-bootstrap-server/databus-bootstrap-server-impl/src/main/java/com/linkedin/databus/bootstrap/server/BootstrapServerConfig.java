package com.linkedin.databus.bootstrap.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class BootstrapServerConfig implements ConfigBuilder<BootstrapServerStaticConfig>
{
	  //BypassSnapshot disabled by default
	  public static final long DEFAULT_DEFAULT_THRESHOLD_FOR_SNAPSHOT_BYPASS = -1;

	  //Predicate push down default
	  public static final boolean DEFAULT_PREDICATEPUSHDOWN = true;
	  
	  // if the number of events between sinceSCN and start SCN is less than this threshold, then snapshot could be disabled.
	  private Long defaultRowsThresholdForSnapshotBypass = DEFAULT_DEFAULT_THRESHOLD_FOR_SNAPSHOT_BYPASS;

	  // Per Source num-rows threshold overrides 
	  // if the number of events between sinceSCN and start SCN is less than this threshold, then snapshot could be disabled.
	  private Map<String, Long> rowsThresholdForSnapshotBypass;

	  //Selectively disable the snapshotBypass feature
	  private Map<String, Boolean> disableSnapshotBypass;

	  //Bootstrap DB Config
	  private BootstrapConfig db;

	  //Predicate push down to sql level -- disables server side filtering 
	  private boolean predicatePushDown;
	  
  	  public boolean getPredicatePushDown()
      {
        return predicatePushDown;
      }
  
      public void setPredicatePushDown(boolean predicatePushDown)
      {
        this.predicatePushDown = predicatePushDown;
      }

    @Override
	  public BootstrapServerStaticConfig build() throws InvalidConfigException {
		  return new BootstrapServerStaticConfig(defaultRowsThresholdForSnapshotBypass, rowsThresholdForSnapshotBypass, disableSnapshotBypass, predicatePushDown, db.build());
	  }

	  public Long getDefaultRowsThresholdForSnapshotBypass() {
		  return defaultRowsThresholdForSnapshotBypass;
	  }

	  public void setDefaultRowsThresholdForSnapshotBypass(
			  Long defaultRowsThresholdForSnapshotBypass) {
		  this.defaultRowsThresholdForSnapshotBypass = defaultRowsThresholdForSnapshotBypass;
	  }

	  public Long getRowsThresholdForSnapshotBypass(String source) {
		  Long threshold = rowsThresholdForSnapshotBypass.get(source);
		  
		  if (null == threshold)
			  return defaultRowsThresholdForSnapshotBypass;
		  
		  return threshold;
	  }
	  
	  public void setRowsThresholdForSnapshotBypass(
			  String source, Long threshold) {
		  rowsThresholdForSnapshotBypass.put(source, threshold);
	  }

	  public Boolean getDisableSnapshotBypass(String source) {
		  Boolean disableBypass = disableSnapshotBypass.get(source);
		  
		  if ( null == disableBypass)
			  return false;
		  
		  return disableBypass;
	  }

	  public void setDisableSnapshotBypass(String source, Boolean disableBypass) {
		  disableSnapshotBypass.put(source,disableBypass);
	  }

	  public BootstrapConfig getDb() {
		  return db;
	  }

	  public void setDb(BootstrapConfig db) {
		  this.db = db;
	  }

	  public BootstrapServerConfig()  throws IOException
	  {
		super();
		defaultRowsThresholdForSnapshotBypass = DEFAULT_DEFAULT_THRESHOLD_FOR_SNAPSHOT_BYPASS;
		disableSnapshotBypass = new HashMap<String, Boolean>();
		rowsThresholdForSnapshotBypass = new HashMap<String, Long>();
		predicatePushDown = DEFAULT_PREDICATEPUSHDOWN;
		db = new BootstrapConfig();
	  }	  
}

