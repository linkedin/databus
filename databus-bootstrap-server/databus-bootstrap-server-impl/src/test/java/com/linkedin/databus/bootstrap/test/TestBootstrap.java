package com.linkedin.databus.bootstrap.test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.server.BootstrapProcessor;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.util.DBHelper;

public class TestBootstrap 
{
	  @Test
	  public void testBootstrapProcessor() 
	    throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, 
	    IOException, BootstrapProcessingException, InvalidConfigException, BootstrapDatabaseTooOldException 
	    {    
		  EventProcessor processorCallback = new EventProcessor();
		  BootstrapConfig config = new BootstrapConfig();
		  BootstrapReadOnlyConfig staticConfig = config.build();

		  BootstrapProcessor processor = new BootstrapProcessor(staticConfig, null);
		  String sourceName = "events";

		  // Create the tables for all the sources before starting up the threads
		  
		  BootstrapConn _bootstrapConn = new BootstrapConn();
		  _bootstrapConn.initBootstrapConn(false,
				  staticConfig.getBootstrapDBUsername(),
				  staticConfig.getBootstrapDBPassword(),
				  staticConfig.getBootstrapDBHostname(),
				  staticConfig.getBootstrapDBName());
		  
		  BootstrapDBMetaDataDAO dao = new BootstrapDBMetaDataDAO(_bootstrapConn,
	        									staticConfig.getBootstrapDBHostname(),
	        									staticConfig.getBootstrapDBUsername(),
	        									staticConfig.getBootstrapDBPassword(),
	        									staticConfig.getBootstrapDBName(),
	        									false);
		  SourceStatusInfo srcStatusInfo = dao.getSrcIdStatusFromDB(sourceName, false);
		  
		  if (srcStatusInfo.getSrcId() >= 0 )
		  {
			  dao.dropSourceInDB(srcStatusInfo.getSrcId());
		  }
		  
		  dao.addNewSourceInDB(sourceName,BootstrapProducerStatus.ACTIVE);
		  setBootstrapLoginfoTable(_bootstrapConn);    
		  _bootstrapConn.getDBConn().commit();

		  Checkpoint c = new Checkpoint();

		  c.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  c.setSnapshotSource(sourceName);
		  c.setSnapshotOffset(9999985);
		  c.setBootstrapStartScn((long) 9999999);
		  c.setBootstrapSinceScn(Long.valueOf(12345));
		  // System.out.println("Snapshot");
		  processor.streamSnapShotRows(c, processorCallback);


		  c.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  c.setCatchupSource(sourceName);
		  c.setWindowScn(0L);
		  c.setWindowOffset(0);
		  c.setBootstrapStartScn((long) 0);
		  c.setBootstrapTargetScn((long) 0);

		  // System.out.println("Catchup");
		  boolean phaseCompleted = processor.streamCatchupRows(c, processorCallback);
		  Assert.assertEquals( true, phaseCompleted);
	    }
		
	  @Test
	  public void testBootstrapService() 
	              throws InstantiationException, IllegalAccessException, ClassNotFoundException, 
	                     SQLException, IOException, BootstrapProcessingException, InvalidConfigException, BootstrapDatabaseTooOldException
	                     {
		  EventProcessor processorCallback = new EventProcessor();
		  BootstrapConfig config = new BootstrapConfig();
		  BootstrapReadOnlyConfig staticConfig = config.build();


		  String sources[] = new String[4];
		  sources[0] = "event";
		  sources[1] = "event1";
		  sources[2] = "event2";
		  sources[3] = "event3";

		  // Create the tables for all the sources before starting up the threads
		  BootstrapConn _bootstrapConn = new BootstrapConn();
		  _bootstrapConn.initBootstrapConn(false,
				  staticConfig.getBootstrapDBUsername(),
				  staticConfig.getBootstrapDBPassword(),
				  staticConfig.getBootstrapDBHostname(),
				  staticConfig.getBootstrapDBName());
		  
		  BootstrapDBMetaDataDAO dao = new BootstrapDBMetaDataDAO(_bootstrapConn,        		
				  								staticConfig.getBootstrapDBHostname(),
				  								staticConfig.getBootstrapDBUsername(),
				  								staticConfig.getBootstrapDBPassword(),
				  								staticConfig.getBootstrapDBName(),
				  								false);

		  for (String source : sources)
		  {
			  SourceStatusInfo srcStatusInfo = dao.getSrcIdStatusFromDB(source, false);
			  
			  if (srcStatusInfo.getSrcId() >= 0 )
			  {
				  dao.dropSourceInDB(srcStatusInfo.getSrcId());
			  }
			  
			  dao.addNewSourceInDB(source,BootstrapProducerStatus.ACTIVE);      
		  }

		  setBootstrapLoginfoTable(_bootstrapConn);

		  _bootstrapConn.getDBConn().commit();

		  DatabusBootstrapClient s = new DatabusBootstrapClient(sources);
		  Checkpoint cp;
		  while((cp = s.getNextBatch(10, processorCallback)).getConsumptionMode() != DbusClientMode.ONLINE_CONSUMPTION)
		  {
			  // System.out.println(cp);
		  }
		  // System.out.println(cp);
	                     }
	  
	  private void setBootstrapLoginfoTable(BootstrapConn bootstrapConn)
	  {
		    Statement stmt = null;
		    try
		    {
		    	String updateStmt = "update bootstrap_loginfo set minwindowscn = 0, maxwindowscn = 0";
		    	stmt =  bootstrapConn.getDBConn().createStatement();
		    	stmt.executeUpdate(updateStmt);
		    } catch ( SQLException ex) {
		    	throw new RuntimeException(ex);
		    } finally {
		    	DBHelper.close(stmt);
		    }
	  }

}
