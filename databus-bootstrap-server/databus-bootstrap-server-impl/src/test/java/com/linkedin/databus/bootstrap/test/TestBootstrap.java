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


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.server.BootstrapProcessor;
import com.linkedin.databus.bootstrap.server.BootstrapServerConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.request.BootstrapDBException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.util.DBHelper;

public class TestBootstrap
{
  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestBootstrap_", ".log", Level.ERROR);
  }

	  @Test
	  public void testBootstrapProcessor()
	    throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException,
	    IOException, BootstrapProcessingException, InvalidConfigException, BootstrapDatabaseTooOldException ,BootstrapDBException
	    {
		  EventProcessor processorCallback = new EventProcessor();
		  BootstrapConfig config = new BootstrapConfig();
		  BootstrapReadOnlyConfig staticConfig = config.build();

          BootstrapServerConfig configBuilder = new BootstrapServerConfig();
          configBuilder.setEnableMinScnCheck(false);
          BootstrapServerStaticConfig staticServerConfig = configBuilder.build();

		  BootstrapProcessor processor = new BootstrapProcessor(staticServerConfig, null);
		  String sourceName = "TestBootstrap.testBootstrapProcessor.events";

		  // Create the tables for all the sources before starting up the threads

		  BootstrapConn _bootstrapConn = new BootstrapConn();
		  boolean autoCommit = true;
		  _bootstrapConn.initBootstrapConn(autoCommit,
				  staticConfig.getBootstrapDBUsername(),
				  staticConfig.getBootstrapDBPassword(),
				  staticConfig.getBootstrapDBHostname(),
				  staticConfig.getBootstrapDBName());

		  BootstrapDBMetaDataDAO dao = new BootstrapDBMetaDataDAO(_bootstrapConn,
	        									staticConfig.getBootstrapDBHostname(),
	        									staticConfig.getBootstrapDBUsername(),
	        									staticConfig.getBootstrapDBPassword(),
	        									staticConfig.getBootstrapDBName(),
	        									autoCommit);
		  SourceStatusInfo srcStatusInfo = dao.getSrcIdStatusFromDB(sourceName, false);

		  if (srcStatusInfo.getSrcId() >= 0 )
		  {
			  dao.dropSourceInDB(srcStatusInfo.getSrcId());
		  }

		  dao.addNewSourceInDB(sourceName,BootstrapProducerStatus.ACTIVE);
		  srcStatusInfo = dao.getSrcIdStatusFromDB(sourceName, false);

		  setBootstrapLoginfoTable(_bootstrapConn, 0, Long.MAX_VALUE);

		  //insert one row
		  insertOneSnapshotEvent(dao, srcStatusInfo.getSrcId(), Long.MAX_VALUE - 10,
		                         Long.MAX_VALUE - 100, "check", "test_payload_data");

		  BootstrapCheckpointHandler bstCheckpointHandler =
		      new BootstrapCheckpointHandler(sourceName);
		  Checkpoint c = bstCheckpointHandler.createInitialBootstrapCheckpoint(null, 0L);

		  c.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
          c.setBootstrapStartScn(Long.MAX_VALUE - 10);
		  c.setSnapshotOffset(Long.MAX_VALUE - 1000);
		  // System.out.println("Snapshot");
		  processor.streamSnapShotRows(c, processorCallback);
		  Assert.assertEquals(processorCallback.getrIds().size(), 1);
		  Assert.assertEquals(Long.MAX_VALUE-10, processorCallback.getrIds().get(0).longValue());
		  Assert.assertEquals(Long.MAX_VALUE-100, processorCallback.getSequences().get(0).longValue());
		  Assert.assertEquals("check", processorCallback.getSrcKeys().get(0));
		  Assert.assertEquals("test_payload_data", new String(processorCallback.getValues().get(0)));

		  processorCallback.reset();

          c.setSnapshotOffset(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET);
          c.setBootstrapTargetScn(c.getBootstrapStartScn().longValue());
          bstCheckpointHandler.advanceAfterTargetScn(c);

          bstCheckpointHandler.advanceAfterSnapshotPhase(c);
//		  c.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
//		  c.setCatchupSource(sourceName);
//		  c.setWindowScn(0L);
//		  c.setWindowOffset(0);

		  // System.out.println("Catchup");
		  boolean phaseCompleted = processor.streamCatchupRows(c, processorCallback);
		  Assert.assertEquals( true, phaseCompleted);
	    }

	  private void insertOneSnapshotEvent(BootstrapDBMetaDataDAO dao, int srcId, long rId, long scn, String srcKey, String data)
	  {
		  PreparedStatement stmt = null;
		  try
		  {
			  Connection conn = dao.getBootstrapConn().getDBConn();
			  String st = "insert into tab_" + srcId + "(id, scn, srckey, val) values(?, ?, ?, ?)";
			  System.out.println("SQL :" + st);
			  stmt = conn.prepareStatement(st);
			  stmt.setLong(1,rId);
			  stmt.setLong(2,scn);
			  stmt.setString(3, srcKey);
			  stmt.setBlob(4, new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())));
			  stmt.executeUpdate();
		  } catch ( Exception ex) {
			  System.err.println("Exception :" + ex);
			  throw new RuntimeException(ex);
		  } finally {
			  DBHelper.close(stmt);
		  }
	  }
	  @Test
	  public void testBootstrapService()
	              throws InstantiationException, IllegalAccessException, ClassNotFoundException,
	                     SQLException, IOException, BootstrapProcessingException, InvalidConfigException,
	                     BootstrapDatabaseTooOldException,BootstrapDBException
	  {
	      final Logger log = Logger.getLogger("TestBootstrap.testBootstrapService");
		  EventProcessor processorCallback = new EventProcessor();
		  BootstrapConfig config = new BootstrapConfig();
		  BootstrapReadOnlyConfig staticConfig = config.build();


		  String sources[] = new String[4];
		  sources[0] = "TestBootstrap.testBootstrapService.event";
		  sources[1] = "TestBootstrap.testBootstrapService.event1";
		  sources[2] = "TestBootstrap.testBootstrapService.event2";
		  sources[3] = "TestBootstrap.testBootstrapService.event3";

		  // Create the tables for all the sources before starting up the threads
		  BootstrapConn _bootstrapConn = new BootstrapConn();
		  final boolean autoCommit = true;
		  _bootstrapConn.initBootstrapConn(autoCommit,
				  staticConfig.getBootstrapDBUsername(),
				  staticConfig.getBootstrapDBPassword(),
				  staticConfig.getBootstrapDBHostname(),
				  staticConfig.getBootstrapDBName());

		  BootstrapDBMetaDataDAO dao = new BootstrapDBMetaDataDAO(_bootstrapConn,
				  								staticConfig.getBootstrapDBHostname(),
				  								staticConfig.getBootstrapDBUsername(),
				  								staticConfig.getBootstrapDBPassword(),
				  								staticConfig.getBootstrapDBName(),
				  								autoCommit);

		  for (String source : sources)
		  {
			  SourceStatusInfo srcStatusInfo = dao.getSrcIdStatusFromDB(source, false);

			  if (srcStatusInfo.getSrcId() >= 0 )
			  {
				  dao.dropSourceInDB(srcStatusInfo.getSrcId());
			  }

			  dao.addNewSourceInDB(source,BootstrapProducerStatus.ACTIVE);
		  }

		  setBootstrapLoginfoTable(_bootstrapConn, 1, 1);

		  DatabusBootstrapClient s = new DatabusBootstrapClient(sources);
		  Checkpoint cp;
		  while((cp = s.getNextBatch(10, processorCallback)).getConsumptionMode() != DbusClientMode.ONLINE_CONSUMPTION)
		  {
			log.debug(cp);
		  }
      }

	  private void setBootstrapLoginfoTable(BootstrapConn bootstrapConn, long minLogScn, long maxLogScn)
	  {
		    PreparedStatement stmt = null;
		    try
		    {
		    	stmt =  bootstrapConn.getDBConn().prepareStatement("update bootstrap_loginfo set minwindowscn = ?, maxwindowscn = ?");
		    	stmt.setLong(1, minLogScn);
                stmt.setLong(2, maxLogScn);
		    	stmt.executeUpdate();
		    } catch ( SQLException ex) {
		    	throw new RuntimeException(ex);
		    } finally {
		    	DBHelper.close(stmt);
		    }
	  }

}
