/**
 * 
 */
package com.linkedin.databus.bootstrap.test;

import com.linkedin.databus.bootstrap.api.BootstrapEventCallback;
import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;
import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapEventProcessResultImpl;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

import java.sql.ResultSet;

/**
 * @author ksurlake
 *
 */
public class EventProcessor implements BootstrapEventCallback
{

	public BootstrapEventProcessResult onEvent(ResultSet rs, DbusEventsStatisticsCollector statsCollector) throws BootstrapProcessingException
	{
		//System.out.println("scn = " + scn + "rid = " + rid);
	  if (false)
	  {
	    throw new BootstrapProcessingException("dummy for testing");
	  }
	  
	  return new BootstrapEventProcessResultImpl(1, false, false);
	}
	
	public void onCheckpointEvent(Checkpoint ckpt, DbusEventsStatisticsCollector statsCollector)
	{
	}
}
