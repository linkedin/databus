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
import java.util.ArrayList;
import java.util.List;

/**
 * @author ksurlake
 *
 */
public class EventProcessor implements BootstrapEventCallback
{

	private final List<Long> rIds = new ArrayList<Long>();
	private final List<Long> sequences = new ArrayList<Long>();
	private final List<String> srcKeys = new ArrayList<String>();
	private final List<byte[]> values = new ArrayList<byte[]>();
	
	
	public BootstrapEventProcessResult onEvent(ResultSet rs, DbusEventsStatisticsCollector statsCollector) throws BootstrapProcessingException
	{	
	  try
	  {
		  rIds.add(rs.getLong(1));
		  sequences.add(rs.getLong(2));
		  srcKeys.add(rs.getString(3));
		  values.add(rs.getBytes(4));
	  } catch (Exception ex) {
		  throw new BootstrapProcessingException(ex);
	  }
	  
	  return new BootstrapEventProcessResultImpl(1, false, false);
	}
	
	
	public void reset()
	{
		rIds.clear();
		sequences.clear();
		srcKeys.clear();
		values.clear();
	}
	
	
	public List<Long> getrIds() {
		return rIds;
	}


	public List<Long> getSequences() {
		return sequences;
	}


	public List<String> getSrcKeys() {
		return srcKeys;
	}


	public List<byte[]> getValues() {
		return values;
	}


	public void onCheckpointEvent(Checkpoint ckpt, DbusEventsStatisticsCollector statsCollector)
	{
	}
}
