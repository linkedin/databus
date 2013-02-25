/**
 * 
 */
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
