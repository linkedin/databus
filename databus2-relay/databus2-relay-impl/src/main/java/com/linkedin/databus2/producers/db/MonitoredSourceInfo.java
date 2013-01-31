/*
 * $Id: MonitoredSourceInfo.java 157085 2010-12-23 23:21:58Z jwesterm $
 */
package com.linkedin.databus2.producers.db;
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


import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 157085 $
 */
public class MonitoredSourceInfo
{
  private final short _sourceId;
  private final EventFactory _factory;
  private final String _sourceName;
  private final String _eventQueryHints;
  private final String _eventTxnChunkedQueryHints;
  private final String _eventScnChunkedQueryHints;
  private final String _eventView;
  private final String _eventSchema;
  private final EventSourceStatistics _statisticsBean;
  private final boolean _skipInfinityScn;

  public String getEventView()
  {
    return _eventView;
  }
  public short getSourceId()
  {
    return _sourceId;
  }
  public EventFactory getFactory()
  {
    return _factory;
  }
  public String getSourceName()
  {
    return _sourceName;
  }
  public String getEventQueryHints()
  {
    return _eventQueryHints;
  }
  public boolean hasEventQueryHints()
  {
    return _eventQueryHints != null;
  }
  
  public String getEventSchema()
  {
    return _eventSchema;
  }
  
  public EventSourceStatistics getStatisticsBean()
  {
    return _statisticsBean;
  }
 
  public String getEventTxnChunkedQueryHints() {
	return _eventTxnChunkedQueryHints;
  }
  

  public String getEventScnChunkedQueryHints() {
	return _eventScnChunkedQueryHints;
  }
  
  public MonitoredSourceInfo(short sourceId, String sourceName, String eventSchema, String eventView,
          EventFactory factory, EventSourceStatistics statisticsBean,
          String eventQueryHints, String eventTxnChunkedQueryHints, String eventScnChunkedQueryHints, boolean skipInfinityScn)
  {
	    _eventView = eventView;
	    _eventSchema = eventSchema;
	    _sourceId = sourceId;
	    _factory = factory;
	    _sourceName = sourceName;
	    _eventQueryHints = eventQueryHints;
	    _skipInfinityScn = skipInfinityScn;
	    _eventScnChunkedQueryHints = eventScnChunkedQueryHints;
	    _eventTxnChunkedQueryHints = eventTxnChunkedQueryHints;
	    
	    if(statisticsBean == null)
	    {
	      statisticsBean = new EventSourceStatistics(sourceName);
	    }
	    _statisticsBean = statisticsBean;

  }
  

  public MonitoredSourceInfo(short sourceId, String sourceName, String eventSchema, String eventView,
                             EventFactory factory, EventSourceStatistics statisticsBean,
                             boolean skipInfinityScn)
  {
    this(sourceId, sourceName, eventSchema, eventView, factory, statisticsBean,  null, null, null,
         skipInfinityScn);
  }

  @Override
  public String toString()
  {
    return _sourceName + " (id=" + _sourceId + ")";
  }
  public boolean isSkipInfinityScn()
  {
    return _skipInfinityScn;
  }
}
