package com.linkedin.databus2.producers;
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



import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;

/** 
 * Given a DbusEventBufferStatsCollector; produce RelayLogging and RelayStats artifacts *
 * @author snagaraj
 *
 */

public class RelayStatsAdapter
{
		private DbusEventsStatisticsCollector _stats;
		private String _name;
		ReadEventCycleSummary _lastReadEventSummary = null;
	
		public RelayStatsAdapter(String name, DbusEventsStatisticsCollector statsCollector)
		{
			_stats = statsCollector;
			_name = name;
		}
	
		/**
		 * Return event cycle summary object ; since last invocation
		 * @return
		 */
		public synchronized ReadEventCycleSummary getReadEventCycleSummary()
		{
			ReadEventCycleSummary diff = null;
			if (_stats != null)
			{
				ReadEventCycleSummary currentReadEventSummary  = getReadEventSummary();
				if (_lastReadEventSummary != null)
				{
					diff = getDiff(currentReadEventSummary, _lastReadEventSummary);
					_lastReadEventSummary = null;
				} 
				else
				{
					diff = currentReadEventSummary;
				}
				_lastReadEventSummary = currentReadEventSummary;
			}
			return diff;
		}
		
		/**
		 
		 * @return eventsummary at this instant of time; readTime will be computed as the diff between cur and last ; 
		 */
		protected ReadEventCycleSummary getReadEventSummary()
		{
			
			List<EventReaderSummary> sourceSummaries = new ArrayList<EventReaderSummary>();
			for (Integer srcId : _stats.getSources())
			{
				sourceSummaries.add(getSummary(srcId));
			}
			ReadEventCycleSummary summary = new ReadEventCycleSummary(_name, sourceSummaries,
					_stats.getTotalStats().getMaxScn(), System.currentTimeMillis());
			return summary;
		}

		/**
		 * Return stats for each table access; readTime will be extrapolated diff between cur and last readings;
		 * @param sourceId
		 * @return
		 */
		protected EventReaderSummary getSummary(int sourceId) 
		{
			DbusEventsTotalStats stats = _stats.getSourceStats(sourceId); 
			if (stats != null)
			{
				EventReaderSummary summary = new EventReaderSummary( (short) sourceId, stats.getDimension(), 
						stats.getMaxScn(), (int) stats.getNumDataEvents(), 
						stats.getSizeDataEvents()*(int) (stats.getNumDataEvents()),
						System.currentTimeMillis(), 0, _stats.getTotalStats().getTimestampMinScnEvent(),
						stats.getTimestampMaxScnEvent(),0L);
				return summary;
			}
			return null;
		}
		
		/**
		 * produce diff of two summaries; src1 -src2
		 */
		protected ReadEventCycleSummary getDiff(ReadEventCycleSummary src1, ReadEventCycleSummary src2)
		{
			List<EventReaderSummary> src1Summaries = src1.getSourceSummaries();
			List<EventReaderSummary> src2Summaries = src2.getSourceSummaries();
			if (src1Summaries.size() == src2Summaries.size())
			{	
				List<EventReaderSummary> sourceSummaries = new ArrayList<EventReaderSummary>();
				for (int i=0; i < src1Summaries.size();++i)
				{
					sourceSummaries.add(getDiff(src1Summaries.get(i),src2Summaries.get(i)));
				}
				ReadEventCycleSummary r1 = new ReadEventCycleSummary(src1.getEventSourceName(), sourceSummaries, 
						src1.getEndOfWindowScn(), src1.getReadMillis()-src2.getReadMillis());
				return r1;
			}
			return null;
		}
		
		/**
		 * produce diff of eventreader summary s1-s2
		 */
		protected EventReaderSummary getDiff(EventReaderSummary s1, EventReaderSummary s2)
		{
			EventReaderSummary diff = new EventReaderSummary(s1.getSourceId(), s1.getSourceName(), s1.getEndOfPeriodSCN(), 
					s1.getNumberOfEvents()-s2.getNumberOfEvents(), s1.getSizeOfSerializedEvents()-s2.getSizeOfSerializedEvents(), s1.getReadMillis()-s2.getReadMillis(), 
					s1.getEventMillis(), 
					s2.getTimeProdEnd(), s1.getTimeProdEnd(), s1.getQueryExecTime());
			return diff;
		}
		/**
		 * Return event source statistics object ; 
		 * @return
		 */
		public synchronized EventSourceStatistics[] getEventSourceStatistics()
		{
			if (_stats == null) 
				return null;
			
			List<Integer> sourceIds = _stats.getSources();
			if (sourceIds.size() > 0)
			{
				EventSourceStatistics[] stats = new EventSourceStatistics[sourceIds.size()];
				int i=0;
				for (Integer srcId : sourceIds)
				{
					EventSourceStatistics stat = getEventSourceStat(_stats.getSourceStats(srcId));
					stats[i++] = stat;
				}
				return stats;
			}
			return null;
		}
		
		protected EventSourceStatistics getEventSourceStat(DbusEventsTotalStats stats)
		{
			if (stats ==null ) return null;
			long numErrors = stats.getNumHeaderErrEvents() + stats.getNumPayloadErrEvents() + stats.getNumInvalidEvents();
			EventSourceStatistics eventStats = new EventSourceStatistics(_name,(int) stats.getNumDataEvents(),stats.getTimeSinceLastAccess(),
													stats.getMaxScn(), numErrors, stats.getSizeDataEvents());
			return eventStats;
		}

}
