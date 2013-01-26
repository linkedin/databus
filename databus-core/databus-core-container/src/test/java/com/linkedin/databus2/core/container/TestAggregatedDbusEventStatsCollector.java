package com.linkedin.databus2.core.container;

import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.DbusEventGenerator;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.container.netty.ServerContainer.GlobalStatsCalc;
import com.linkedin.databus2.test.TestUtil;
import java.util.Vector;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TestAggregatedDbusEventStatsCollector
{
 
	static
	{
		TestUtil.setupLogging(true, null, Level.INFO); 
	}
	public final static String MODULE = TestAggregatedDbusEventStatsCollector.class.getName();
	public final static Logger LOG = Logger.getLogger(MODULE);

  @Test
  public void testAggregatedStats() 
  {
	  //aggregate stats collectors
	  int n[] = {2,10,100,1000};
	  for (int i:n)
	  {
		  runAggregateTestStats(i);
	  }
  }
  
  void runAggregateTestStats (int n)
  {
	  try
	  {
	  DbusEventsStatisticsCollector aggregateEventStatsCollectors = new AggregatedDbusEventsStatisticsCollector(0,
              "eventsInbound",
              true,
              true,
              null);
	  
	   //collection of n+1 stats collectors; 
	   StatsCollectors<DbusEventsStatisticsCollector> eventStatsCollectors = 
			   new StatsCollectors<DbusEventsStatisticsCollector>(aggregateEventStatsCollectors);
	   
	   //add new individual stats collectors
	   int maxEventsInWindow=10;
	   StatsWriter[] nStatsWriters = createStatsWriters(n,maxEventsInWindow);
	   for (StatsWriter sw: nStatsWriters)
	   {
		   eventStatsCollectors.addStatsCollector(sw.getStatsName(),sw.getEventsStatsCollector());
	   }
	   
	  //aggregator thread;
	   //250 ms poll time
	   GlobalStatsCalc agg = new GlobalStatsCalc(10);	   
	   agg.registerStatsCollector(eventStatsCollectors);
	   Thread aggThread = new Thread(agg);
	   aggThread.start();
	   
	   //start writers
	   for (StatsWriter sw: nStatsWriters)
	   {
		   sw.start();
	   }
	   
	   //Let the writers start
	   Thread.sleep(1000);
	   
	   
	   long startTimeMs = System.currentTimeMillis();
	   long durationInMs = 5*1000; //5s
	   DbusEventsTotalStats globalStats = aggregateEventStatsCollectors.getTotalStats();
	   long prevValue = 0, prevSize =0;
	   while (System.currentTimeMillis() < (startTimeMs+durationInMs))
	   {
		   //constraint checks;
		   
		   //check that readers don't have partial updates or get initialized
		   long value = globalStats.getNumDataEvents();
		   long size = globalStats.getSizeDataEvents();
		   Assert.assertTrue(value > 0);
		   if (prevValue > 0 && (value != prevValue))
		   {
			   Assert.assertTrue(size != prevSize);
			   prevValue = value;
			   prevSize = size;
			   
		   }
		   Assert.assertTrue(globalStats.getMaxSeenWinScn() > 0);
		   Thread.sleep(RngUtils.randomPositiveInt()%10+1);
	   }
	   //shutdown
	   for (StatsWriter sw: nStatsWriters)
	   {
		   sw.shutdown();
		   sw.interrupt();
	   }
	   //Give a chance to catchup;
	   Thread.sleep(1000);
	   
	   agg.halt();
	   aggThread.interrupt();
	   
	   //final tally aggregatedEventTotalStats = sum of all individual statsWriter objects in a thread free way
	   
	   AggregatedDbusEventsTotalStats myTotalStats = new AggregatedDbusEventsTotalStats(StatsWriter.OWNERID, "mytotal", true, false, null);
	   for (DbusEventsStatisticsCollector s:eventStatsCollectors.getStatsCollectors())
	   {
		   DbusEventsTotalStats writerStat = s.getTotalStats();
		   //obviously - we assume this is correct here. we want to check that the updates happen correctly in a concurrent setting
		   myTotalStats.mergeStats(writerStat);
	   }
	   LOG.info("global = " + globalStats.getNumDataEvents() + " Sigma writers=" + myTotalStats.getNumDataEvents());
	   Assert.assertTrue(globalStats.getNumDataEvents() == myTotalStats.getNumDataEvents());
	   Assert.assertTrue(globalStats.getMaxSeenWinScn() == myTotalStats.getMaxSeenWinScn());

	  }
	  catch (InterruptedException e)
	  {
		  Assert.assertTrue(false);
	  }
	   
  }

  StatsWriter[]  createStatsWriters(int n,int maxWindowSize)
  {
	  StatsWriter[] sw = new StatsWriter[n];
	  for (int i=0;i < n;++i)
	  {
		  sw[i] = new StatsWriter(i+1, maxWindowSize);
	  }
	  return sw;
  }
  
  /**
   * 
   * @author snagaraj
   * Create a writer to a stats collector object it owns
   */
  class StatsWriter extends Thread
  {
	  	private final DbusEventsStatisticsCollector _stats;
	  	private final int _maxWindowSize ;
	  	private final String _name;
	  	private volatile boolean _shutdown = false; 
	  	public static final int OWNERID=-1;
	  
		public StatsWriter(int id,int maxWindowSize)
		{
			_maxWindowSize = maxWindowSize;
			_name = "stats_" + id;
			_stats = new DbusEventsStatisticsCollector(OWNERID,_name,true,true,null);
		}
		
		public DbusEventsStatisticsCollector getEventsStatsCollector()
		{
			return _stats;
		}
		
		public void shutdown()
		{
			_shutdown = true;
		}
		
		public String getStatsName()
		{
			return _name;
		}
		
		@Override
		public void run()
		{
			int numEvents = _maxWindowSize*10; 
			Vector<DbusEventInternalWritable> events = new Vector<DbusEventInternalWritable>(numEvents);
			long startScn = RngUtils.randomPositiveLong()%10000;
			long prevScn = startScn-1;
			while (!_shutdown)
			{
				int maxEventSize = 100;
				int payloadSize = 5;
				events.clear();
				DbusEventGenerator eventGen = new DbusEventGenerator(startScn+1);
				long newScn = eventGen.generateEvents(numEvents, _maxWindowSize, maxEventSize, payloadSize,true, events);
				DbusEventInternalWritable p = null;
				for (DbusEventInternalWritable e: events)
				{
					if (p != null && (p.sequence() != e.sequence()))
					{
						//control event for prev sequence;
						p.setSrcId((short) -1);
						_stats.registerDataEvent(p);
					}
					_stats.registerDataEvent(e);
					p = e;
				}
				if (p != null)
				{
					p.setSrcId( (short)-1);
					_stats.registerDataEvent(p);
				}
				_stats.registerBufferMetrics(startScn+1, newScn , prevScn, RngUtils.randomPositiveInt());
				startScn = newScn;
				if (!_shutdown)
				{
					try
					{
						Thread.sleep(RngUtils.randomPositiveInt()%50+10);
					}
					catch (InterruptedException e1)
					{

					}
				}
			}
		}

  }
  
}
