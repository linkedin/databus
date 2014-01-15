package com.linkedin.databus2.core.monitoring;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.StatsCollectorCallback;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventStatsCollectorsPartitioner;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.test.DbusEventCorrupter;
import com.linkedin.databus.core.test.DbusEventGenerator;
import com.linkedin.databus.core.util.RngUtils;

public class TestDbusEventStatsCollectorPartitioner
{
  public static final String MODULE = TestDbusEventStatsCollectorPartitioner.class.getName();
  private static final Logger LOG = Logger.getLogger(MODULE);
  
  @Test
  public void testStatsCollectorPartitionerSingleDb()
  {
    DbusEventStatsCollectorsPartitioner collector =
        new DbusEventStatsCollectorsPartitioner(1, ":inbound", null);

    TestStatsCollectorCallback callback = new TestStatsCollectorCallback();
    collector.registerStatsCallback(callback);

    // Add 2 collectors for DB1
    PhysicalPartition p1 = new PhysicalPartition(1, "db1");
    PhysicalPartition p2 = new PhysicalPartition(2, "db1");
    DbusEventsStatisticsCollector c1 =
        new DbusEventsStatisticsCollector(1, "db1:1", true, false, null);
    DbusEventsStatisticsCollector c2 =
        new DbusEventsStatisticsCollector(1, "db1:2", true, false, null);
    collector.addStatsCollector(p1, c1);
    collector.addStatsCollector(p2, c2);

    StatsWriter w1 = new StatsWriter(c1);
    StatsWriter w2 = new StatsWriter(c2);
    w1.addEvents(2, 2, 100);
    w2.addEvents(2, 2, 200);
    StatsCollectors<DbusEventsStatisticsCollector> col =
        collector.getDBStatsCollector("db1");
    Assert.assertNotNull(col);
    col.mergeStatsCollectors();
    LOG.info("Merged Stats : " + col.getStatsCollector().getTotalStats());
    LOG.info("C1 Stats : " + c1.getTotalStats());
    LOG.info("C2 Stats : " + c2.getTotalStats());
    DbusEventsTotalStats s = col.getStatsCollector().getTotalStats();
    Assert.assertEquals("Total Events", 8, s.getNumDataEvents());
    Assert.assertEquals("Sys Events", 4, s.getNumSysEvents());
    Assert.assertEquals("Min Scn", 101, s.getMinScn());
    Assert.assertEquals("Max Scn", 205, s.getMaxScn());
    Assert.assertEquals("Num Stats Callback", 1, callback.getCollectorsAddedList().size());
    collector.removeAllStatsCollector();
    Assert.assertEquals("Num Stats Callback", 1, callback.getCollectorsRemovedList()
                                                         .size());
  }

  @Test
  public void testStatsCollectorPartitionerMultipleDbs()
  {
    DbusEventStatsCollectorsPartitioner collector =
        new DbusEventStatsCollectorsPartitioner(1, ":inbound", null);

    TestStatsCollectorCallback callback = new TestStatsCollectorCallback();
    collector.registerStatsCallback(callback);

    // Add 2 collectors for DB1
    PhysicalPartition p1 = new PhysicalPartition(1, "db1");
    PhysicalPartition p2 = new PhysicalPartition(2, "db1");
    DbusEventsStatisticsCollector c1 =
        new DbusEventsStatisticsCollector(1, "db1:1", true, false, null);
    DbusEventsStatisticsCollector c2 =
        new DbusEventsStatisticsCollector(1, "db1:2", true, false, null);
    collector.addStatsCollector(p1, c1);
    collector.addStatsCollector(p2, c2);

    // Add 2 collectors for DB2
    PhysicalPartition p3 = new PhysicalPartition(1, "db2");
    PhysicalPartition p4 = new PhysicalPartition(2, "db2");
    DbusEventsStatisticsCollector c3 =
        new DbusEventsStatisticsCollector(1, "db2:1", true, false, null);
    DbusEventsStatisticsCollector c4 =
        new DbusEventsStatisticsCollector(1, "db2:2", true, false, null);
    collector.addStatsCollector(p3, c3);
    collector.addStatsCollector(p4, c4);

    // Add 2 collectors for DB3
    PhysicalPartition p5 = new PhysicalPartition(3, "db3");
    PhysicalPartition p6 = new PhysicalPartition(4, "db3");
    DbusEventsStatisticsCollector c5 =
        new DbusEventsStatisticsCollector(1, "db3:3", true, false, null);
    DbusEventsStatisticsCollector c6 =
        new DbusEventsStatisticsCollector(1, "db3:4", true, false, null);
    collector.addStatsCollector(p5, c5);
    collector.addStatsCollector(p6, c6);

    StatsWriter w1 = new StatsWriter(c1);
    StatsWriter w2 = new StatsWriter(c2);
    StatsWriter w3 = new StatsWriter(c3);
    StatsWriter w4 = new StatsWriter(c4);
    StatsWriter w5 = new StatsWriter(c5);
    StatsWriter w6 = new StatsWriter(c6);
    w1.addEvents(2, 2, 100);
    w2.addEvents(2, 2, 200);
    w3.addEvents(3, 2, 300);
    w4.addEvents(3, 2, 400);
    w5.addEvents(4, 2, 500);
    w6.addEvents(4, 2, 600);

    // Verify DB1 collector
    StatsCollectors<DbusEventsStatisticsCollector> col =
        collector.getDBStatsCollector("db1");
    Assert.assertNotNull(col);
    col.mergeStatsCollectors();
    LOG.info("Merged Stats : " + col.getStatsCollector().getTotalStats());
    LOG.info("C1 Stats : " + c1.getTotalStats());
    LOG.info("C2 Stats : " + c2.getTotalStats());
    DbusEventsTotalStats s = col.getStatsCollector().getTotalStats();
    Assert.assertEquals("Total Events", 8, s.getNumDataEvents());
    Assert.assertEquals("Sys Events", 4, s.getNumSysEvents());
    Assert.assertEquals("Min Scn", 101, s.getMinScn());
    Assert.assertEquals("Max Scn", 205, s.getMaxScn());

    // Verify DB2 collector
    col = collector.getDBStatsCollector("db2");
    Assert.assertNotNull(col);
    col.mergeStatsCollectors();
    LOG.info("Merged Stats : " + col.getStatsCollector().getTotalStats());
    LOG.info("C3 Stats : " + c3.getTotalStats());
    LOG.info("C4 Stats : " + c4.getTotalStats());
    s = col.getStatsCollector().getTotalStats();
    Assert.assertEquals("Total Events", 12, s.getNumDataEvents());
    Assert.assertEquals("Sys Events", 4, s.getNumSysEvents());
    Assert.assertEquals("Min Scn", 301, s.getMinScn());
    Assert.assertEquals("Max Scn", 407, s.getMaxScn());

    // Verify DB3 collector
    col = collector.getDBStatsCollector("db3");
    Assert.assertNotNull(col);
    col.mergeStatsCollectors();
    LOG.info("Merged Stats : " + col.getStatsCollector().getTotalStats());
    LOG.info("C3 Stats : " + c5.getTotalStats());
    LOG.info("C4 Stats : " + c6.getTotalStats());
    s = col.getStatsCollector().getTotalStats();
    Assert.assertEquals("Total Events", 16, s.getNumDataEvents());
    Assert.assertEquals("Sys Events", 4, s.getNumSysEvents());
    Assert.assertEquals("Min Scn", 501, s.getMinScn());
    Assert.assertEquals("Max Scn", 609, s.getMaxScn());

    Assert.assertEquals("Num Stats Callback", 3, callback.getCollectorsAddedList().size());
    collector.removeAllStatsCollector();
    Assert.assertEquals("Num Stats Callback", 3, callback.getCollectorsRemovedList()
                                                         .size());
  }

  public class TestStatsCollectorCallback implements
      StatsCollectorCallback<StatsCollectors<DbusEventsStatisticsCollector>>
  {

    public final List<StatsCollectors<DbusEventsStatisticsCollector>> _collectorsAddedList;
    public final List<StatsCollectors<DbusEventsStatisticsCollector>> _collectorsRemovedList;

    public TestStatsCollectorCallback()
    {
      _collectorsAddedList =
          new ArrayList<StatsCollectors<DbusEventsStatisticsCollector>>();
      _collectorsRemovedList =
          new ArrayList<StatsCollectors<DbusEventsStatisticsCollector>>();
    }

    @Override
    public void addedStats(StatsCollectors<DbusEventsStatisticsCollector> stats)
    {
      _collectorsAddedList.add(stats);
    }

    @Override
    public void removedStats(StatsCollectors<DbusEventsStatisticsCollector> stats)
    {
      _collectorsRemovedList.add(stats);
    }

    public void reset()
    {
      _collectorsAddedList.clear();
      _collectorsRemovedList.clear();
    }

    public List<StatsCollectors<DbusEventsStatisticsCollector>> getCollectorsAddedList()
    {
      return _collectorsAddedList;
    }

    public List<StatsCollectors<DbusEventsStatisticsCollector>> getCollectorsRemovedList()
    {
      return _collectorsRemovedList;
    }
  }

  private static class StatsWriter
  {
    private final DbusEventsStatisticsCollector _stats;

    public StatsWriter(DbusEventsStatisticsCollector stats)
    {
      _stats = stats;
    }

    public DbusEventsStatisticsCollector getEventsStatsCollector()
    {
      return _stats;
    }

    public void addEvents(int eventsPerWindow, int numWindows, long beginScn)
    {
      long startScn = beginScn;
      int numEvents = eventsPerWindow * numWindows;
      Vector<DbusEvent> events = new Vector<DbusEvent>(numEvents);
      long prevScn = startScn - 1;

      int maxEventSize = 100;
      int payloadSize = 5;
      events.clear();
      DbusEventGenerator eventGen = new DbusEventGenerator(startScn + 1);
      long newScn =
          eventGen.generateEvents(numEvents,
                                  eventsPerWindow,
                                  maxEventSize,
                                  payloadSize,
                                  true,
                                  events);
      DbusEventInternalWritable p = null;
      for (DbusEvent e : events)
      {
        if (p != null && (p.sequence() != e.sequence()))
        {
          // control event for prev sequence;
          p.setSrcId((short) -1);
          _stats.registerDataEvent(p);
        }
        _stats.registerDataEvent((DbusEventInternalReadable) e);
        try
        {
          p = DbusEventCorrupter.makeWritable(e); // for testing only!
        }
        catch (InvalidEventException iee)
        {
          throw new RuntimeException(iee);
        }
      }

      // Add window boundary at the end
      if (p != null)
      {
        p.setSrcId((short) -1);
        _stats.registerDataEvent(p);
      }
      _stats.registerBufferMetrics(startScn + 1,
                                   newScn,
                                   prevScn,
                                   RngUtils.randomPositiveInt());
      startScn = newScn;
    }

  }
}
