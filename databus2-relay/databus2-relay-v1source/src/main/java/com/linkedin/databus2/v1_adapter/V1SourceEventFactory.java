package com.linkedin.databus2.v1_adapter;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.linkedin.databus.core.DatabusException;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Event;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.monitors.db.EventFactory;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;

public class V1SourceEventFactory implements EventFactory<Object, Object>
{
  private final static String DUMMY_KEY = "dummy key";
  private final static String DUMMY_VALUE = "dummy value";

  private final OracleAvroGenericEventFactory _avroFactory;
  private final DbusEventBuffer _eventBuffer;
  private final DbusEventsStatisticsCollector _eventStats;


  public V1SourceEventFactory(OracleAvroGenericEventFactory avroFactory,
                              DbusEventBuffer eventBuffer,
                              DbusEventsStatisticsCollector eventStats)
  {
    super();
    _avroFactory = avroFactory;
    _eventBuffer = eventBuffer;
    _eventStats = eventStats;
  }

  @Override
  public Event<Object, Object> createEvent(long scn,
                                           long timestamp,
                                           ResultSet row,
                                           int idx) throws SQLException, DatabusException
  {
    try
    {
      _avroFactory.createAndAppendEvent(scn, timestamp, row, _eventBuffer, false,  _eventStats);
    }
    catch (EventCreationException e)
    {
      throw new DatabusException("event creation error", e);
    }
    catch (UnsupportedKeyException e)
    {
      throw new DatabusException("event creation error", e);
    }

    return new Event<Object, Object>(DUMMY_KEY, DUMMY_VALUE, scn, timestamp);
  }

}
