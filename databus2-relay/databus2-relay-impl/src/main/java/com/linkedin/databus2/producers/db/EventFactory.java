/*
 * $Id: EventFactory.java 160976 2011-01-21 23:32:12Z lgao $
 */
package com.linkedin.databus2.producers.db;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.producers.EventCreationException;

/**
 * Factory interface for event creation factory classes.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 160976 $
 */
public interface EventFactory
{
  /**
   * Create an event from the current row in the given ResultSet.
   *
   * @param scn the scn of the event
   * @param timestamp the timestamp of the event (in millis)
   * @param row the row to read
   * @param eventBuffer the buffer to store the event
   * @param enableTracing Flag to enable debugging
   * @param DbusEventStatisticsCollector Stats Collector
   * @throws SQLException if there is a database problem
   * @throws EventCreationException if there is any other problem
   * @throws UnsupportedKeyException if the DbusEventKey type is other than String or Long
   * @return size of the serialized event
   */
  public long createAndAppendEvent(long scn, 
                                   long timestamp, 
                                   ResultSet row, 
                                   DbusEventBufferAppendable eventBuffer, 
                                   boolean enableTracing, 
                                   DbusEventsStatisticsCollector dbusEventsStatisticsCollector)
  throws SQLException, EventCreationException, UnsupportedKeyException;
  
  
  /**
   * Create an event from the Avro GenericRecord.
   *
   * @param scn the scn of the event
   * @param timestamp the timestamp of the event (in millis)
   * @param record the avro record to be stored as payload
   * @param eventBuffer the buffer to store the event
   * @param enableTracing Flag to enable debugging
   * @param DbusEventStatisticsCollector Stats Collector
   * @throws EventCreationException if there is any other problem
   * @throws UnsupportedKeyException if the DbusEventKey type is other than String or Long
   * @return size of the serialized event
   */
  public long createAndAppendEvent(long scn,
                                   long timestamp,
                                   GenericRecord record,
                                   DbusEventBufferAppendable eventBuffer,
                                   boolean enableTracing,
                                   DbusEventsStatisticsCollector dbusEventsStatisticsCollector)
  throws EventCreationException, UnsupportedKeyException;
}
