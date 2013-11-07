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


import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.producers.EventCreationException;

/**
 * Factory interface for event creation factory classes.
 */
//TODO - I don't see much value from this interface. Its place is certainly not in
//databus2-event-producer-common but we have to get rid of MonitoredSourceInfo dependency.
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
