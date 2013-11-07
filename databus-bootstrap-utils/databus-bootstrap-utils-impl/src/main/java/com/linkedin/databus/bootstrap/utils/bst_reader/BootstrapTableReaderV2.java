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
package com.linkedin.databus.bootstrap.utils.bst_reader;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.utils.BootstrapReaderEventHandler;
import com.linkedin.databus.bootstrap.utils.bst_reader.filter.BootstrapReaderFilter;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.util.DBHelper;

/**
 * Reads the data from a given bootstrap table
 */
public class BootstrapTableReaderV2
{
  private static final int MAX_EVENT_SIZE = 20 * 1024 * 1024;

  private final String _tableName;
  private final  DbusEventAvroDecoder _decoder;
  private final DbusEventFactory _eventFactory;
  private final BootstrapReaderEventHandler _eventHandler;
  private final List<BootstrapReaderFilter> _eventFilters;
  private final BootstrapReadOnlyConfig _bstConfig;
  private final Logger _log;
  private final BootstrapConn _bootstrapConn;
  private final Connection _jdbcConn;
  private final MetaDataFilters _metadataFilters;
  private final String _queryString;
  private final PreparedStatement _query;

  public BootstrapTableReaderV2(String tableName,
                                 MetaDataFilters metadataFilters,
                                 VersionedSchemaSet schemaSet,
                                 BootstrapReaderEventHandler eventHandler,
                                 List<BootstrapReaderFilter> eventFilters,
                                 BootstrapReadOnlyConfig bstConfig,
                                 Logger log)
         throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
  {
    super();
    _log = null != log ? log : Logger.getLogger(BootstrapTableReaderV2.class);
    _metadataFilters = metadataFilters;
    _tableName = tableName;
    _decoder = new DbusEventAvroDecoder(schemaSet);
    _eventFactory  = new DbusEventV1Factory();
    _eventHandler = eventHandler;
    _eventFilters = new ArrayList<BootstrapReaderFilter>(eventFilters);
    _bstConfig = bstConfig;
    _bootstrapConn = createBstConnection();
    _jdbcConn = _bootstrapConn.getDBConn();
    _queryString = createQueryString();
    _query = _jdbcConn.prepareStatement(_queryString);
  }

  public void execute() throws SQLException
  {
    ResultSet rs = null;
    boolean hasMore = true;
    long curId = -1;
    try
    {
      _log.info("Executing query : " + _queryString);
      ByteBuffer buffer = ByteBuffer.allocateDirect(MAX_EVENT_SIZE);
      int count = 0;
      DbusEventInternalReadable event = _eventFactory.createReadOnlyDbusEventFromBuffer(buffer, 0);

      _eventHandler.onStart(_queryString);
      while (hasMore)
      {
        _log.debug("currentId=" + curId);
        _query.setLong(1, curId);
        rs = _query.executeQuery();

        hasMore = false;
        while (rs.next())
        {
          hasMore = true;
          buffer.clear();
          buffer.put(rs.getBytes("val"));
          curId = rs.getLong("id");
          event = event.reset(buffer, 0);
          GenericRecord record = _decoder.getGenericRecord(event);
          if (checkFilters(event, record))
          {
            _eventHandler.onRecord(event, record);
          }
          count++;
        }

        rs.close();
      }
      _eventHandler.onEnd(count);

    }
    finally
    {
      DBHelper.close(rs, _query, _jdbcConn);
    }
  }

  private boolean checkFilters(DbusEventInternalReadable event, GenericRecord payload)
  {
    for (BootstrapReaderFilter filter: _eventFilters)
    {
      if (! filter.matches(event, payload))
      {
        return false;
      }
    }
    return true;
  }

  private BootstrapConn createBstConnection()
          throws InstantiationException, IllegalAccessException, ClassNotFoundException,
                 SQLException
  {
    _log.info("<<<< Creating Bootstrap Connection >>>>");
    BootstrapConn bstConn = new BootstrapConn();
    bstConn.initBootstrapConn(true,
                               _bstConfig.getBootstrapDBUsername(),
                               _bstConfig.getBootstrapDBPassword(),
                               _bstConfig.getBootstrapDBHostname(),
                               _bstConfig.getBootstrapDBName());
    return bstConn;
  }

  private String createQueryString()
  {
    StringBuilder sql = new StringBuilder();

    sql.append("SELECT id,val FROM ").append(_tableName).append(" WHERE id > ?");
    if (null != _metadataFilters.getMinKey())
    {
      sql.append(" AND srckey >= '" + _metadataFilters.getMinKey() + "'");
    }
    if (null != _metadataFilters.getMaxKey())
    {
      sql.append(" AND srckey <= '" + _metadataFilters.getMaxKey() + "'");
    }
    if (0 < _metadataFilters.getMinScn())
    {
      sql.append(" AND scn >= " + _metadataFilters.getMinScn());
    }
    if (0 < _metadataFilters.getMaxScn())
    {
      sql.append(" AND scn <= " + _metadataFilters.getMaxScn());
    }

    sql.append(" ORDER BY id LIMIT " + _bstConfig.getBootstrapBatchSize());

    return sql.toString();
  }

}
