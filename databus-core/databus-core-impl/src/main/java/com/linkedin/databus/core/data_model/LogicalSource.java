package com.linkedin.databus.core.data_model;
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


import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.util.IdNamePair;

/**
 * Represents a Databus logical source
 *
 * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+2.0+and+Databus+3.0+Data+Model">Databus 2.0 and Databus 3.0 Data Model</a>
 */
public class LogicalSource
{
  private final Integer _id;
  private final String _name;

  /** Value to be used if the logical source id is unknown */
  static final Integer UNKNOWN_LOGICAL_SOURCE_ID = (int)Short.MIN_VALUE;
  static final Integer ALL_LOGICAL_SOURCES_ID = -1;
  static final String ALL_LOGICAL_SOURCES_NAME = "*";

  public static final LogicalSource ALL_LOGICAL_SOURCES =
      new LogicalSource(ALL_LOGICAL_SOURCES_ID, ALL_LOGICAL_SOURCES_NAME);

  /**
   * Constructor
   * @param  id         the globally unique source id
   * @param  name       the globally unique source name
   * */
  public LogicalSource(Integer id, String name)
  {
    super();
    if (null == id) throw new NullPointerException("id");
    if (null == name) throw new NullPointerException("name");
    _id = id;
    _name = name;
  }

  public LogicalSource(IdNamePair pair) {
    this(pair.getId().intValue(), pair.getName());
  }

  /** Creates a logical source by name only */
  public LogicalSource(String sourceName)
  {
    this(UNKNOWN_LOGICAL_SOURCE_ID, sourceName);
  }

  /** Creates a logical source wildcard that matches all sources */
  public static LogicalSource createAllSourcesWildcard()
  {
    return ALL_LOGICAL_SOURCES;
  }

  /**
   * Create a LogicalSource object from a JSON string
   * @param  json           the string with JSON serialization of the LogicalSource
   */
  public static LogicalSource createFromJsonString(String json)
         throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    Builder result = mapper.readValue(json, Builder.class);
    return result.build();
  }

  /** The source id */
  public Integer getId()
  {
    return _id;
  }

  /** The fully-qualified source name */
  public String getName()
  {
    return _name;
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  public String toJsonString()
  {
    StringBuilder sb = new StringBuilder(_name.length() + 50);
    sb.append("{\"id\":");
    sb.append(_id.shortValue());
    sb.append(",\"name\":\"");
    sb.append(_name);
    sb.append("\"}");

    return sb.toString();
  }

  /** Checks if the object denotes a wildcard */
  public boolean isWildcard()
  {
    return isAllSourcesWildcard();
  }

  /** Checks if the object denotes a ALL_LOGICAL_SOURCES wildcard */
  public boolean isAllSourcesWildcard()
  {
    return _id.intValue() == ALL_LOGICAL_SOURCES_ID.intValue();
  }

  public boolean equalsSource(LogicalSource other)
  {
    //compare only the names as id may be unknown
    return isAllSourcesWildcard() || other.isAllSourcesWildcard() || _name.equals(other.getName());
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || !(other instanceof LogicalSource)) return false;
    return equalsSource((LogicalSource)other);
  }

  @Deprecated
  /** Added for legacy reason to transition for the use of IdNamePair for sources. To be used for
   * tracking of those uses*/
  public IdNamePair asIdNamePair()
  {
    return new IdNamePair(Long.valueOf(_id.longValue()), _name);
  }

  @Override
  public int hashCode()
  {
    return _id.intValue() == ALL_LOGICAL_SOURCES_ID.intValue() ? _id.hashCode() : _name.hashCode() ;
  }

  public boolean idKnown()
  {
    return _id.intValue() != UNKNOWN_LOGICAL_SOURCE_ID.intValue();
  }

  /**
   * Converts the logical source to a human-readable string
   * @param   sb        a StringBuilder to accumulate the string representation; if null, a new one will be allocated
   * @return  the StringBuilder
   */
  public StringBuilder toSimpleString(StringBuilder sb)
  {
    if (null == sb)
    {
      sb = new StringBuilder(50);
    }
    sb.append("[");
    if (isWildcard())
    {
      sb.append("*");
    }
    else if (idKnown())
    {
      sb.append("name=").append(_name).append(", id=").append(_id);
    }
    else
    {
      sb.append("name=").append(_name);
    }
    sb.append("]");
    return sb;
  }

  /**
   * Converts the logical source to a human-readable string
   */
  public String toSimpleString()
  {
    return toSimpleString(null).toString();
  }

  public static class Builder
  {
    //private Integer _id = UNKNOWN_LOGICAL_SOURCE_ID;
    private Integer _id;
    private String _name;

    public Builder()
    {
      _id = ALL_LOGICAL_SOURCES_ID;
      _name = ALL_LOGICAL_SOURCES_NAME;
    }

    public Integer getId()
    {
      return _id;
    }

    public void setId(Integer id)
    {
      _id = id;
    }

    public String getName()
    {
      return _name;
    }

    public void setName(String name)
    {
      _name = name;
      if (_id.intValue() == ALL_LOGICAL_SOURCES_ID.intValue() && !ALL_LOGICAL_SOURCES_NAME.equals(name))
      {
        //set the id to unknown if we have an explicit table name
        _id = UNKNOWN_LOGICAL_SOURCE_ID;
      }
    }

    public void makeAllSourcesWildcard()
    {
      _id = ALL_LOGICAL_SOURCES_ID;
      _name = ALL_LOGICAL_SOURCES_NAME;
    }

    public LogicalSource build()
    {
      return new LogicalSource(_id, _name);
    }

  }

}
