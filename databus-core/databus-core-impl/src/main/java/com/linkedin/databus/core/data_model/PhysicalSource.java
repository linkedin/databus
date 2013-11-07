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

/**
 * Represents a Databus physical source
 *
 * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+2.0+and+Databus+3.0+Data+Model">Databus 2.0 and Databus 3.0 Data Model</a>
 */
public class PhysicalSource
{

  /*
   * Physical Source Role
   */

  private final String _uri;
  private final String _resourceKey;
  private final Role _role;

  public static final String PHYSICAL_SOURCE_MASTER = "MASTER";
  public static final String PHYSICAL_SOURCE_SLAVE = "SLAVE";
  public static final String PHYSICAL_SOURCE_ANY = "ANY";

  static final String ANY_PHISYCAL_SOURCE_URI  = "databus:physical-source:ANY";
  static final String MASTER_PHISYCAL_SOURCE_URI  = "databus:physical-source:MASTER";
  static final String SLAVE_PHISYCAL_SOURCE_URI  = "databus:physical-source:SLAVE";

  public static final PhysicalSource ANY_PHISYCAL_SOURCE =
      new PhysicalSource(ANY_PHISYCAL_SOURCE_URI, PHYSICAL_SOURCE_ANY, "");
  public static final PhysicalSource MASTER_PHISYCAL_SOURCE =
      new PhysicalSource(MASTER_PHISYCAL_SOURCE_URI, PHYSICAL_SOURCE_MASTER, "");
  public static final PhysicalSource SLAVE_PHISYCAL_SOURCE =
      new PhysicalSource(SLAVE_PHISYCAL_SOURCE_URI, PHYSICAL_SOURCE_SLAVE, "");

  public PhysicalSource(String sourceUri) {
    this(sourceUri, PHYSICAL_SOURCE_MASTER, "");
  }

  public PhysicalSource(String sourceUri, String role, String resourceKey)
  {
    super();
    if (null == sourceUri) throw new NullPointerException("physical source uri");
    _uri = sourceUri;

    _role = new Role(role);
    _resourceKey = resourceKey;
  }

  public static PhysicalSource createAnySourceWildcard()
  {
    return ANY_PHISYCAL_SOURCE;
  }

  public static PhysicalSource createMasterSourceWildcard()
  {
    return MASTER_PHISYCAL_SOURCE;
  }

  public static PhysicalSource createSlaveSourceWildcard()
  {
    return SLAVE_PHISYCAL_SOURCE;
  }

  /**
   * Create a PhysicalSource object from a JSON string
   * @param  json           the string with JSON serialization of the PhysicalSource
   */
  public static PhysicalSource createFromJsonString(String json)
         throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    Builder result = mapper.readValue(json, Builder.class);
    return result.build();
  }

  /** The physical source URI */
  public String getUri()
  {
    return _uri;
  }

  /** The physical source ROle */
  public Role getRole()
  {
    return _role;
  }

  public String getResourceKey()
  {
	return _resourceKey;
  }

  @Override
  public String toString()
  {
    return new StringBuilder("uri=").append(_uri).
        append(";role=").append(_role.toString()).
        append(";rk=").append(_resourceKey).toString();
  }

  public String toJsonString()
  {
    StringBuilder sb = new StringBuilder(50);
    sb.append("{");
    sb.append("\"uri\":\""); sb.append(_uri); sb.append("\",");
    sb.append("\"role\":\""); sb.append(_role); sb.append("\"");
    sb.append("}");

    return sb.toString();
  }

  public boolean equalsSource(PhysicalSource other)
  {
	String thisUri = _uri.replaceFirst(":ANY$", ":");
	String otherUri = other._uri.replaceFirst(":ANY$", ":");
    return thisUri.equals(otherUri) && _role.equals(other._role);
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || !(other instanceof PhysicalSource)) return false;
    if (this == other) return true;
    return equalsSource((PhysicalSource)other);
  }

  @Override
  public int hashCode()
  {
    return _uri.hashCode() + _role.hashCode()<<16;
  }

  public boolean isAnySourceWildcard()
  {
    return ANY_PHISYCAL_SOURCE.equals(this);
  }

  public boolean isMasterSourceWildcard()
  {
    return MASTER_PHISYCAL_SOURCE.equals(this);
  }

  public boolean isSlaveSourceWildcard()
  {
    return SLAVE_PHISYCAL_SOURCE.equals(this);
  }

  public boolean isWildcard()
  {
    return isAnySourceWildcard() || isMasterSourceWildcard() || isSlaveSourceWildcard();
  }

  public static class Builder
  {
    private String _uri = ANY_PHISYCAL_SOURCE_URI;
    private String _role = PHYSICAL_SOURCE_MASTER;
    private String _resourceKey = "";

    public String getUri()
    {
      return _uri;
    }

    public void setUri(String uri)
    {
      _uri = uri;
    }

    public String getRole()
    {
      return _role;
    }

    public void setRole(String role)
    {
      _role = role;
    }

    public String getResourceKey()
    {
      return _resourceKey;
    }

    public void setResource(String rk)
    {
      _resourceKey = rk;
    }


    public void makeAnySourceWildcard()
    {
      _uri = ANY_PHISYCAL_SOURCE_URI;
    }

    public void makeMasterSourceWildcard()
    {
      _uri = MASTER_PHISYCAL_SOURCE_URI;
    }

    public void makeSlaveSourceWildcard()
    {
      _uri = SLAVE_PHISYCAL_SOURCE_URI;
    }

    public PhysicalSource build()
    {
      return new PhysicalSource(_uri, _role, _resourceKey);
    }

  }

  /**
   * Converts the physical source to a human-readable string
   * @param   sb        a StringBuilder to accumulate the string representation; if null, a new one will be allocated
   * @return  the StringBuilder
   */
  public StringBuilder toSimpleString(StringBuilder sb)
  {
    if (null == sb)
    {
      sb = new StringBuilder(20);
    }
    sb.append("[");
    if (isAnySourceWildcard())
    {
      sb.append(ANY_PHISYCAL_SOURCE);
    }
    else if (isMasterSourceWildcard())
    {
      sb.append(MASTER_PHISYCAL_SOURCE);
    }
    else if (isSlaveSourceWildcard())
    {
      sb.append(SLAVE_PHISYCAL_SOURCE);
    }
    else
    {
      sb.append("uri=").append(_uri);
    }
    sb.append("]");

    return sb;
  }

  /**
   * Converts the physical source to a human-readable string
   */
  public String toSimpleString()
  {
    return toSimpleString(null).toString();
  }

}
