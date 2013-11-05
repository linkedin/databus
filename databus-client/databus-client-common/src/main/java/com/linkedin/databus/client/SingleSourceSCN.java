package com.linkedin.databus.client;
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
import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.client.pub.SCN;

/** An SCN implementation for a single databus source */
public class SingleSourceSCN implements SCN
{
  private static final long serialVersionUID = 1L;

  /** An identifier of the source */
  private final int _sourceId;
  /** The sequence number of the event window the change is part of */
  private final long _sequence;
  /** Cache the json representation */
  private String _jsonString = null;
  

  
  public SingleSourceSCN(int sourceId, long sequence)
  {
    _sequence = sequence;
    _sourceId = sourceId;
  }

  @Override
  public int compareTo(SCN otherSCN)
  {
    if (null == otherSCN || ! isComparable(otherSCN)) return -1;

    SingleSourceSCN other =(SingleSourceSCN)otherSCN;
    return (int)(getSequence() - other.getSequence());
  }

  @Override
  public boolean equals(Object o)
  {
    if (null == o || !(o instanceof SingleSourceSCN)) return false;
    SingleSourceSCN other = (SingleSourceSCN)o;
    return getSequence() == other.getSequence();
  }

  @Override
  public int hashCode()
  {
	  return (int)(getSequence());
  }
  
  @Override
  public boolean isComparable(SCN otherSCN)
  {
    if (! (otherSCN instanceof SingleSourceSCN)) return false;

    SingleSourceSCN other =(SingleSourceSCN)otherSCN;
    return (getSourceId() == other.getSourceId());
  }

  public String toJsonString()
  {
    if (null == _jsonString)
    {
      StringWriter jsonBuilder = new StringWriter(1000);
      ObjectMapper mapper = new ObjectMapper();

      try
      {
        mapper.writeValue(jsonBuilder, this);
        _jsonString = jsonBuilder.toString();
      }
      catch (IOException e)
      {
        _jsonString = "Serialization error";
      }
      catch (RuntimeException e)
      {
        _jsonString = "Serialization error";
      }
    }

    return _jsonString;
  }

  
  @Override
  public String toString()
  {
    return toJsonString();
  }

  public int getSourceId()
  {
    return _sourceId;
  }

  public long getSequence()
  {
    return _sequence;
  }
}
