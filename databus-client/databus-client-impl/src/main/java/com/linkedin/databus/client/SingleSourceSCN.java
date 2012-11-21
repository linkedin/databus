package com.linkedin.databus.client;

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
      catch (Exception e)
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