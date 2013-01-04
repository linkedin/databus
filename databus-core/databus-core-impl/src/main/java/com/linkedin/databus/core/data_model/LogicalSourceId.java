package com.linkedin.databus.core.data_model;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Represents a Databus Logical partition
 *
 * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+2.0+and+Databus+3.0+Data+Model">Databus 2.0 and Databus 3.0 Data Model</a>
 * */
public class LogicalSourceId
{
  private final LogicalSource _source;
  private final Short _id;

  static final Short ALL_LOGICAL_PARTITIONS_ID = -1;

  /**
   * Constructor
   * @param  source         the logical source owning the partition
   * @param  partitionId    the unique partition id within the logical source
   */
  public LogicalSourceId(LogicalSource source, Short partitionId)
  {
    super();
    if (null == source) throw new NullPointerException("logical partition source");
    if (null == partitionId) throw new NullPointerException("logical partition id");
    _source = source;
    _id = partitionId;
  }

  /**
   * Create a LogicalSourceId object from a JSON string
   * @param  json           the string with JSON serialization of the LogicalSourceId
   */
  public static LogicalSourceId createFromJsonString(String json)
         throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    Builder result = mapper.readValue(json, Builder.class);
    return result.build();
  }

  /** The source to which this partition belongs to*/
  public LogicalSource getSource()
  {
    return _source;
  }

  /** A unique id for that partition within the logical source; >= 0*/
  public Short getId()
  {
    return _id;
  }

  /** Checks if the object denotes a wildcard */
  public boolean isWildcard()
  {
    return isAllPartitionsWildcard();
  }

  /** Checks if the object denotes a ALL_LOGICAL_PARTITIONS wildcard */
  public boolean isAllPartitionsWildcard()
  {
    return ALL_LOGICAL_PARTITIONS_ID.shortValue() == _id.shortValue();
  }

  /** Creates a ALL_LOGICAL_PARTITIONS wildcard for a given source */
  public static LogicalSourceId createAllPartitionsWildcard(LogicalSource source)
  {
    return new LogicalSourceId(source, ALL_LOGICAL_PARTITIONS_ID);
  }

  public boolean equalsPartition(LogicalSourceId other)
  {
    return (isAllPartitionsWildcard() || other.isAllPartitionsWildcard() ||
            _id.shortValue() == other._id.shortValue()) &&
            _source.equals(other._source);
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || !(other instanceof LogicalSourceId)) return false;
    return equalsPartition((LogicalSourceId)other);
  }

  @Override
  public int hashCode()
  {
    return _id.hashCode() ^ _source.hashCode();
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  public String toJsonString()
  {
    StringBuilder sb = new StringBuilder(100);
    sb.append("{\"source\":");
    sb.append(_source.toString());
    sb.append(",\"id\":");
    sb.append(_id.shortValue());
    sb.append("}");

    return sb.toString();
  }

  public static class Builder
  {
    private LogicalSource.Builder _source = new LogicalSource.Builder();
    private Short _id = ALL_LOGICAL_PARTITIONS_ID;

    public LogicalSource.Builder getSource()
    {
      return _source;
    }

    public void setSource(LogicalSource.Builder source)
    {
      _source = source;
    }

    public Short getId()
    {
      return _id;
    }

    public void setId(Short id)
    {
      _id = id;
    }

    public void makeAllPartitionsWildcard(LogicalSource source)
    {
      _source.setId(source.getId());
      _source.setName(source.getName());
      _id = ALL_LOGICAL_PARTITIONS_ID;
    }

    public LogicalSourceId build()
    {
      return new LogicalSourceId(_source.build(), _id);
    }
  }

}
