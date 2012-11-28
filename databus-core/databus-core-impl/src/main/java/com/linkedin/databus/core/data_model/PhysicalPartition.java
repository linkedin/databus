package com.linkedin.databus.core.data_model;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.NamedObject;

/**
 * Represents a Databus physical partition
 *
 * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+2.0+and+Databus+3.0+Data+Model">Databus 2.0 and Databus 3.0 Data Model</a>
 */
public class PhysicalPartition implements NamedObject
{
  private final Integer _id;
  private final String _name;
  private String _simpleStringCache;

  static final Integer ANY_PHYSICAL_PARTITION_ID = -1;
  static final String ANY_PHYSICAL_PARTITION_NAME = "*";
  public static final char DBNAME_PARTID_SEPARATOR = ':';

  public static final PhysicalPartition ANY_PHYSICAL_PARTITION =
      new PhysicalPartition(ANY_PHYSICAL_PARTITION_ID, ANY_PHYSICAL_PARTITION_NAME);

  /** Default constructor for bean compliance and JSON deserialization. Sets to partition to
   * {@link #ANY_PHYSICAL_PARTITION_ID} */
  public PhysicalPartition()
  {
    this(ANY_PHYSICAL_PARTITION_ID, ANY_PHYSICAL_PARTITION_NAME);
  }

  public PhysicalPartition(Integer id, String name) {
    super();
    if (null == id) throw new NullPointerException("id");
    _id = id;
    _name = name;
  }

  public static PhysicalPartition parsePhysicalPartitionString(String pPartString, String del)
  throws IOException
  {
    // format is name<del>id
    String [] parts = pPartString.split(del);
    if(parts.length <= 1)
      throw new IOException("invalid physical source name/id format in " + pPartString + ";del=" + del);

    String idS = parts[parts.length-1];
    Integer id = Integer.parseInt(idS);
    String name = pPartString.substring(0, pPartString.length() - idS.length() - 1);
    if(name.length()<1)
      throw new IOException("invalid physical source name format in " + pPartString + ";del=" + del);

    if(id.intValue()<0)
      throw new IOException("invalid physical source id format in " + pPartString + ";del=" + del);

    return new PhysicalPartition(id, name);
  }

  public static PhysicalPartition createAnyPartitionWildcard()
  {
    return ANY_PHYSICAL_PARTITION;
  }

  public static PhysicalPartition createAnyPartitionWildcard(String dbName)
  {
    return new PhysicalPartition(ANY_PHYSICAL_PARTITION_ID, dbName);
  }

  /**
   * Create a PhysicalPartition object from a JSON string
   * @param  json           the string with JSON serialization of the PhysicalPartition
   */
  public static PhysicalPartition createFromJsonString(String json)
         throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    Builder result = mapper.readValue(json, Builder.class);
    return result.build();
  }

  /** Creates from a string in the format DBNAME:PARTITIONID or just DBNAME for a partition wildcard */
  public static PhysicalPartition createFromSimpleString(String simpleString)
  {
    if (null == simpleString) return null;
    int index = simpleString.indexOf(DBNAME_PARTID_SEPARATOR);
    String dbName = (index < 0) ? simpleString : simpleString.substring(0, index);
    if (dbName.length() < 1) throw new IllegalArgumentException("invalid physical partition string: " + simpleString);

    PhysicalPartition result = null;

    if (index < 0) result = new PhysicalPartition(ANY_PHYSICAL_PARTITION_ID, dbName);
    else
    {
      String idStr = simpleString.substring(index + 1);
      if (idStr.equals("*")) result = new PhysicalPartition(ANY_PHYSICAL_PARTITION_ID, dbName);
      else
      {
        Integer ppartId = -1;
        try
        {
          ppartId = Integer.parseInt(idStr);
        }
        catch (NumberFormatException nfe)
        {
          throw new IllegalArgumentException("invalid physical partition string: " + simpleString);
        }
        result = new PhysicalPartition(ppartId, dbName);
      }
    }

    return result;
  }

  /** The physical partition globally unique id */
  public Integer getId()
  {
    return _id;
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  public String toJsonString()
  {
    StringBuilder sb = new StringBuilder(64);
    sb.append("{\"id\":");
    sb.append(_id.shortValue());
    sb.append(",\"name\":");
    sb.append("\"");
    sb.append(_name);
    sb.append("\"");
    sb.append("}");

    return sb.toString();
  }

  /** Generates a string in the format DBNAME:PARTITIONID or just DBNAME for a partition wildcard */
  public String toSimpleString()
  {
    if (null == _simpleStringCache)
    {
      StringBuilder res = new StringBuilder(_name);
      res.append(DBNAME_PARTID_SEPARATOR);
      if (isAnyPartitionWildcard())
      {
        res.append("*");
      }
      else
      {
        res.append(_id);
      }
      _simpleStringCache = res.toString();
    }

    return _simpleStringCache;
  }

  /** Checks if the object denotes a wildcard */
  public boolean isWildcard()
  {
    return isAnyPartitionWildcard();
  }

  /** Checks if the object denotes a ALL_LOGICAL_SOURCES wildcard */
  public boolean isAnyPartitionWildcard()
  {
    return _id.equals(ANY_PHYSICAL_PARTITION_ID);
  }

  public boolean equalsPartition(PhysicalPartition other)
  {
    return (_id.shortValue() == other._id.shortValue() &&
        _name.equals(other._name));
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || !(other instanceof PhysicalPartition)) return false;
    return equalsPartition((PhysicalPartition)other);
  }

  @Override
  public int hashCode()
  {
    return _id.hashCode()<<16 + _name.hashCode();
  }

  @Override
  /** return name of the partition. Actual meaning of this name depends on the application.
   * For espresso it is db name
   */
  public String getName()
  {
    return _name;
  }

  public static class Builder
  {
    private Integer _id = ANY_PHYSICAL_PARTITION_ID;
    private String _name = ANY_PHYSICAL_PARTITION_NAME;

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
      _name = name;;
    }

    public void makeAnyPartitionWildcard()
    {
      _id = ANY_PHYSICAL_PARTITION_ID;
      _name = ANY_PHYSICAL_PARTITION_NAME;
    }

    public PhysicalPartition build()
    {
      return new PhysicalPartition(_id, _name);
    }

  }

}
