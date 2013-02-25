/*
 * $Id: LogicalSourceConfig.java 168967 2011-02-25 21:56:00Z cbotev $
 */
package com.linkedin.databus2.relay.config;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 168967 $
 */
public class LogicalSourceConfig implements ConfigBuilder<LogicalSourceStaticConfig>
{
  // in case logical partition is not specified - we assume value 0 (for legacy code)
  public static final Short DEFAULT_LOGICAL_SOURCE_PARTITION = 0;
  public static final String DEFAULT_SCN_CHUNKING_HINTS = "/*+ first_rows LEADING(tx) */";
  public static final String DEFAULT_ROW_CHUNKING_HINTS = "/*+ first_rows LEADING(tx) cardinality(tx,1) */";
  public static final String DEFAULT_EVENT_QUERY_HINTS = "/*+ first_rows LEADING(tx) */";

  private short _id = -1;
  private String _name;
  private String _uri;
  private String _partitionFunction;
  private short _partition;
  private boolean _skipInfinityScn = false;
  private String _regularQueryHints = DEFAULT_EVENT_QUERY_HINTS;
  private String _chunkedTxnQueryHints = DEFAULT_ROW_CHUNKING_HINTS;
  private String _chunkedScnQueryHints = DEFAULT_SCN_CHUNKING_HINTS;
  
  
  /**
   * Check that none of the configuration settings are null.
   * @throws InvalidConfigException if one or more settings are null
   */
  public void checkForNulls()
  throws InvalidConfigException
  {
    if(_id < 0)
    {
      throw new InvalidConfigException("ID cannot be null. Must be >= 0.");
    }
    if(_name == null || _name.length() == 0)
    {
      throw new InvalidConfigException("Name cannot be null or empty.");
    }
    if(_uri == null || _uri.length() == 0)
    {
      throw new InvalidConfigException("Schema cannot be null or empty.");
    }
    if(_partitionFunction == null || _partitionFunction.length() == 0)
    {
      throw new InvalidConfigException("PartitionFunction cannot be null or empty.");
    }
    
    
  }

  public short getId()
  {
    return _id;
  }
  public void setId(short id)
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
  }
  public String getPartitionFunction()
  {
    return _partitionFunction;
  }
  public void setPartitionFunction(String partitionFunction)
  {
    _partitionFunction = partitionFunction;
  }
  public String getUri()
  {
    return _uri;
  }
  public void setUri(String uri)
  {
    _uri = uri;
  }

 public void setPartition(short partition)
  {
    _partition = partition;
  }
  public short getPartition()
  {
    return _partition;
  }

  /*
  @Override
  public String toString()
  {

    try
    {
      ObjectMapper mapper = new ObjectMapper();
      StringWriter writer = new StringWriter();
      mapper.writeValue(writer, this);
      return writer.toString();
    }
    catch(Exception ex)
    {
      // Should never happen, but the ObjectMapper could throw an Exception.
      return super.toString();
    }
  }
*/
  @Override
  public LogicalSourceStaticConfig build() throws InvalidConfigException
  {
    checkForNulls();
    return new LogicalSourceStaticConfig(_id, _name, _uri, _partitionFunction, _partition,
                                         _skipInfinityScn, _regularQueryHints, _chunkedTxnQueryHints, _chunkedScnQueryHints);
  }

  public boolean isSkipInfinityScn()
  {
    return _skipInfinityScn;
  }

  public void setSkipInfinityScn(boolean skipInfScn)
  {
    _skipInfinityScn = skipInfScn;
  }

  public String getRegularQueryHints() {
	  return _regularQueryHints;
  }

  public void setRegularQueryHints(String regularQueryHints) {
	  this._regularQueryHints = regularQueryHints;
  }

  public String getChunkedTxnQueryHints() {
	  return _chunkedTxnQueryHints;
  }

  public void setChunkedTxnQueryHints(String chunkedTxnQueryHints) {
	  this._chunkedTxnQueryHints = chunkedTxnQueryHints;
  }

  public String getChunkedScnQueryHints() {
	  return _chunkedScnQueryHints;
  }

  public void setChunkedScnQueryHints(String chunkedScnQueryHints) {
	  this._chunkedScnQueryHints = chunkedScnQueryHints;
  }  
}
