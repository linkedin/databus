package com.linkedin.databus2.relay.config;

import com.linkedin.databus.core.data_model.LogicalPartition;
import com.linkedin.databus.core.data_model.LogicalSource;

public class LogicalSourceStaticConfig
{
  private final short _id;
  private final String _name;
  private final String _uri;
  private final String _partitionFunction;
  //private final short _pid;
  private final LogicalSource _source;
  private final LogicalPartition _partition;
  private final boolean _skipInfinityScn;

  
  private final String _regularQueryHints;
  private final String _chunkedTxnQueryHints;
  private final String _chunkedScnQueryHints;
  
  //////////// STATIC DEFAULTS /////////////////
  // in case no logical partition is specified we assume default 0 (for legacy code)
  private static final LogicalPartition _defaultPartition =
    new LogicalPartition(LogicalSourceConfig.DEFAULT_LOGICAL_SOURCE_PARTITION);
  public static LogicalPartition getDefaultLogicalSourcePartition () {
    return _defaultPartition;
  }


  public LogicalSourceStaticConfig(short id,
                                   String name,
                                   String uri,
                                   String partitionFunction,
                                   short partition,
                                   boolean skipInfinityScn,
                                   String regularQueryHints,
                                   String chunkedTxnQueryHints,
                                   String chunkedScnQueryHints)
  {
    super();
    _id = id;
    _name = name;
    _uri = uri;
    _partitionFunction = partitionFunction;
    _source = new LogicalSource(Integer.valueOf(_id), _name);  // SHOULD be name or uri?
    _partition = new LogicalPartition(partition);
    _skipInfinityScn = skipInfinityScn;
    _regularQueryHints = regularQueryHints;
    _chunkedTxnQueryHints = chunkedTxnQueryHints;
    _chunkedScnQueryHints = chunkedScnQueryHints;
  }

  /** Globally unique source id */
  public short getId()
  {
    return _id;
  }

  public LogicalSource getLogicalSource() {
    return _source;
  }

  /** Fully qualified source name as specified by the Avro schema for the source */
  public String getName()
  {
    return _name;
  }

  /** DB connection URI (table name) */
  public String getUri()
  {
    return _uri;
  }

  /** Partitioning function spec */
  public String getPartitionFunction()
  {
    return _partitionFunction;
  }
  /** get Partition */
  public LogicalPartition getPartition()
  {
    return _partition;
  }


  /**
   * A flag to skip updates with infinity SCN which have not been processed by the DB coalesce
   * job yet. This gets better poll performance at the expense of additional replication latency. */
  public boolean isSkipInfinityScn()
  {
    return _skipInfinityScn;
  }

  public String getRegularQueryHints() {
	  return _regularQueryHints;
  }


  public String getChunkedTxnQueryHints() {
	  return _chunkedTxnQueryHints;
  }


  public String getChunkedScnQueryHints() {
	  return _chunkedScnQueryHints;
  }  
}
