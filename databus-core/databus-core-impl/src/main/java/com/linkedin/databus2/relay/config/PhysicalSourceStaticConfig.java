package com.linkedin.databus2.relay.config;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.StringUtils;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

public class PhysicalSourceStaticConfig
{
  private final String _name;
  private final int _id;
  private final PhysicalPartition _partiton;
  private final String _uri;
  private final PhysicalSource _source;
  private final LogicalSourceStaticConfig[] _sources;
  private final long _slowSourceQueryThreshold;
  private final long _restartScnOffset;
  private final BackoffTimerStaticConfig _retries;
  private final String _resourceKey;
  private final String _role;
  private final DbusEventBuffer.StaticConfig _dbusEventBuffer;
  private final long _eventRatePerSec;
  // Applicable only for GG relays
  private final long _maxThrottleDurationInSecs;

  private final ChunkingType _chunkingType;
  private final long _txnsPerChunk;
  private final long _scnChunkSize;
  private final long _chunkedScnThreshold;
  private final long _maxScnDelayMs;

  private final int _largestEventSizeInBytes;
  private final long _largestWindowSizeInBytes;
  private final boolean _errorOnMissingFields;
  private final String _xmlVersion;
  private final String _xmlEncoding;
  private final ReplicationBitSetterStaticConfig _replBitSetter;

  /////////// DEFAULT VALUES ////////////////////
  private static final PhysicalSource _defaultSource;
  private static final PhysicalPartition _defaultPartition;
  static {
    _defaultSource = new PhysicalSource(PhysicalSourceConfig.DEFAULT_PHYSICAL_SOURCE_URI);
    _defaultPartition = new PhysicalPartition(PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION,
                                              PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);
  };

  /**
   *
   * NO_CHUNKING     => Use regular query to fetch delta changes.
   * TXN_CHUNKING    => Use txn based chunk query to fetch delta changes.
   * SCN_CHUNKING    => Use scn based chunk query to fetch delta changes.
   *
   */
  public enum ChunkingType
  {
	  NO_CHUNKING,
	  TXN_CHUNKING,
	  SCN_CHUNKING;

	  public boolean isChunkingEnabled()
	  {
		  return (! (this.equals(NO_CHUNKING)));
	  }
  };

  public static PhysicalSource getDefaultPhysicalSource() {
    return _defaultSource;
  }
  public static PhysicalPartition getDefaultPhysicalPartition() {
    return _defaultPartition;
  }
  /////////////////////////////////////////////////


  public PhysicalSourceStaticConfig(String name,
  	                                int id,
                                    String uri,
                                    String resourceKey,
                                    LogicalSourceStaticConfig[] sources,
                                    String role,
                                    long slowSourceQueryThreshold,
                                    long restartScnOffset,
                                    BackoffTimerStaticConfig errorRetries,
                                    ChunkingType chunkingType,
                                    long txnsPerChunk,
                                    long scnChunkSize,
                                    long chunkedScnThreshold,
                                    long maxScnDelayMs,
                                    long eventRatePerSec,
                                    long maxThrottleDurationInSecs,
                                    DbusEventBuffer.StaticConfig dbusEventBuffer,
                                    int largestEventSizeInBytes,
                                    long largestWindowSizeInBytes,
                                    boolean errorOnMissingFields,
                                    String xmlVersion,
                                    String xmlEncoding,
                                    ReplicationBitSetterStaticConfig replicationBitSetter)
  {
    super();
    _name = name;
    _id = id;
    _partiton = new PhysicalPartition(id, _name);
    _uri = uri;
    _source = new PhysicalSource(uri, role, resourceKey);
    _sources = sources;
    _slowSourceQueryThreshold = slowSourceQueryThreshold;
    _restartScnOffset = restartScnOffset;
    _retries = errorRetries;
    _resourceKey = resourceKey;
    _role = role;
    _chunkingType = chunkingType;
    _txnsPerChunk = txnsPerChunk;
    _scnChunkSize = scnChunkSize;
    _chunkedScnThreshold = chunkedScnThreshold;
    _maxScnDelayMs = maxScnDelayMs;
    _eventRatePerSec=eventRatePerSec;
    _maxThrottleDurationInSecs=maxThrottleDurationInSecs;
    _dbusEventBuffer = dbusEventBuffer;
    _largestEventSizeInBytes = largestEventSizeInBytes;
    _largestWindowSizeInBytes = largestWindowSizeInBytes;
    _errorOnMissingFields = errorOnMissingFields;
    _xmlEncoding = xmlEncoding;
    _xmlVersion = xmlVersion;
    _replBitSetter = replicationBitSetter;
  }

  /** role, if any */
  public String getRole() {
    return _role;
  }

  /** resource key used to create the buffer, if any */
  public String getResourceKey() {
    return _resourceKey;
  }

  /** The unique name of the source */
  public String getName()
  {
    return _name;
  }

  /** The unique id of the source */
  public int getId()
  {
  	return _id;
  }
  public PhysicalPartition getPhysicalPartition() {
    return _partiton;
  }

  /** Database JDBC connection URI */
  public String getUri()
  {
    return _uri;
  }

  /** get PhysicalSource */
  public PhysicalSource getPhysicalSource()
  {
    return _source;
  }

  /** Configuration of the logical Databus sources available in the physical database. */
  public LogicalSourceStaticConfig[] getSources()
  {
    return _sources;
  }

  /**
   * The max time in ms with no updates after which the database event puller switches to slow
   * source mode. */
  public long getSlowSourceQueryThreshold()
  {

    return _slowSourceQueryThreshold;
  }

  /**
   * The delta to rollback the last scn read on relay restart. This is meant to not start the
   * relay with an empty buffer.
   *
   * The value is ignored if it is <= 0.
   * */
  public long getRestartScnOffset()
  {
    return _restartScnOffset;
  }

  /** DB poll and error retries configuration */
  public BackoffTimerStaticConfig getRetries()
  {
    return _retries;
  }

  public ChunkingType getChunkingType() {
	  return _chunkingType;
  }

  public long getTxnsPerChunk() {
	  return _txnsPerChunk;
  }

  public long getScnChunkSize() {
	  return _scnChunkSize;
  }

  public long getChunkedScnThreshold() {
	  return _chunkedScnThreshold;
  }

  public long getMaxScnDelayMs() {
	return _maxScnDelayMs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("name=").append(_name).append(";part=").append(_partiton).append(";uri=")
    .append(StringUtils.sanitizeDbUri(_uri))
    .append(";role=").append(_role).append(";rsKey=").append(_resourceKey).append(";#src=")
    .append(_sources.length);

    return sb.toString();
  }

  /** Return expected event rate per sec */
  public long getEventRatePerSec() {
	  return _eventRatePerSec;
  }
  public long getMaxThrottleDurationInSecs() {
    return _maxThrottleDurationInSecs;
  }
  public int getLargestEventSizeInBytes() {
	  return _largestEventSizeInBytes;
  }
  public long getLargestWindowSizeInBytes() {
	  return _largestWindowSizeInBytes;
  }

  public DbusEventBuffer.StaticConfig getDbusEventBuffer() {
	  return _dbusEventBuffer;
  }

  public boolean isDbusEventBufferSet()
  {
	  return _dbusEventBuffer != null? true: false;
  }

  public boolean getErrorOnMissingFields()
  {
    return _errorOnMissingFields;
  }

  public String getXmlVersion()
  {
    return _xmlVersion;
  }

  public String getXmlEncoding()
  {
    return _xmlEncoding;
  }

  public ReplicationBitSetterStaticConfig getReplBitSetter()
  {
    return _replBitSetter;
  }
}
