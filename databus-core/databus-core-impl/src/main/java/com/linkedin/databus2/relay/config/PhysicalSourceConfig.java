/*
 * $Id: PhysicalSourceConfig.java 272015 2011-05-21 03:03:57Z cbotev $
 */
package com.linkedin.databus2.relay.config;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;


/**
 * Configuration for a physical databus source - typically a database or another relay.
 * A single PhysicalSource can have multiple logical sources. The distinction being that a physical
 * has some sort of sequence generator (SCN if the source DB is Oracle) shared across all logical
 * sources.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 272015 $
 */
public class PhysicalSourceConfig implements ConfigBuilder<PhysicalSourceStaticConfig>
{

  public static final long DEFAULT_DBPOLL_INTERVAL_MILLIS = 500;
  public static final long MAX_DBPOLL_INTERVAL_MILLIS = 5 * 60 * 1000;

  // these default values are used for creating a default partition
  // for cases when no physical partition configuration is present (legacy code)
  public static final String DEFAULT_PHYSICAL_SOURCE_URI = "default_physical_source_uri";
  public static final Integer DEFAULT_PHYSICAL_PARTITION = 0;
  public static final String DEFAULT_PHYSICAL_PARTITION_NAME = "default_partition_name";
  public static final String DEFAULT_PHYSICAL_SOURCE_NAME = "default_physical_config_name";
  public static final String PHYSICAL_SOURCE_BASE_DIR = "databus.relay.base.string";
  public static final String PHYSICAL_SOURCE_BASE_DIR_DEFAULT = ".";

  public static final String DEFAULT_CHUNKING_TYPE = ChunkingType.NO_CHUNKING.toString();
  public static final long DEFAULT_SCN_CHUNK_SIZE = 20000;
  public static final long DEFAULT_TXN_CHUNK_SIZE = 20000;
  public static final long DEFAULT_SCN_CHUNKED_THRESHOLD = 20000;
  public static final long DEFAULT_MAX_SCN_DELAY_MS = 300000;
  public static final int DEFAULT_LARGEST_EVENT_SIZE = 1 * 1024*1024; //1MB
  public static final long DEFAULT_LARGEST_WINDOW_SIZE = 5*1024*1024; //5MB

  private String _name; // for example - database name
  private int _id;      // physical partition
  private String _uri;  // physical machne URI
  private String _resourceKey; // resource key which was used in case of Cluster manager
  private String _role;
  private long _slowSourceQueryThreshold;
  private long _restartScnOffset;
  private List<LogicalSourceConfig> _sources;
  private BackoffTimerStaticConfigBuilder _retries;
  private String _chunkingType;
  private long _txnsPerChunk;
  private long _scnChunkSize;
  private long _chunkedScnThreshold;
  private long _maxScnDelayMs;
  private  DbusEventBuffer.Config _dbusEventBuffer;
  // Used mainly by simulator
  // Used for GG relays to throttle rate at which events are read from trail files
  private long _eventRatePerSec;
  // Used for GG relays to specify the duration for which throttling be in effect after startup
  // When not specified, or equal to zero means that throttling is disabled
  private long _maxThrottleDurationInSecs;

 //used by chained relays to estimate , but could be used by regular relays for appropriate dynamic sizing or checks
  private int _largestEventSizeInBytes;
  private long _largestWindowSizeInBytes;

  //Golden gate relay specific configs
  /**
   *   If _errorOnMissingFields set to true, then the parser will terminate on missing fields in xmlTrail file,
   *   if set to false, then the parser will not terminate on missing fields (will use null value for the missing fields),
   *   provided it's NOT the primary key.
   */
  private boolean _errorOnMissingFields;
  /**
   * The config is used to specify what would be the xml version that the trail files are based on. The default value is 1.1
   * Accepts 1.0/1.1
   */
  private String _xmlVersion;
  /**
   * The type of encoding used by the xml, the default value for this config is ISO-8859-1
   */
  private String _xmlEncoding;

  /**
   * Config for deciding if an event is replicated or not !!
   */
  private ReplicationBitSetterConfig _replBitSetter;

  public PhysicalSourceConfig()
  {
    _id = 0;
    _resourceKey = "";
    _role = PhysicalSource.PHYSICAL_SOURCE_MASTER;
    _sources = new ArrayList<LogicalSourceConfig>();
    _restartScnOffset = 0;
    _slowSourceQueryThreshold = 3000;
    _retries = new BackoffTimerStaticConfigBuilder();
    _retries.setInitSleep(DEFAULT_DBPOLL_INTERVAL_MILLIS);
    _retries.setMaxRetryNum(-1);
    _retries.setMaxSleep(MAX_DBPOLL_INTERVAL_MILLIS);
    _retries.setSleepIncFactor(2.0);
    _retries.setSleepIncDelta(0);
    _chunkingType = DEFAULT_CHUNKING_TYPE;
    _txnsPerChunk = DEFAULT_TXN_CHUNK_SIZE;
    _scnChunkSize = DEFAULT_SCN_CHUNK_SIZE;
    _chunkedScnThreshold = DEFAULT_SCN_CHUNKED_THRESHOLD;
    _maxScnDelayMs = DEFAULT_MAX_SCN_DELAY_MS;
    _largestEventSizeInBytes = DEFAULT_LARGEST_EVENT_SIZE;
    _largestWindowSizeInBytes = DEFAULT_LARGEST_WINDOW_SIZE;
    _eventRatePerSec=10;
    _maxThrottleDurationInSecs = 0;
    _errorOnMissingFields = true;
    _dbusEventBuffer = null; //This buffer is per physical source, if not initialized in multi-tenant source config, the global eventbuffer config is used.
    _xmlEncoding = "ISO-8859-1";
    _replBitSetter = new ReplicationBitSetterConfig();
    _xmlVersion = "1.0";
  }

  /** create a PhysicalSourceConfiguration without any logical sources
   * use addSource(LogicalSourceConfig s) to add logical source
   */
  public PhysicalSourceConfig(String pSourceName, String pUri, int pPartionId) {
    this();

    setId(pPartionId);
    setName(pSourceName);
    setUri(pUri);
  }

  /** create physical source with some default values
   *  CAUTION - this createas a default physical partition (0)
   *  please use only in case when actual physical partition parameters are unavailable
   **/
  public PhysicalSourceConfig(Collection<IdNamePair> srcIds) {
    this();

    List<LogicalSourceConfig> newSources =
        new ArrayList<LogicalSourceConfig>(srcIds.size());

    for(IdNamePair p : srcIds) {
      LogicalSourceConfig source =  new LogicalSourceConfig();
      source.setId(p.getId().shortValue());
      source.setName(p.getName());
      // these two shouldn't ever be used
      source.setPartitionFunction("DefaultPartition");
      source.setUri("defaultUri");
      newSources.add(source);
    }
    setSources(newSources);
    setName("DefaultSource");
    setId(DEFAULT_PHYSICAL_PARTITION);
    setUri(DEFAULT_PHYSICAL_SOURCE_URI);
  }

  /** create physical source with some default values
   *  CAUTION - this createas a default physical partition (0)
   *  please use only in case when actual physical partition parameters are unavailable
   **/
  public static PhysicalSourceConfig createFromLogicalSources(Collection<LogicalSource> srcIds) {
    PhysicalSourceConfig res = new PhysicalSourceConfig();

    List<LogicalSourceConfig> newSources =
        new ArrayList<LogicalSourceConfig>(srcIds.size());

    for(LogicalSource p : srcIds) {
      LogicalSourceConfig source =  new LogicalSourceConfig();
      source.setId(p.getId().shortValue());
      source.setName(p.getName());
      // these two shouldn't ever be used
      source.setPartitionFunction("DefaultPartition");
      source.setUri("defaultUri");
      newSources.add(source);
    }
    res.setSources(newSources);
    res.setName(DEFAULT_PHYSICAL_PARTITION_NAME);
    res.setId(DEFAULT_PHYSICAL_PARTITION);
    res.setUri(DEFAULT_PHYSICAL_SOURCE_URI);

    return res;
  }

  /**
   * Check that none of the configuration settings are null.
   * @throws InvalidConfigException if one or more settings are null
   */
  public void checkForNulls()
      throws InvalidConfigException
      {
    if(_name == null || _name.length() == 0)
    {
      throw new InvalidConfigException("Name cannot be null or empty.");
    }

    if(_id < 0)
      throw new  InvalidConfigException("Physical source id cannot be empty.");

    if(_uri == null || _name.length() == 0)
    {
      throw new InvalidConfigException("URI cannot be null or empty.");
    }

    if(_sources == null || _sources.size() == 0)
    {
      throw new InvalidConfigException("Sources cannot be null or empty.");
    }

    for(LogicalSourceConfig source : _sources)
    {
      source.checkForNulls();
    }
    if (_slowSourceQueryThreshold < 0) {
      _slowSourceQueryThreshold = 0;
    }
      }

  public String getUri()
  {
    return _uri;
  }

  public void setUri(String uri)
  {
    _uri = uri;
  }

  public int getId()
  {
    return _id;
  }

  public void setId(int id)
  {
    _id = id;
  }

  public void setRole(String role) {
    _role = role;
  }

  public String getRole() {
    return _role;
  }

  public void setResourceKey(String rk) {
    _resourceKey = rk;
  }

  public String getResourceKey() {
    return _resourceKey;
  }

  private LogicalSourceConfig addOrGetSource(int index)
  {
    if (index >= _sources.size())
    {
      for (int i = _sources.size(); i <= index; ++i)
      {
        _sources.add(new LogicalSourceConfig());
      }
    }

    return _sources.get(index);
  }

  public LogicalSourceConfig getSource(int index)
  {
    LogicalSourceConfig result = addOrGetSource(index);
    return result;
  }

  public void setSource(int index, LogicalSourceConfig source)
  {
    addOrGetSource(index);
    _sources.set(index, source);
  }

  public void addSource(LogicalSourceConfig source) {
    if(! _sources.contains(source))
      _sources.add(source);
  }

  public List<LogicalSourceConfig> getSources()
  {
    return _sources;
  }

  public void setSources(List<LogicalSourceConfig> newSources)
  {
    _sources = newSources;
  }

  public String getName()
  {
    return _name;
  }

  public void setName(String name)
  {
    _name = name;
  }

  public long getSlowSourceQueryThreshold()
  {
    return _slowSourceQueryThreshold;
  }

  public void setSlowSourceQueryThreshold(long slowSourceQueryThreshold)
  {
    _slowSourceQueryThreshold = slowSourceQueryThreshold;
  }

  public long getRestartScnOffset() {
    return _restartScnOffset;
  }

  public void setRestartScnOffset(long r) {
    _restartScnOffset = r;
  }




  public int getLargestEventSizeInBytes() {
	  return _largestEventSizeInBytes;
  }

  public void setLargestEventSizeInBytes(int largestEventSizeInBytes) {
	  _largestEventSizeInBytes = largestEventSizeInBytes;
  }

  public long getLargestWindowSizeInBytes() {
	  return _largestWindowSizeInBytes;
  }

  public void setLargestWindowSizeInBytes(long largestWindowSizeInBytes) {
	  _largestWindowSizeInBytes = largestWindowSizeInBytes;
  }


  public boolean getErrorOnMissingFields()
  {
    return _errorOnMissingFields;
  }

  public void setErrorOnMissingFields(boolean errorOnMissingFields)
  {
    _errorOnMissingFields = errorOnMissingFields;
  }

  @Override
  public String toString()
  {
    try
    {
      ObjectMapper mapper = new ObjectMapper();
      StringWriter writer = new StringWriter();
      mapper.writeValue(writer, this);
      JSONObject jsonObj = new JSONObject(writer.toString());

      //The getMethod on this _dbusEventBuffer should be only called by the ConfigLoader
      if(_dbusEventBuffer!=null)
        {
          ObjectMapper mapperDbus = new ObjectMapper();
          StringWriter writerDbus = new StringWriter();
          mapperDbus.writeValue(writerDbus, _dbusEventBuffer);
          jsonObj.put("dbusEventBuffer", new JSONObject(writerDbus.toString()));
        }
      else
        jsonObj.put("dbusEventBuffer", JSONObject.NULL);

      return jsonObj.toString();
    }
    catch(Exception ex)
    {
      ex.printStackTrace();
      // Should never happen, but the ObjectMapper could throw an Exception.
      return super.toString();
    }
  }

  /**
   * Converse of toString; populate this object from the String
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public static PhysicalSourceConfig fromString(String str) throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    PhysicalSourceConfig config = mapper.readValue(str,PhysicalSourceConfig.class);
    return config;
  }



  /**
   * Converse of toString; populate this object from a File handle pointing to a json file
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public static PhysicalSourceConfig fromFile(File f) throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    PhysicalSourceConfig config = mapper.readValue(f,PhysicalSourceConfig.class);
    return config;
  }


  /**
   * Represents obj in map; rather obj->json->map
   * @return Map representation of object
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public Map<String,Object> toMap() throws JsonParseException, JsonMappingException, IOException
  {
    String str = toString();
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String,Object> map = mapper.readValue(str, HashMap.class);
    return map;
  }

  /**
   * Given a map of key value pairs; keys should correspond to field names ; return an object. Used to read configuration specification
   * @param map
   * @return
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static PhysicalSourceConfig fromMap(Map<String,Object> map) throws JsonGenerationException, JsonMappingException, IOException
  {

    ObjectMapper mapper = new ObjectMapper();
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, map);
    String str =  writer.toString();
    return fromString(str);
  }


  @Override
  public PhysicalSourceStaticConfig build() throws InvalidConfigException
  {
    checkForNulls();
    //check config options for chained relays
    if (_largestEventSizeInBytes >= _largestWindowSizeInBytes)
    {
      throw new InvalidConfigException("Invalid relay config: largestEventSizeInBytes has to be lesser than largestWindowSizeInBytes:"
          + " largestEventSizeInBytes=" + _largestEventSizeInBytes + " largestWindowSizeInBytes=" + _largestWindowSizeInBytes);
    }

    LogicalSourceStaticConfig[] sourcesStaticConfigs = new LogicalSourceStaticConfig[_sources.size()];
    for (int i = 0 ; i < _sources.size(); ++i)
    {
      sourcesStaticConfigs[i] = _sources.get(i).build();
    }
    ChunkingType chunkingType = ChunkingType.valueOf(_chunkingType);
    return new PhysicalSourceStaticConfig(_name, _id, _uri, _resourceKey,
                                          sourcesStaticConfigs, _role,
                                          _slowSourceQueryThreshold,
                                          _restartScnOffset,
                                          _retries.build(),
                                          chunkingType,
                                          _txnsPerChunk,
                                          _scnChunkSize,
                                          _chunkedScnThreshold,
                                          _maxScnDelayMs,
                                          _eventRatePerSec,
                                          _maxThrottleDurationInSecs,
                                          isDbusEventBufferSet()?_dbusEventBuffer.build():null,
                                          _largestEventSizeInBytes,
                                          _largestWindowSizeInBytes,
                                          _errorOnMissingFields,
                                          _xmlVersion,
                                          _xmlEncoding,
                                          _replBitSetter.build());
  }

  public BackoffTimerStaticConfigBuilder getRetries()
  {
    return _retries;
  }

  public void setRetries(BackoffTimerStaticConfigBuilder retries)
  {
    _retries = retries;
  }

  public String getChunkingType() {
	  return _chunkingType;
  }

  public void setChunkingType(String chunkingType) {
	  this._chunkingType = chunkingType;
  }

  public long getTxnsPerChunk() {
	  return _txnsPerChunk;
  }

  public void setTxnsPerChunk(long txnsPerChunk) {
	  this._txnsPerChunk = txnsPerChunk;
  }

  public long getScnChunkSize() {
	  return _scnChunkSize;
  }

  public void setScnChunkSize(long scnChunkSize) {
	  this._scnChunkSize = scnChunkSize;
  }

  public long getChunkedScnThreshold() {
	  return _chunkedScnThreshold;
  }

  public void setChunkedScnThreshold(long chunkedScnThreshold) {
	  this._chunkedScnThreshold = chunkedScnThreshold;
  }

  public long getMaxScnDelayMs() {
	return _maxScnDelayMs;
  }

  public void setMaxScnDelayMs(long maxScnDelayMs) {
	this._maxScnDelayMs = maxScnDelayMs;
  }

  public long getEventRatePerSec()
  {
	  return _eventRatePerSec;
  }

  public void setEventRatePerSec(long eventRatePerSec)
  {
	  _eventRatePerSec = eventRatePerSec;
  }

  public long getMaxThrottleDurationInSecs()
  {
    return _maxThrottleDurationInSecs;
  }

  public void setMaxThrottleDurationInSecs(long maxThrottleDurationInSecs)
  {
    _maxThrottleDurationInSecs = maxThrottleDurationInSecs;
  }

  @JsonIgnore
  public DbusEventBuffer.Config getDbusEventBuffer() {

    if(_dbusEventBuffer == null)
      _dbusEventBuffer = new DbusEventBuffer.Config();

    return _dbusEventBuffer;
  }

  public void setDbusEventBuffer(DbusEventBuffer.Config _dbusEventBuffer) {
    this._dbusEventBuffer = _dbusEventBuffer;
  }

  @JsonIgnore
  public boolean isDbusEventBufferSet()
  {
    return _dbusEventBuffer!=null?true:false;
  }

  public String getXmlVersion()
  {
    return _xmlVersion;
  }

  public void setXmlVersion(String xmlVersion)
  {
    _xmlVersion = xmlVersion;
  }

  public String getXmlEncoding()
  {
    return _xmlEncoding;
  }

  public void setXmlEncoding(String xmlEncoding)
  {
    _xmlEncoding = xmlEncoding;
  }

  public ReplicationBitSetterConfig getReplBitSetter()
  {
    return _replBitSetter;
  }

  public void setReplBitSetter(ReplicationBitSetterConfig replBitSetter)
  {
    this._replBitSetter = replBitSetter;
  }
}
