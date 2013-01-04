package com.linkedin.databus.core;


import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalPartition;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BufferNotFoundException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;
import com.linkedin.databus2.core.filter.ConjunctionDbusFilter;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.LogicalSourceAndPartitionDbusFilter;
import com.linkedin.databus2.core.filter.PhysicalPartitionDbusFilter;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class DbusEventBufferMult
{
  public static final String MODULE = DbusEventBufferMult.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String PERF_MODULE = MODULE + "Perf";
  public static final Logger PERF_LOG = Logger.getLogger(PERF_MODULE);

  // set of unique buffers
  final private Set<DbusEventBuffer> _uniqBufs = new HashSet<DbusEventBuffer>();

  // physical key to a buffer mapping
  final private Map<PhysicalPartitionKey, DbusEventBuffer> _bufsMap =
    new HashMap<PhysicalPartitionKey, DbusEventBuffer>();

  // partition to physical sources mapping
  final private Map<PhysicalPartitionKey, Set<PhysicalSource>> _partKey2PhysiscalSources =
      new HashMap<PhysicalPartitionKey, Set<PhysicalSource>>();

  // logical partition to physical partition mapping
  final private Map<LogicalPartitionKey, PhysicalPartitionKey> _logicalPKey2PhysicalPKey =
    new HashMap<LogicalPartitionKey, PhysicalPartitionKey>();

  // when passed only logical source id - need to find corresponding LogicalSource
  final private Map<Integer, LogicalSource> _logicalId2LogicalSource =
    new HashMap<Integer, LogicalSource>();

  private File _mmapDirectory = null;


  // specify if we want to drop SCN less then current when adding new events to this buffers
  boolean _dropOldEvents = false;

  private final double _nanoSecsInMSec = 1000000.0;
  public static final String BAK_DIRNAME_SUFFIX = ".BAK";

  // construction
  public DbusEventBufferMult() {
    // creates empty mult buffer, new buffers can be added later
  }

  // we should keep a set of all the buffers
  // we should build mapping based on unique physical source ids
  public DbusEventBufferMult(PhysicalSourceStaticConfig [] pConfigs, DbusEventBuffer.StaticConfig config) throws InvalidConfigException {

    if(pConfigs == null) {
      // if we expect to get partitions configs from relay - we can create and EMPTY relay
      LOG.warn("Creating empty MULT buffer. No pConfigs passed");
      return;
    }

    LOG.info("Creating new DbusEventBufferMult for " + pConfigs.length + " physical configurations");
    for(PhysicalSourceStaticConfig pConfig : pConfigs) {
      addNewBuffer(pConfig, config);
    }
    if (config.getAllocationPolicy() == DbusEventBuffer.AllocationPolicy.MMAPPED_MEMORY)
    {
      _mmapDirectory = config.getMmapDirectory();
    }
  }

  /** getOnebuffer by logical source args */
  public DbusEventBuffer getOneBuffer(LogicalSource lSource,
                                       LogicalPartition lPartition) {
    if(lPartition == null)
      lPartition = LogicalSourceStaticConfig.getDefaultLogicalSourcePartition();

    if(lSource == null)
      throw new IllegalArgumentException("cannot find buffer without source");

    LogicalPartitionKey lKey = new LogicalPartitionKey(lSource, lPartition);

    PhysicalPartitionKey pKey = _logicalPKey2PhysicalPKey.get(lKey);
    return _bufsMap.get(pKey);
  }

  /** getOneBuffer by physical partiton */
  public DbusEventBuffer getOneBuffer( PhysicalPartition pPartition) {

    if(pPartition == null)
      pPartition = PhysicalSourceStaticConfig.getDefaultPhysicalPartition();

    PhysicalPartitionKey key = new PhysicalPartitionKey(pPartition);
    return _bufsMap.get(key);
  }

  public void resetBuffer(PhysicalPartition pPartition, long prevScn)
  throws BufferNotFoundException
  {
    DbusEventBuffer buf = getOneBuffer(pPartition);
    if(buf == null) {
      throw new BufferNotFoundException("cannot find buf for partition " + pPartition);
    }
    buf.reset(prevScn);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  //****************
  // public apis
  //****************
  /** get appendable for single Physical source id (all partitions) */
  public DbusEventBufferAppendable getDbusEventBufferAppendable(PhysicalPartition pPartition) {
    return getOneBuffer(pPartition);
  }

  /** get appendable for single Logical source id (all partitions) */
  public DbusEventBufferAppendable getDbusEventBufferAppendable(int lSrcId) {
    LogicalSource lSource = _logicalId2LogicalSource.get(lSrcId);
    return getDbusEventBufferAppendable(lSource);
  }
  /** get appendable for single Logical source (all partitions) */
  public DbusEventBufferAppendable getDbusEventBufferAppendable(LogicalSource lSource) {
    return getDbusEventBufferAppendable(lSource, null);
  }

  /** get appendable for single Logical source (all partitions) */
  public DbusEventBufferAppendable getDbusEventBufferAppendable(LogicalSource lSource,
                                                                LogicalPartition lPartition) {
    return getOneBuffer(lSource, lPartition);
  }

  /** get appendable for physical source/partion pair
  public DbusEventBufferAppendable getDbusEventBufferAppendable(PhysicalSource pSource,
                                             PhysicalPartition pPartition) {
    return getDbusEventBufferAppendable(pPartition);
  }*/

  /** get dbusEventBuffer directly by LSource */
  public DbusEventBuffer getDbusEventBuffer(LogicalSource lSource) {
    return getOneBuffer(lSource, null);
  }

  public DbusEventBufferBatchReadable getDbusEventBufferBatchReadable(CheckpointMult cpMult,
            Set<PhysicalPartitionKey> ppartKeys, StatsCollectors<DbusEventsStatisticsCollector> statsCollectors)
  throws IOException
  {
    return new DbusEventBufferBatchReader(cpMult, ppartKeys, statsCollectors);
  }

  public DbusEventBufferBatchReadable getDbusEventBufferBatchReadable(Collection<Integer> ids,
		  CheckpointMult cpMult, StatsCollectors<DbusEventsStatisticsCollector> statsCollector)

  throws IOException
  {
	  return new DbusEventBufferBatchReader(ids, cpMult, statsCollector);
  }


  public DbusEventBufferBatchReadable getDbusEventBufferBatchReadable(
         CheckpointMult cpMult,
         Collection<PhysicalPartitionKey> physicalPartitions,
         StatsCollectors<DbusEventsStatisticsCollector> statsCollector)
         throws IOException
  {
    return new DbusEventBufferBatchReader(cpMult, physicalPartitions, statsCollector);
  }

  /** get physical partition corresponding to this src and DEFAULT logical partition */
  public PhysicalPartition getPhysicalPartition(int srcId ) {
    return getPhysicalPartition(
                         srcId,
                         new LogicalPartition(LogicalSourceConfig.DEFAULT_LOGICAL_SOURCE_PARTITION));
  }

  /** get physical partition mapping by logical source */
  public PhysicalPartition getPhysicalPartition(int srcId, LogicalPartition lPartition ) {
    LogicalSource lSource = _logicalId2LogicalSource.get(srcId);
    if(lSource == null)
      return null;

    LogicalPartitionKey lKey = new LogicalPartitionKey(lSource, lPartition);
    PhysicalPartitionKey pKey = _logicalPKey2PhysicalPKey.get(lKey);
    return (pKey==null) ? null : pKey.getPhysicalPartition();
  }

  // iterator to go over all buffers
  public Iterable<DbusEventBuffer> bufIterable() {
    return _uniqBufs;
  }

  //CM API
  /** add new buffer
   * also checks if any buffers should be removed
   * @throws InvalidConfigException */
  public synchronized DbusEventBuffer addNewBuffer(PhysicalSourceStaticConfig pConfig,
                                                   DbusEventBuffer.StaticConfig config) throws InvalidConfigException {

	long startTimeTs = System.nanoTime();

    if(config == null)
      throw new InvalidConfigException("config cannot be null for addNewBuffer");

    // see if a buffer for this mapping exists
    PhysicalPartition pPartition = pConfig.getPhysicalPartition();

    PhysicalPartitionKey pKey = new PhysicalPartitionKey(pPartition);

    //record pSource association to the buffer
    PhysicalSource pSource = pConfig.getPhysicalSource();
    Set<PhysicalSource> set = _partKey2PhysiscalSources.get(pKey);
    if(set == null) {
      set = new HashSet<PhysicalSource>();
      _partKey2PhysiscalSources.put(pKey, set);
    }
    set.add(pSource);
    DbusEventBuffer buf = _bufsMap.get(pKey);

    if(buf != null) {
      LOG.info("Adding new buffer. Buffer " + buf.hashCode() + " already exists for: " + pConfig);
    } else {

      if(pConfig.isDbusEventBufferSet())
    	  {
    	  	buf = new DbusEventBuffer(pConfig.getDbusEventBuffer(), pPartition);
    	  	LOG.info("Using- source specific event buffer config, the event buffer size allocated is: " + buf.getAllocatedSize());
    	  }
      else
    	  {
    	  	buf = new DbusEventBuffer(config, pPartition);
    	  	LOG.info("Using- global event buffer config, the buffer size allocated is: " + buf.getAllocatedSize());
    	  }

      addBuffer(pConfig, buf);
    }

    buf.increaseRefCounter();

    deallocateRemovedBuffers(false); // check if some buffers need to be removed

    long endTimeTs = System.nanoTime();
    if (PERF_LOG.isDebugEnabled())
    {
    	PERF_LOG.debug("addNewBuffer took:" + (endTimeTs - startTimeTs) / _nanoSecsInMSec + "ms");
    }
    return buf;
  }

  /*
   * Remove an existing buffer - just decrements ref counter
   * If pSource is null, then it removes all the physical sources associated with the buffer.
   * Typically only during dropDatabase call when it has information only for the pKey
   */
  public synchronized void removeBuffer(PhysicalPartitionKey pKey, PhysicalSource pSource) {
    long startTimeTs = System.nanoTime();

    DbusEventBuffer buf = _bufsMap.get(pKey);
    if(buf == null) {
      LOG.error("Cannot find buffer for key = " + pKey);
      return;
    }
    // remove physical source association
    Set<PhysicalSource> set = _partKey2PhysiscalSources.get(pKey);
    if (pSource != null)
    {
        LOG.info("removing physicalSource = " + pSource + " for key = " + pKey);
        if(set == null || !set.remove(pSource)) {
          // not good, but not critical
          LOG.warn("couldn't remove pSource for key=" + pKey + ";set = " + set + "; psource=" + pSource);
        }
    }
    else
    {
        LOG.info("removing all physicalSources for key = " + pKey);
        _partKey2PhysiscalSources.remove(pKey);
    }

    buf.decreaseRefCounter();
    long endTimeTs = System.nanoTime();
    if (PERF_LOG.isDebugEnabled())
    {
    	PERF_LOG.debug("removeNewBuffer took:" + (endTimeTs - startTimeTs) / _nanoSecsInMSec + "ms");
    }
  }

  public synchronized void removeBuffer(PhysicalSourceStaticConfig pConfig) {
	  PhysicalPartitionKey pKey = new PhysicalPartitionKey(pConfig.getPhysicalPartition());
	  PhysicalSource pSource = pConfig.getPhysicalSource();
	  removeBuffer(pKey, pSource);
  }

  // actually removes buffers with refcount == 0 (and update happen more then a 'threshold' time ago)
  public synchronized void deallocateRemovedBuffers(boolean now) {
    HashSet<DbusEventBuffer> set = new HashSet<DbusEventBuffer>(5); //most cases should 1 or 2

    // first step remove the keys pointing to the expired buffer
    Iterator<PhysicalPartitionKey> it = _bufsMap.keySet().iterator();
    for(; it.hasNext(); ) {
      PhysicalPartitionKey pKey = it.next();
      DbusEventBuffer buf = _bufsMap.get(pKey);
      if(buf.shouldBeRemoved(now)) {
        it.remove();
        removeAuxMapping(pKey);
        set.add(buf);
      }
    }

    // now remove the buffers from the list of uniq buffers
    for(DbusEventBuffer b : set) {
      b.closeBuffer();
      _uniqBufs.remove(b);
    }
  }

  public synchronized void close()
  {
    if (_mmapDirectory != null)
    {
      // Move all meta files. We will create new ones when each buffer gets closed.
      File bakDir = new File(_mmapDirectory.getAbsolutePath() + BAK_DIRNAME_SUFFIX);
      FilePrefixFilter filter = new FilePrefixFilter(DbusEventBuffer.getMmapMetaInfoFileNamePrefix());
      File[] metaFiles = _mmapDirectory.listFiles(filter);
      for (File f : metaFiles)
      {
        if (f.isFile())
        {
          moveFile(f, bakDir);
        }
      }
    }

    for (Map.Entry<PhysicalPartitionKey, DbusEventBuffer> entry: _bufsMap.entrySet())
    {
      try
      {
        entry.getValue().closeBuffer();
      }
      catch (RuntimeException e)
      {
        LOG.error("error closing buffer for partition: " +
                  entry.getKey().getPhysicalPartition() + ": " + e.getMessage(), e);
      }
    }
    if (_mmapDirectory != null)
    {
      File bakDir = new File(_mmapDirectory.getAbsolutePath() + BAK_DIRNAME_SUFFIX);
      moveUnusedSessionDirs(bakDir);
    }
  }

  private void moveUnusedSessionDirs(File bakDir)
  {
    LOG.info("Moving unused session directories from " + _mmapDirectory);
    FilePrefixFilter sessionFileFilter = new FilePrefixFilter(DbusEventBuffer.getSessionPrefix());
    FilePrefixFilter metaFileFilter = new FilePrefixFilter((DbusEventBuffer.getMmapMetaInfoFileNamePrefix()));
    File[] metaFiles, sessionDirs;
    try
    {
      metaFiles = _mmapDirectory.listFiles(metaFileFilter);
      sessionDirs = _mmapDirectory.listFiles(sessionFileFilter);
    }
    catch(SecurityException e)
    {
      LOG.warn("Could not scan directories. Nothing moved.", e);
      return;
    }
    HashMap<String, File> sessionFileMap = new HashMap<String, File>(sessionDirs.length);

    for (File f : sessionDirs)
    {
      if (f.isDirectory())
      {
        sessionFileMap.put(f.getName(), f);
      }
    }

    for (File f: metaFiles)
    {
      if (!f.isFile())
      {
        continue;
      }
      // If the metafile references a session, remove it from the map.
      try
      {
        DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(f);
        mi.loadMetaInfo();
        if (mi.isValid())
        {
          String sessionId = mi.getSessionId();
          sessionFileMap.remove(sessionId);
        }
      }
      catch(DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException e)
      {
        // If we cannot parse even one meta file, there is no way to know which session is
        // valid, and which one is not, so we don't continue here.
        LOG.warn("Error parsing meta info file" + f.getName() + ". Nothing moved", e);
        return;
      }
    }

    // Now the map must have only those session directories that are not referenced in any meta file.
    // Move the remaining session directories to the backup area.
    int nDirsMoved = 0;
    for (File d : sessionFileMap.values())
    {
      try
      {
        if (moveFile(d, bakDir))
        {
          LOG.info("Moved directory " + d.getName());
          nDirsMoved++;
        }
        else
        {
          LOG.warn("Could not move directory " + d.getName() + ". Ignored");
        }
      }
      catch(SecurityException e)
      {
        LOG.warn("Could not move directory " + d.getName() + ". Ignored", e);
      }
    }

    LOG.info("Moved " + nDirsMoved + " session directories from " + _mmapDirectory);
  }

  // move a file or directory element to a backup Directory
  private boolean moveFile(File element, File bakDir)
  {
    LOG.info("backing up " + element);
    String baseName = element.getName();
    String bakDirName = bakDir.getAbsolutePath();

    if (!bakDir.exists())
    {
      // Create the directory
      if (!bakDir.mkdirs())
      {
        LOG.warn("Could not create directory " + bakDir.getName());
        return false;
      }
    }
    else
    {
      if (!bakDir.isDirectory())
      {
        LOG.error(bakDir.getName() + " is not a directory");
        return false;
      }
    }
    File movedFile = new File(bakDirName, baseName);

    return element.renameTo(movedFile);
  }

  private void removeAuxMapping(PhysicalPartitionKey key) {
    // figure out all the logicalKeys pointing to the this pPartKey
    List<LogicalPartitionKey> l = new ArrayList<LogicalPartitionKey>(10);
    for(Map.Entry<LogicalPartitionKey, PhysicalPartitionKey> e : _logicalPKey2PhysicalPKey.entrySet()) {
      if(e.getValue().equals(key)) {
        l.add(e.getKey());
      }
    }

    // now remove the keys
    for(LogicalPartitionKey lk : l)
      _logicalPKey2PhysicalPKey.remove(lk);
  }

  /** assert */
  public void assertBuffers() {
    // TBD
  }

  /** add another buffer with the mappings */
  public synchronized void addBuffer(PhysicalSourceStaticConfig pConfig, DbusEventBuffer buf) {
    LOG.info("addBuffer for phSrc=" + pConfig.getId() + "; buf=" + buf.hashCode());

    PhysicalPartition pPartition = pConfig.getPhysicalPartition();
    PhysicalPartitionKey pKey = new PhysicalPartitionKey(pPartition);


    _bufsMap.put(pKey, buf);
    _uniqBufs.add(buf);
    buf.setDropOldEvents(_dropOldEvents);

    for(LogicalSourceStaticConfig lSrc: pConfig.getSources()) {
      updateLogicalSourceMapping(pKey, lSrc.getLogicalSource(), lSrc.getPartition());
    }
  }

  public Set<PhysicalSource> getPhysicalSourcesForPartition(PhysicalPartition pPart) {
    PhysicalPartitionKey pKey = new PhysicalPartitionKey(pPart);
    return _partKey2PhysiscalSources.get(pKey);
  }

  /**
   * Processes all {@link DatabusSubscription} and generates a filter to match events for any of
   * those subscriptions.
   */
  public DbusFilter constructFilters(Collection<DatabusSubscription> subs) throws DatabusException
  {
    HashMap<PhysicalPartition, PhysicalPartitionDbusFilter> filterMap = null;
    for (DatabusSubscription sub: subs)
    {
      PhysicalPartition ppart = sub.getPhysicalPartition();
      if (sub.getLogicalSource().isWildcard())
      {
        if (!ppart.isWildcard())
        {
          if (null == filterMap) filterMap = new HashMap<PhysicalPartition, PhysicalPartitionDbusFilter>(10);
          filterMap.put(ppart, new PhysicalPartitionDbusFilter(ppart, null));
        }
        else
        {
          LOG.warn("ignoring subscription with both physical partition and logical source wildcards");
        }
      }
      else
      {
        PhysicalPartitionDbusFilter ppartFilter = null != filterMap ? filterMap.get(ppart) : null;
        LogicalSourceAndPartitionDbusFilter logFilter = null;
        if (null == ppartFilter)
        {
          logFilter = new LogicalSourceAndPartitionDbusFilter();
          ppartFilter = new PhysicalPartitionDbusFilter(ppart, logFilter);
          if (null == filterMap) filterMap = new HashMap<PhysicalPartition, PhysicalPartitionDbusFilter>(10);
          filterMap.put(ppart, ppartFilter);
        }
        else
        {
          logFilter = (LogicalSourceAndPartitionDbusFilter)ppartFilter.getNestedFilter();
        }

        if (null != logFilter) logFilter.addSourceCondition(sub.getLogicalPartition());
        else LOG.error("unexpected null filter for logical source");
      }
    }

    if (0 == filterMap.size()) return AllowAllDbusFilter.THE_INSTANCE;
    else if (1 == filterMap.size())
    {
      DbusFilter result = filterMap.entrySet().iterator().next().getValue();
      return result;
    }
    else {
      ConjunctionDbusFilter result = new ConjunctionDbusFilter();
      for (Map.Entry<PhysicalPartition, PhysicalPartitionDbusFilter> filterEntry: filterMap.entrySet())
      {
        result.addFilter(filterEntry.getValue());
      }
      return result;
    }
  }

  public Set<PhysicalPartitionKey> getAllPhysicalPartitionKeys()
  {
    return _bufsMap.keySet();
  }

  private void  updateLogicalSourceMapping(PhysicalPartitionKey pKey,
                                           LogicalSource lSource, LogicalPartition lPartition) {
    // add mapping between the logical source and the buffer
    LogicalPartitionKey lKey = new LogicalPartitionKey(lSource, lPartition);
    LOG.info("logical source " + lKey + " mapped to physical source " + pKey);

    _logicalPKey2PhysicalPKey.put(lKey, pKey);
    _logicalId2LogicalSource.put(lSource.getId(), lSource);
  }

  public void setDropOldEvents(boolean val) {
    _dropOldEvents = val;
    for(DbusEventBuffer buf : _uniqBufs) {
      buf.setDropOldEvents(val);
    }
  }

  //////////////////// multiple buffers streaming ///////////////////////////////
  /**
   * this decorator class helps keep track of how much was written to this channel
   */
  static public class SizeControlledWritableByteChannel implements WritableByteChannel {
    private final WritableByteChannel _channel;
    private int _totalWritten;

    public SizeControlledWritableByteChannel(WritableByteChannel ch) {
      _channel = ch;
      _totalWritten = 0;
    }
    public int writtenSoFar () {
      return _totalWritten;
    }

    @Override public boolean isOpen() { return _channel.isOpen(); }
    @Override public void close() throws IOException { _channel.close(); }
    @Override
    public int write(ByteBuffer src) throws IOException
    {
      int written = _channel.write(src);
      _totalWritten += written;
      return written;
    }

  }

  /**
   * this object is created with the list of source and the checkpoints
   * allows to read from different buffers (mapped by the sources) one window at a time
   */
  public class DbusEventBufferBatchReader implements DbusEventBufferBatchReadable {
    private final HashSet<PhysicalPartitionKey> _pKeys;
    CheckpointMult _checkPoints;
	private final StatsCollectors<DbusEventsStatisticsCollector> _statsCollectors;

    public DbusEventBufferBatchReader(CheckpointMult cpMult,
                                      Collection<PhysicalPartitionKey> physicalPartitions,
                                      StatsCollectors<DbusEventsStatisticsCollector> statsCollectors) throws IOException
    {
      _statsCollectors = statsCollectors;
      _checkPoints = cpMult;

      _pKeys = (null != physicalPartitions) ? new HashSet<PhysicalPartitionKey>(physicalPartitions)
                                            : new HashSet<PhysicalPartitionKey>();
    }

    public DbusEventBufferBatchReader(Collection<Integer> ids,
                                      CheckpointMult cpMult,
                                      StatsCollectors<DbusEventsStatisticsCollector> statsCollectors) throws IOException
    {
      this (cpMult, null, statsCollectors);

      // list of ids comes from request, so some values can be invalid
      // make sure that there is a buf for each id
      // and some can point to the same buf
      boolean debugEnabled = LOG.isDebugEnabled();
      for(int id: ids) {
        // figure out LogicalSource
        LogicalSource lSource = _logicalId2LogicalSource.get(id);
        LogicalPartition lPartition = null; // TBD - should be passed by caller
        if(lPartition == null)
          lPartition = LogicalPartition.createAllPartitionsWildcard(); // use wild card

        if (debugEnabled) LOG.debug("Streaming for logical source=" + lSource + "; partition=" +
                                    lPartition);

        List<LogicalPartitionKey> lpKeys = null;
        // for wild card - take all the source which id match (disregarding logical partition)
        if(lPartition.isAllPartitionsWildcard()) {
        	lpKeys = new ArrayList<LogicalPartitionKey>(_logicalPKey2PhysicalPKey.size());
        	for(LogicalPartitionKey lpKey : _logicalPKey2PhysicalPKey.keySet()) {
        		if(lpKey.getLogicalSource().getId().equals(id)) {
        			lpKeys.add(lpKey);
        		}
        	}
        } else {
        	lpKeys = new ArrayList<LogicalPartitionKey>(1);
        	LogicalPartitionKey lKey = new LogicalPartitionKey(lSource, lPartition);
        	lpKeys.add(lKey);
        }

        for(LogicalPartitionKey lpKey :lpKeys) {
        	PhysicalPartitionKey pKey = _logicalPKey2PhysicalPKey.get(lpKey);
        	DbusEventBuffer buf = null;
        	if(pKey != null) {
        		buf = _bufsMap.get(pKey);
        		if(buf != null) {
        			_pKeys.add(pKey);
        		} else {
                  LOG.warn("couldn't find buffer for pKeyp=" + pKey + " and lKey=" + lpKey);
                }
        	}
        	if(debugEnabled)
            LOG.debug("streaming:  for srcId=" + id + " and lKey=" + lpKey + " found pKey=" + pKey  +
                      " and buf=" +  (null==buf? "null" : buf.hashCode()));
        }
      }
    }



	@Override
    public int streamEvents(boolean streamFromLatestScn,
                            int batchFetchSize,
                            WritableByteChannel writeChannel, Encoding encoding,
                            DbusFilter filter)
    throws ScnNotFoundException, BufferNotFoundException, OffsetNotFoundException
    {
	  long startTimeTs = System.nanoTime();

      int numEventsStreamed = 0;
      int batchFetchSoFar = 0;
      DbusEventBuffer.StreamingMode mode =  DbusEventBuffer.StreamingMode.WINDOW_AT_TIME;

      // control how much is written thru this channel
      SizeControlledWritableByteChannel ch = new SizeControlledWritableByteChannel(writeChannel);

      boolean somethingStreamed = true;
      boolean debugEnabled = LOG.isDebugEnabled();
      while (somethingStreamed && batchFetchSoFar < batchFetchSize) {
        somethingStreamed = false; // if nothing gets streamed for a full circle we need to stop

        // go over relevant buffers
        for(PhysicalPartitionKey pKey : _pKeys) {
          DbusEventBuffer buf = _bufsMap.get(pKey);
          if (null == buf)
          {
            // in this case we want to disconnect the client
        	  String errMsg = "Buffer not found for physicalPartitionKey " + pKey;
        	  LOG.error(errMsg);
        	  throw new BufferNotFoundException(errMsg);
          }
          Checkpoint cp=null;
          PhysicalPartition pPartition = pKey.getPhysicalPartition();
          DbusEventsStatisticsCollector statsCollector = _statsCollectors == null ? null : _statsCollectors.getStatsCollector(pPartition.toSimpleString());

          if(_checkPoints != null) {
            cp = _checkPoints.getCheckpoint(pKey.getPhysicalPartition());// get the corresponding checkpoint
            if(debugEnabled)
              LOG.debug("get Checkpoint by pPartitio" + pPartition + ";cp=" + cp);
          }
          if(cp == null) {
            cp = new Checkpoint(); // create a checkpoint, NOTE: these values won't get back to caller
            cp.setFlexible();
            _checkPoints.addCheckpoint(pPartition, cp);
          }

          //by default we stream one Window worth of events
          if(_pKeys.size() == 1) // for single buffer just read as much as you can
            mode = DbusEventBuffer.StreamingMode.CONTINUOUS;

          int numEvents =  buf.streamEvents(cp, streamFromLatestScn, batchFetchSize - batchFetchSoFar,
                                            ch, encoding, filter, mode, statsCollector);

          if(numEvents>0)
            somethingStreamed = true;
          numEventsStreamed += numEvents;
          batchFetchSoFar = ch.writtenSoFar();
          if(debugEnabled)
            LOG.debug("one iteration:  read " + numEvents + " from buf " + buf.hashCode() +
                      "; read so far "  + batchFetchSoFar + "(out of " + batchFetchSize + ")");

          if(batchFetchSoFar >=  batchFetchSize)
            break;
        }
      }

      long endTimeTs = System.nanoTime();
      if (PERF_LOG.isDebugEnabled())
      {
      	PERF_LOG.debug("streamEvents took: " + (endTimeTs - startTimeTs) / _nanoSecsInMSec + "ms");
      }

      return numEventsStreamed;
    }
  }

  /**
   * @return number of unique buffers
   */
  public int bufsNum() {
    return _uniqBufs.size();
  }

  /* these needs to be reviewed when refactoring DbusEventBuffer (for startEvents and start()) */
  public void startAllEvents() {
    for(DbusEventBuffer buf : _uniqBufs) {
      buf.startEvents();
    }
  }
  public void endAllEvents(long seq, long nTime, DbusEventsStatisticsCollector stats) {
    for(DbusEventBuffer buf : _uniqBufs) {
      buf.endEvents(seq, stats);
    }
  }

  /**
   * clear all buffers in Mult
   */
  public void clearAll() {
    for(DbusEventBuffer buf : _uniqBufs)
      buf.clear();
  }

  public synchronized void saveBufferMetaInfo(boolean infoOnly) throws IOException {
    for(DbusEventBuffer buf : _uniqBufs)
      buf.saveBufferMetaInfo(infoOnly);
  }

  public void validateRelayBuffers() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
    for(DbusEventBuffer buf : _uniqBufs)
      buf.validateEventsInBuffer();
  }

  /**
   * guarantees that no events in un-finished windows
   */
  public void rollbackAllBuffers() {
    for(DbusEventBuffer buf : _uniqBufs)
      buf.rollbackEvents();
  }

  private static class FilePrefixFilter implements FilenameFilter
  {
    private final String _prefix;
    FilePrefixFilter(String prefix)
    {
      _prefix = prefix;
    }

    @Override
    public boolean accept(File dir, String name)
    {
      if (name.startsWith(_prefix))
      {
        return true;
      }
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  ////////////////// mapping keys//////////////////////////////////////////////////////////////////
  // LogicalPartitionKey ->  PhysicalPartitionKey
  // PhysicalPartitionKey -> DbusEventBuffer
  public static class LogicalPartitionKey {
    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_lPartition == null) ? 0 : _lPartition.hashCode());
      result = prime * result + ((_lSource == null) ? 0 : _lSource.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      LogicalPartitionKey other = (LogicalPartitionKey) obj;
      if (_lPartition == null)
      {
        if (other._lPartition != null)
          return false;
      }
      else if (!_lPartition.equals(other._lPartition))
        return false;
      if (_lSource == null)
      {
        if (other._lSource != null)
          return false;
      }
      else if (!_lSource.equals(other._lSource))
        return false;
      return true;
    }
    private final LogicalSource _lSource;
    private final LogicalPartition _lPartition;

    public LogicalSource getLogicalSource()
    {
      return _lSource;
    }
    public LogicalPartition getLogicalPartition()
    {
      return _lPartition;
    }

    public LogicalPartitionKey (LogicalSource lSource, LogicalPartition lPartition)
    {
      _lSource = lSource;
      if(lPartition == null)
        lPartition = LogicalSourceStaticConfig.getDefaultLogicalSourcePartition();
      _lPartition = lPartition;
    }
    @Override
    public String toString() {
      return "" + _lSource + _lPartition;
    }
  }
  //////////////////////////////////
  public static class PhysicalPartitionKey {
    @Override
    public int hashCode()
    {
      final int prime = 37;
      int result = 1;
      result = prime * result + ((_pPartition == null) ? 0 : _pPartition.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PhysicalPartitionKey other = (PhysicalPartitionKey) obj;
      if (_pPartition == null)
      {
        if (other._pPartition != null)
          return false;
      }
      else if (!_pPartition.equals(other._pPartition))
        return false;
      return true;
    }


    private PhysicalPartition _pPartition;

    public PhysicalPartition getPhysicalPartition()
    {
      return _pPartition;
    }
    public void setPhysicalPartition(PhysicalPartition p) {
      _pPartition = p;
    }

    public PhysicalPartitionKey() {
      _pPartition = new PhysicalPartition();
    }

    public PhysicalPartitionKey (PhysicalPartition pPartition)
    {
      _pPartition = pPartition;
    }

    public String toJsonString() {
      return ("{\"physicalPartition\":" + _pPartition.toJsonString() + "}");
    }
    @Override
    public String toString() {
      return toJsonString();
    }
  }
}
