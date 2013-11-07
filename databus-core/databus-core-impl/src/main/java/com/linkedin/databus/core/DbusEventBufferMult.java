package com.linkedin.databus.core;
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
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
  final private TreeMap<PhysicalPartitionKey, DbusEventBuffer> _bufsMap =
    new TreeMap<PhysicalPartitionKey, DbusEventBuffer>();

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
  private DbusEventFactory _eventFactory;


  // specify if we want to drop SCN less then current when adding new events to this buffers
  boolean _dropOldEvents = false;

  private final double _nanoSecsInMSec = 1000000.0;
  public static final String BAK_DIRNAME_SUFFIX = ".BAK";

  // (almost) empty constructor:  used only by tests
  public DbusEventBufferMult()
  {
    // creates empty mult buffer, new buffers can be added later
    _eventFactory = new DbusEventV2Factory();  // this is required in order to add buffers, though
  }

  // we should keep a set of all the buffers
  // we should build mapping based on unique physical source ids
  public DbusEventBufferMult(PhysicalSourceStaticConfig [] pConfigs,
                             DbusEventBuffer.StaticConfig config,
                             DbusEventFactory eventFactory)
  throws InvalidConfigException
  {
    _eventFactory = eventFactory;

    if(pConfigs == null) {
      // if we expect to get partitions configs from relay - we can create an EMPTY relay
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
    if (pKey == null) {
      return null;
    }
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
                                                   DbusEventBuffer.StaticConfig config)
  throws InvalidConfigException
  {
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
      if (pConfig.isDbusEventBufferSet())
      {
        buf = new DbusEventBuffer(pConfig.getDbusEventBuffer(), pPartition, _eventFactory);
        LOG.info("Using- source specific event buffer config, the event buffer size allocated is: " + buf.getAllocatedSize());
      }
      else
      {
        buf = new DbusEventBuffer(config, pPartition, _eventFactory);
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
      b.closeBuffer(false); // do not persist buffer's mmap info
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
        entry.getValue().closeBuffer(true);
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
    LOG.info("addBuffer for phSrc=" + pConfig + "; buf=" + buf.hashCode());

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

  public NavigableSet<PhysicalPartitionKey> getAllPhysicalPartitionKeys()
  {
    return _bufsMap.navigableKeySet();
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
    private final NavigableSet<PhysicalPartitionKey> _pKeys;
    CheckpointMult _checkPoints;
    int _clientEventVersion = 0;
	  private final StatsCollectors<DbusEventsStatisticsCollector> _statsCollectors;

    public DbusEventBufferBatchReader(CheckpointMult cpMult,
                                      Collection<PhysicalPartitionKey> physicalPartitions,
                                      StatsCollectors<DbusEventsStatisticsCollector> statsCollectors) throws IOException
    {
      _statsCollectors = statsCollectors;
      _checkPoints = cpMult;

      // physicalPartitions will be null in v2 mode
      _pKeys = (null != physicalPartitions) ? new TreeSet<PhysicalPartitionKey>(physicalPartitions)
                                            : new TreeSet<PhysicalPartitionKey>();
    }

    // V2 mode
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
    public StreamEventsResult streamEvents(boolean streamFromLatestScn,
                                           int batchFetchSize,
                                           WritableByteChannel writeChannel,
                                           Encoding encoding,
                                           DbusFilter filter)
    throws ScnNotFoundException, BufferNotFoundException, OffsetNotFoundException
    {
	    long startTimeTs = System.nanoTime();

      int numEventsStreamed = 0;
      int batchFetchSoFar = 0;
      DbusEventBuffer.StreamingMode mode =  DbusEventBuffer.StreamingMode.WINDOW_AT_TIME;

      // control how much is written thru this channel
      SizeControlledWritableByteChannel ch = new SizeControlledWritableByteChannel(writeChannel);

      boolean debugEnabled = LOG.isDebugEnabled();

      // If there is a partition that had a partial window sent, then we need to start from that
      // partition (and the offset within that checkpoint).
      // If there was no partial window sourced and a cursor was set, then the cursor was the partition
      // from which we last streamed data. We start streaming from the partition *after* the cursor,
      // unless the cursor itself is not there any more (in which case, we stream from first partition).
      NavigableSet<PhysicalPartitionKey> workingSet = _pKeys;
      PhysicalPartition partialWindowPartition = _checkPoints.getPartialWindowPartition();
      if (partialWindowPartition != null) {
        // Ignore cursorPartition, and construct a working set that includes the partialWindowPartition.
        workingSet = _pKeys.tailSet(new PhysicalPartitionKey(partialWindowPartition), true);
        if (workingSet.size() == 0) {
          // Apparently we sourced an incomplete window from a partition that has since moved away?
          // Throw an exception
          throw new OffsetNotFoundException("Partial window offset not found" + partialWindowPartition);
        }
      } else {
        // We sent a complete window last time (or, this is the first time we are sending something)
        // If cursor partition exists, and we can find it in our pKeys, we have a working set starting
        // from the partition greater than cursor. Otherwise, we go with the entire key set.
        workingSet = _pKeys;
        PhysicalPartition cursorPartition = _checkPoints.getCursorPartition();
        if (cursorPartition != null) {
          PhysicalPartitionKey ppKey = new PhysicalPartitionKey(cursorPartition);
          workingSet = _pKeys.tailSet(ppKey, false);
          if (workingSet.isEmpty() || !_pKeys.contains(ppKey)) {
            workingSet = _pKeys;
          }
        }
      }

      // Initialize a datastructure, that contains the list of all physical partitions for which we invoked
      // streamEvents with streamFromLatest==true.
      // Details described in DDSDBUS-2461, DDSDBUS-2341 and rb 178201
      Set<PhysicalPartitionKey> streamFromLatestState = new HashSet<PhysicalPartitionKey>();

      // Send events from each partition in the working set, iterating through them in an ascending
      // order. Stop sending when we have overflowed the buffer, or there is nothing more to send in
      // any of the buffers.
      // Note that in the first iteration of the outer loop, it may be that we have not scanned all
      // partitions (because our working set was smaller). In that case, even if nothing was streamed,
      // we want to continue through (at least) one more iteration, scanning all partitions.
      // Keep track of  partitions that are not able to send data because the event would not fit
      // into the buffer offered by the client. If we are returning from this method without sending a
      // single event *and* we had events in some of the partitions that would not fit in the client's
      // buffer, then send back the size of the smallest of such events to the client. It could well be
      // that the client can make progress in other partitions, but one partition could be blocked forever
      // because of this event if the client was offering its full buffer size.
      int minPendingEventSize = 0;
      boolean done = false;
      while (!done) {
        boolean somethingStreamed = false;

        // go over relevant buffers
        for(PhysicalPartitionKey pKey : workingSet) {
          DbusEventBuffer buf = _bufsMap.get(pKey);
          if (null == buf)
          {
            // in this case we want to disconnect the client
        	  String errMsg = "Buffer not found for physicalPartitionKey " + pKey;
        	  LOG.error(errMsg);
        	  throw new BufferNotFoundException(errMsg);
          }
          PhysicalPartition pPartition = pKey.getPhysicalPartition();
          DbusEventsStatisticsCollector statsCollector = _statsCollectors == null ? null : _statsCollectors.getStatsCollector(pPartition.toSimpleString());

          Checkpoint cp=null;
          cp = _checkPoints.getCheckpoint(pKey.getPhysicalPartition());// get the corresponding checkpoint
          if(debugEnabled)
            LOG.debug("get Checkpoint by pPartition" + pPartition + ";cp=" + cp);
          if(cp == null) {
            cp = new Checkpoint(); // create a checkpoint, NOTE: these values won't get back to V2 callers
            cp.setFlexible();
            _checkPoints.addCheckpoint(pPartition, cp);
          }

          //by default we stream one Window worth of events
          if(_pKeys.size() == 1) // for single buffer just read as much as you can
            mode = DbusEventBuffer.StreamingMode.CONTINUOUS;

          StreamEventsArgs args = new StreamEventsArgs(batchFetchSize - batchFetchSoFar);
          boolean streamFromLatestScnForPartition = computeStreamFromLatestScnForPartition(pKey, streamFromLatestState, streamFromLatestScn);
          args.setEncoding(encoding).setStreamFromLatestScn(streamFromLatestScnForPartition);
          args.setSMode(mode).setFilter(filter).setStatsCollector(statsCollector).setMaxClientEventVersion(_clientEventVersion);
          StreamEventsResult result =  buf.streamEvents(cp, ch, args);
          int numEvents = result.getNumEventsStreamed();
          if (numEvents == 0 && result.getSizeOfPendingEvent() > 0)
          {
            // There was an event in the DbusEventBuffer that could not fit into the client's buffer.
            if (minPendingEventSize == 0)
            {
              minPendingEventSize = result.getSizeOfPendingEvent();
            }
            else if (result.getSizeOfPendingEvent() < minPendingEventSize)
            {
              minPendingEventSize = result.getSizeOfPendingEvent();
            }
          }

          if(numEvents>0) {
            somethingStreamed = true;
          }
          numEventsStreamed += numEvents;
          batchFetchSoFar = ch.writtenSoFar();
          if(debugEnabled)
            LOG.debug("one iteration:  read " + numEvents + " from buf " + buf.hashCode() +
                      "; read so far "  + batchFetchSoFar + "(out of " + batchFetchSize + ")");

          _checkPoints.addCheckpoint(pPartition, cp);
          if (cp.getWindowOffset() < 0) {
            _checkPoints.setCursorPartition(pPartition);
          }
          if(batchFetchSoFar >=  batchFetchSize) {
            break;
          }
        }
        if (batchFetchSoFar >= batchFetchSize) {
          done = true;
        } else if (!somethingStreamed && (workingSet.size() == _pKeys.size())) {
          done = true;
        }
        // Start again with all keys.
        workingSet = _pKeys;
      }

      long endTimeTs = System.nanoTime();
      if (PERF_LOG.isDebugEnabled())
      {
      	PERF_LOG.debug("streamEvents took: " + (endTimeTs - startTimeTs) / _nanoSecsInMSec + "ms");
      }

      return new StreamEventsResult(numEventsStreamed,
                                    minPendingEventSize > 0 ? minPendingEventSize : 0);
    }

    @Override
    public CheckpointMult getCheckpointMult()
    {
      return _checkPoints;
    }

    @Override
    public void setClientMaxEventVersion(int version)
    {
      _clientEventVersion = version;
    }

    /**
     * A helper method to deal with enableStreamFromLatestScn logic between DbusEventBufferMult and DbusEventBuffer
     * If streamFromLatest==true, invoke streamEvents call on DbusEventBuffer exactly once with streamFromLatest==true.
     * Else if streamFromLatest==false, invoke streamEvents with streamFromLatest==false everytime.
     * Because of the information lost about the checkpoint, the same last event is repeatedly streamed until the buffer
     * fills up. The stop-gap solution to fix the issue is to send a subsequent call with streamFromLatest==false, and hence
     * have the incoming checkpoint respected
     *
     * @pKey The partition key that we currently want to stream from
     * @ppList The list of partition keys, which have already been invoked streamEvents with streamFromLatest==true. This is
     *         assumed to be an allocated HashSet
     * @streamFromLatestScn The boolean variable that is input into {@link DbusEventBufferMult##streamEvents}
     */
    protected boolean computeStreamFromLatestScnForPartition(
        PhysicalPartitionKey pKey, Set<PhysicalPartitionKey> ppList, boolean streamFromLatestScn)
    {
      if (! streamFromLatestScn)
      {
        // If streamFromLatestScn is false, always return false
        return false;
      }
      if ( pKey == null || ppList.contains(pKey) )
      {
        return false;
      }
      if (!ppList.contains(pKey))
      {
        ppList.add(pKey);
      }
      return true;
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
  public static class PhysicalPartitionKey implements Comparable {
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

    @Override
    public int compareTo(Object other) {
      if (!(other instanceof PhysicalPartitionKey)) {
        throw new ClassCastException("PhysicalPartitionKey class expected instead of " + other.getClass().getSimpleName());
      }
      PhysicalPartition op = ((PhysicalPartitionKey)other).getPhysicalPartition();
      return _pPartition.compareTo(op);
    }
  }
}
