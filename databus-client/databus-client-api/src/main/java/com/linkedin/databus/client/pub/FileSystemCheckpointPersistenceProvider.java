package com.linkedin.databus.client.pub;
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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonProcessingException;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigManager;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * Stores checkpoints to disk as simple files. The format is that the checkpoint for each stream
 * is stored in a different file whose name can be uniquely derived from the stream. For debugging
 * purposes, the provider can also support rolling files, where the last few (configurable)
 * checkpoint files are also stored. The format of the files is &lt;StreamId&gt;.xxx where xxx is
 * monotonically increasing number. The latest checkpoint is &lt;StreamId&gt;.current .
 *
 * The format of the files is
 * Line 1: json representation of the checkpoint
 * Line 2: crc of json
 *
 * @author cbotev
 *
 */
public class FileSystemCheckpointPersistenceProvider extends CheckpointPersistenceProviderAbstract
{
  public static final String MODULE = FileSystemCheckpointPersistenceProvider.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final int MAX_STREAMID_NAME_LENGTH = 240;
  private static final String COMMON_SOURCENAME_PREFIX = "com.linkedin.events.";

  /** The static configuration */
  private final StaticConfig _staticConfig;
  /** Runtime config manager*/
  private final ConfigManager<RuntimeConfig> _configManager;
  /** Cache for checkpoints. The invariant is that if the cache has an entry, it is the
   * latest version. If there is no entry, then either there is no checkpoint or the checkpoint
   * is persisted in a file on the disk */
  private final HashMap<String, CacheEntry> _cache;
  private final ReadWriteLock _cacheLock = new ReentrantReadWriteLock(true);

  public FileSystemCheckpointPersistenceProvider() throws InvalidConfigException
  {
    this (new Config(), 2);
  }

  public FileSystemCheckpointPersistenceProvider(Config config, int version) throws InvalidConfigException
  {
    this(config.build(), version);
  }

  public FileSystemCheckpointPersistenceProvider(StaticConfig config, int version) throws InvalidConfigException
  {
    super(version);
    _staticConfig = config;
    _cache = new HashMap<String, CacheEntry>(100);

    _staticConfig.getRuntime().setManagedInstance(this);
    _configManager = new ConfigManager<RuntimeConfig>(_staticConfig.getRuntimeConfigPrefix(),
                                                      _staticConfig.getRuntime());

  }

  public StaticConfig getStaticConfig()
  {
    return _staticConfig;
  }

  @Override
  public Checkpoint loadCheckpointV3(List<DatabusSubscription> subs,
		  							 RegistrationId registrationId)
  {
    return loadCheckpointInternal(convertSubsToListOfStrings(subs), registrationId);
  }

  @Override
  public void storeCheckpointV3(List<DatabusSubscription> subs,
                                Checkpoint checkpoint,
                                RegistrationId registrationId) throws IOException
  {
    storeCheckpointInternal(convertSubsToListOfStrings(subs), checkpoint, registrationId);
  }

  @Override
  public void removeCheckpointV3(List<DatabusSubscription> subs,
		  						 RegistrationId registrationId)
  {
   removeCheckpointInternal(convertSubsToListOfStrings(subs), registrationId);
  }

  @Override
  public Checkpoint loadCheckpoint(List<String> sourceNames)
  {
	  return loadCheckpointInternal(sourceNames, null);
  }

  @Override
  public void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint) throws IOException
  {
	  storeCheckpointInternal(sourceNames, checkpoint, null);
  }

  @Override
  public void removeCheckpoint(List<String> sourceNames)
  {
	  removeCheckpointInternal(sourceNames, null);
  }

  private Checkpoint loadCheckpointInternal(List<String> sourceNames, RegistrationId registrationId)
  {
    String streamId = calcStreamId(sourceNames);

    try
    {
      CacheEntry cacheEntry = doLoadCheckpoint(sourceNames, streamId, registrationId);
      Checkpoint result = (null == cacheEntry) ? null : cacheEntry.getCheckpoint();
      return result;
    }
    catch (IOException ioe)
    {
      LOG.error("Error loading checkpoint", ioe);
      return null;
    }

  }

  private void storeCheckpointInternal(List<String> sourceNames, Checkpoint checkpoint, RegistrationId registrationId)
  throws IOException
  {
    String streamId = calcStreamId(sourceNames);

    //make sure we have the latest copy
    CacheEntry cacheEntry = doLoadCheckpoint(sourceNames, streamId, registrationId);
    if (null == cacheEntry)
    {
      throw new IOException("Creation of checkpoint failed: " + streamId);
    }

    if (!cacheEntry.setCheckpoint(checkpoint))
    {
      throw new IOException("Storing of checkpoint failed");
    }
  }

  private void removeCheckpointInternal(List<String> sourceNames, RegistrationId registrationId)
  {
    LOG.info("Removing checkpoint for:" + sourceNames);
    String streamId = calcStreamId(sourceNames);
    String key = calculateIndexForCache(streamId, registrationId);

    Lock writeLock = _cacheLock.writeLock();
    writeLock.lock();
    try
    {
      _cache.remove(key);
      File rootDirectoryForRegistrationId;
      if (null == registrationId)
    	  rootDirectoryForRegistrationId = getStaticConfig().getRootDirectory();
      else
    	  rootDirectoryForRegistrationId = new File (getStaticConfig().getRootDirectory(), registrationId.getId());

      File curCheckpointFile = new File(rootDirectoryForRegistrationId,
                                        streamId + ".current");
      if (curCheckpointFile.exists())
      {
        if (!curCheckpointFile.delete())
        {
          LOG.error("checkpoint removal failed: " + sourceNames);
          LOG.error("could not delete file:" + curCheckpointFile.getAbsolutePath());
        }
      }
    }
    finally
    {
      writeLock.unlock();
    }
  }

  /**
   * Creates a cache entry if none exists; otherwise returns current cache entry
   * @param  sourceNames     the stream sources
   * @return the cache entry
   * @throws IOException
   */
  CacheEntry doLoadCheckpoint(List<String> sourceNames, String streamId, RegistrationId registrationId) throws IOException
  {

    Lock readLock = _cacheLock.readLock();
    readLock.lock();
    CacheEntry cacheEntry = null;
    String key = calculateIndexForCache(streamId, registrationId);
    try
    {
      cacheEntry = _cache.get(key);
    }
    finally
    {
      readLock.unlock();
    }

    if (null == cacheEntry)
    {
      Lock writeLock = _cacheLock.writeLock();
      writeLock.lock();
      try
      {
        cacheEntry = new CacheEntry(streamId, registrationId);
        _cache.put(key, cacheEntry);
      }
      finally
      {
        writeLock.unlock();
      }
    }

    return cacheEntry;
  }

  private String calculateIndexForCache(String streamId, RegistrationId registrationId)
  {
  	String key = streamId;
  	if (null != registrationId)
  	{
  		assert(false == registrationId.getId().isEmpty());
  		key = registrationId.getId() + streamId;
  	}
  	return key;
  }

  static String calcStreamId(List<String> sourceNames)
  {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("cp_");

    boolean useShortNames = (sourceNames.size() >= 5);

    boolean first = true;
    for (String sourceName: sourceNames)
    {
      if (!first) sb.append('-');
      first = false;

      //Be less verbose for checkpoint with many sources; don't break previous consumers with
      //existing checkpoints
      String realSourceName = (useShortNames && sourceName.startsWith(COMMON_SOURCENAME_PREFIX)) ?
          sourceName.substring(COMMON_SOURCENAME_PREFIX.length()) :
          sourceName;

      sb.append(realSourceName.replaceAll("\\W", "_"));
    }

    if (sb.length() > MAX_STREAMID_NAME_LENGTH) sb.delete(MAX_STREAMID_NAME_LENGTH, sb.length());

    return sb.toString();
  }

  public File generateCheckpointFile(String basename, RegistrationId registrationId, int index)
  {
	File rootDirectory;
	if (null == registrationId)
		rootDirectory = _staticConfig.getRootDirectory();
	else
		rootDirectory = new File(_staticConfig.getRootDirectory(), registrationId.getId());
    return StaticConfig.generateCheckpointFile(rootDirectory, basename, index);
  }

  public ConfigManager<RuntimeConfig> getConfigManager()
  {
    return _configManager;
  }

  class CacheEntry
  {
    private Checkpoint _checkpoint;
    private boolean _checkpointLoaded;
    private TreeSet<HistoryEntry> _historyEntries;
    private final String _filePrefix;
    private final RegistrationId _registrationId;
    private File _rootDirectory;

    public CacheEntry(String streamId, RegistrationId registrationId) throws IOException
    {
      _checkpointLoaded = false;
      _filePrefix = streamId + ".";
      _registrationId = registrationId;
      if (null == _registrationId)
    	  _rootDirectory = _staticConfig.getRootDirectory();
      else
    	  _rootDirectory = new File (_staticConfig.getRootDirectory(), _registrationId.getId());
      boolean success = _rootDirectory.mkdirs();

      if ( ! success )
      {
    	  LOG.error("Failed to create checkpoint directory (" + _rootDirectory + ")");
      }
    }

    public synchronized Checkpoint getCheckpoint()
    {
      if (! _checkpointLoaded)
      {
        loadCurrentCheckpoint();
        _checkpointLoaded = true;
      }

      return _checkpoint;
    }

    private boolean loadCurrentCheckpoint()
    {
      //We read the current checkpoint from the file. If we encounter an error, we log an error
      //but we preserve the null value for the checkpoint and move on.
      boolean hasError = false;
      File cpFile = new File(_rootDirectory, _filePrefix + "current");
      if (cpFile.exists())
      {
        try
        {
          BufferedReader checkpointFile = new BufferedReader(new FileReader(cpFile));

          String jsonLine = checkpointFile.readLine();
          if (null == jsonLine)
          {
            LOG.error("Checkpoint JSON serialization expected");
            hasError = true;
          }

          Checkpoint newCheckpoint = null;
          if (!hasError)
          {
            try
            {
              newCheckpoint = new Checkpoint(jsonLine);
              if (LOG.isDebugEnabled()) LOG.debug("checkpoint loaded:" + newCheckpoint.toString());
            }
            catch (JsonProcessingException jpe)
            {
              hasError = true;
              LOG.error("Unable to deserialize checkpoint", jpe);
            }
          }

          if (!hasError)
          {
            _checkpoint = newCheckpoint;
          }

          checkpointFile.close();
        }
        catch (IOException ioe)
        {
          LOG.error(ioe);
        }
      }

      return !hasError;
    }

    public synchronized boolean setCheckpoint(Checkpoint checkpoint)
    {
      if (null == checkpoint)
      {
        LOG.error("Cannot save null checkpoints");
        return false;
      }

      boolean hasError = false;
      RuntimeConfig runtimeConfig = getConfigManager().getReadOnlyConfig();

      if (runtimeConfig.isHistoryEnabled())
      {
        findHistoryFiles();
      }

      File newCheckpointFile = new File(_rootDirectory,
                                        _filePrefix + "newcurrent");
      File curCheckpointFile = new File(_rootDirectory,
                                        _filePrefix + "current");
      boolean hasCurCheckpointFile = curCheckpointFile.exists();
      hasError = hasError || !storeCheckpoint(checkpoint, newCheckpointFile);

      if (!hasError)
      {
        if (hasCurCheckpointFile && runtimeConfig.isHistoryEnabled() )
        {
          addHistoryEntry(curCheckpointFile);
        }
        else
        {
          File oldCheckpointFile = new File(_rootDirectory,
                                            _filePrefix + "oldcurrent");
          if (oldCheckpointFile.exists() && !oldCheckpointFile.delete())
          {
            LOG.error("removing old checkpoint file failed:" + oldCheckpointFile.getAbsolutePath());
          }
          if (hasCurCheckpointFile && !curCheckpointFile.renameTo(oldCheckpointFile))
          {
            LOG.error("saving old checkpoint file failed: " + oldCheckpointFile.getAbsolutePath());
          }

          if (hasCurCheckpointFile)
          {
            if (curCheckpointFile.exists() && !curCheckpointFile.delete())
            {
              LOG.warn("deletion of checkpoint file failed:" + curCheckpointFile.getAbsolutePath());
            }
          }
        }

        if (!newCheckpointFile.renameTo(curCheckpointFile))
        {
          hasError = true;
          LOG.error("Saving current checkpoint failed");
        }
      }

      if (!hasError)
      {
        _checkpoint = checkpoint;
      }

      return !hasError;
    }

    private void findHistoryFiles()
    {
      _historyEntries = new TreeSet<HistoryEntry>();

      File[] historyFiles = getStaticConfig().getRootDirectory().listFiles(new FilenameFilter()
          {
            @Override
            public boolean accept(File dir, String name)
            {
              return name.startsWith(_filePrefix);
            }
          });

      if (null == historyFiles)
      {
        LOG.warn("Unable to find history files " + _filePrefix + "*");
      }
      else
      {
        for (File f: historyFiles)
        {
          HistoryEntry entry = createHistoryEntry(f);
          if (null != entry) _historyEntries.add(entry);
        }
      }
    }

    private boolean storeCheckpoint(Checkpoint checkpoint, File toFile)
    {
      boolean hasError = false;

      if (toFile.exists() && !toFile.delete())
      {
        LOG.error("deletion of file failed: " + toFile.getAbsolutePath());
      }
      try
      {
        PrintWriter out = new PrintWriter(toFile);
        String checkpointJson = checkpoint.toString();
        out.println(checkpointJson);
        out.close();
      }
      catch (Exception e)
      {
        hasError = true;
        LOG.error("Store checkpoint error", e);
      }

      return ! hasError;
    }

    private void cleanupHistoryFiles()
    {
      RuntimeConfig runtimeConfig = getConfigManager().getReadOnlyConfig();
      int maxSize = runtimeConfig.getHistorySize();
      while (_historyEntries.size() > maxSize)
      {
        _historyEntries.remove(_historyEntries.last());
      }
    }

    private HistoryEntry createHistoryEntry(File file)
    {
      int suffixIdx = file.getName().lastIndexOf('.');
      if (-1 == suffixIdx) return null;

      String suffix = file.getName().substring(suffixIdx + 1);
      int index = -1;
      try
      {
        index = Integer.parseInt(suffix);
        return (index < 0) ? null : new HistoryEntry(index, file);
      }
      catch (NumberFormatException nfe)
      {
        return null;
      }
    }

    private void addHistoryEntry(File f)
    {
      HistoryEntry newEntry = new HistoryEntry(-1, f);
      _historyEntries.add(newEntry);
      cleanupHistoryFiles();

      Iterator<HistoryEntry> descIter = _historyEntries.descendingIterator();
      while (descIter.hasNext())
      {
        HistoryEntry entry = descIter.next();
        entry.bumpUpVersion();
      }
    }

    class HistoryEntry implements Comparable<HistoryEntry>
    {
      private int _index;
      private File _file;

      public HistoryEntry(int index, File file)
      {
        super();
        _index = index;
        _file = file;
      }

      public void bumpUpVersion()
      {
        ++_index;
        File newFile = generateCheckpointFile(_filePrefix, _registrationId, _index);
        if (newFile.exists() && !newFile.delete())
        {
          LOG.warn("failed to remove file:" + newFile.getAbsolutePath());
        }

        if (!_file.renameTo(newFile))
        {
          LOG.error("File rollover failed: from " + _file.getAbsolutePath() + " to " +
                    newFile.getAbsolutePath());
        }

        _file = newFile;
      }

      @Override
      public int compareTo(HistoryEntry other)
      {
        return this._index - other._index;
      }

      @Override
      public boolean equals(Object other)
      {
        if (null == other || !(other instanceof HistoryEntry)) return false;
        HistoryEntry otherEntry = (HistoryEntry)other;
        return this._index == otherEntry._index;
      }

      @Override
      public int hashCode()
      {
        return this._index;
      }

    }
  }

  /**
   * Runtime configuration for the file-system checkpoint persistence provider
   *
   * {@see FileSystemCheckpointPersistenceProvider} */
  public static class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final boolean _historyEnabled;
    private final int _historySize;

    public RuntimeConfig(boolean historyEnabled, int historySize)
    {
      super();
      _historyEnabled = historyEnabled;
      _historySize = historySize;
    }

    /** A flag that indicates if the provider is archive previously persisted checkpoints */
    public boolean isHistoryEnabled()
    {
      return _historyEnabled;
    }

    /** The number of archived checkpoints to store. Meaningful only if {@link #isHistoryEnabled()}
     * is *true*. */
    public int getHistorySize()
    {
      return _historySize;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      // No shared state to change; runtime config settings are used on demand
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || !(otherConfig instanceof RuntimeConfig)) return false;
      return equalsConfig((RuntimeConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(RuntimeConfig otherConfig)
    {
      if (null == otherConfig) return false;
      return getHistorySize() == otherConfig.getHistorySize() &&
             isHistoryEnabled() == otherConfig.isHistoryEnabled();
    }

    @Override
    public int hashCode()
    {
      return _historySize ^ (_historyEnabled ? 0xFFFFFFFF : 0);
    }

	@Override
	public String toString() {
		return "RuntimeConfig [_historyEnabled=" + _historyEnabled
				+ ", _historySize=" + _historySize + "]";
	}


  }

  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    /** Max valid history size; if you increase this make sure the file name generation uses
     * enough digits in the extension */
    public static final int MAX_HISTORY_SIZE = 99;
    public static final String CHECKPOINT_FILE_EXT_PATTERN = "%02d";

    /** A flag if checkpoint history is to be saved */
    private boolean _historyEnabled                                  = false;
    /** The number of checkpoints in the history */
    private int _historySize                                         = 5;
    private FileSystemCheckpointPersistenceProvider _managedInstance = null;

    public RuntimeConfigBuilder()
    {
    }

    /**
     * Checks if checkpoint history is enabled.
     * @return true iff checkpoint history is enabled
     */
    public boolean isHistoryEnabled()
    {
      return _historyEnabled;
    }

    public void setHistoryEnabled(boolean historyEnabled)
    {
      _historyEnabled = historyEnabled;
    }

    /**
     * Obtains the max number of checkpoints to be stored in the history (in addition to the latest
     * one).
     * @return the number of checkpoints in the history
     */
    public int getHistorySize()
    {
      return _historySize;
    }

    /** Changes the number of archived checkpoints to store. Meaningful only if
     * {@link #isHistoryEnabled()} is *true*.
     *
     *  The number must be between 1 and {@link #MAX_HISTORY_SIZE}.
     * */
    public void setHistorySize(int historySize)
    {
      _historySize = historySize;
    }

    public FileSystemCheckpointPersistenceProvider getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(FileSystemCheckpointPersistenceProvider managedInstance)
    {
      _managedInstance = managedInstance;
    }


    @Override
    public RuntimeConfig build() throws InvalidConfigException
    {
      if (_historySize <= 0 || _historySize > MAX_HISTORY_SIZE)
      {
        throw new InvalidConfigException("Invalid history size:" + _historySize);
      }
      if (null == _managedInstance)
      {
        throw new InvalidConfigException("No associated managed instance for runtime config");
      }
      return new RuntimeConfig(_historyEnabled, _historySize);
    }

	@Override
	public String toString() {
		return "RuntimeConfigBuilder [_historyEnabled=" + _historyEnabled
				+ ", _historySize=" + _historySize + ", _managedInstance="
				+ _managedInstance + "]";
	}


  }

  /** Static configuration for the file-system checkpoint persistence provider.
   *
   * @see FileSystemCheckpointPersistenceProvider
   */
  public static class StaticConfig
  {
    private final File _rootDirectory;
    private final RuntimeConfigBuilder _runtime;
    private final String _runtimeConfigPrefix;

    public StaticConfig(File rootDirectory, RuntimeConfigBuilder runtime, String runtimeConfigPrefix)
    {
      super();
      _rootDirectory = rootDirectory;
      _runtime = runtime;
      _runtimeConfigPrefix = runtimeConfigPrefix;
    }

    /** The root directory for all checkpoint files. */
    public File getRootDirectory()
    {
      return _rootDirectory;
    }

    /** The runtime configuration properties */
    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public String getRuntimeConfigPrefix()
    {
      return _runtimeConfigPrefix;
    }

    public static File generateCheckpointFile(File rootDir, String basename, int index)
    {
      Formatter fmt = new Formatter();
      try
      {
        fmt.format(RuntimeConfigBuilder.CHECKPOINT_FILE_EXT_PATTERN, index);

        return new File(rootDir, basename + fmt.toString());
      }
      finally
      {
        fmt.close();
      }
    }

	@Override
	public String toString() {
		return "StaticConfig [_rootDirectory=" + _rootDirectory + ", _runtime="
				+ _runtime + ", _runtimeConfigPrefix=" + _runtimeConfigPrefix
				+ "]";
	}


  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {

    /** The root directory for the checkpoint files */
    private String _rootDirectory         = "./databus2-checkpoints";
    /** Property prefix for the runtime*/
    private String _runtimeConfigPrefix   = "databus.checkpointPersistence.fileSystem.";
    private RuntimeConfigBuilder _runtime;

    public Config()
    {
      super();
      _runtime = new RuntimeConfigBuilder();
    }

    public String getRootDirectory()
    {
      return _rootDirectory;
    }

    public void setRootDirectory(String rootDirectory)
    {
      _rootDirectory = rootDirectory;
    }

    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public void setRuntime(RuntimeConfigBuilder runtime)
    {
      _runtime = runtime;
    }

    public String getRuntimeConfigPrefix()
    {
      return _runtimeConfigPrefix;
    }

    public void setRuntimeConfigPrefix(String runtimeConfigPrefix)
    {
      _runtimeConfigPrefix = runtimeConfigPrefix;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      File rootDirectory = new File(_rootDirectory);
      if (!rootDirectory.exists())
      {
        if (!rootDirectory.mkdirs())
        {
          throw new InvalidConfigException("Invalid checkpoint directory:" +
                                           rootDirectory.getAbsolutePath());
        }
      }

      LOG.info("Checkpoint directory:" + rootDirectory.getAbsolutePath());

      return new StaticConfig(rootDirectory, _runtime, _runtimeConfigPrefix);
    }

  }

}
