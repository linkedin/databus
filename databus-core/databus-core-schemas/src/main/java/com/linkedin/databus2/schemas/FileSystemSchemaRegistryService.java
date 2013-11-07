package com.linkedin.databus2.schemas;
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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

/**
 * Implements a simple {@link SchemaRegistryService} by reading schemas from a directory.
 * @author cbotev
 */
public class FileSystemSchemaRegistryService extends VersionedSchemaSetBackedRegistryService
{
  public static final String MODULE = FileSystemSchemaRegistryService.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final FileSystemVersionedSchemaSetProvider _fsSchemaSetProvider;
  private final ResourceVersionedSchemaSetProvider _resourceSchemaSetProvider;
  private final StaticConfig _config;
  private Thread _schemaSetRefreshThread;
  private final AtomicBoolean _stopRefreshThread = new AtomicBoolean();
  private long _lastRefreshTs = -1;

  public static FileSystemSchemaRegistryService build(Config config) throws InvalidConfigException
  {
    return build(config.build());
  }

  public static FileSystemSchemaRegistryService build(StaticConfig config)
  {
    FileSystemSchemaRegistryService service = new FileSystemSchemaRegistryService(config);
    service.initializeSchemaSet();
    if (0 == service.getCurSchemaSet().size())
    {
      throw new DatabusRuntimeException("no schemas loaded; please check the schemas directory: " +
                                        config.getSchemaDir().getAbsolutePath() );
    }

    if (config.getRefreshPeriodMs() > 0)
    {
      service.startSchemasRefreshThread();
    }
    return service;
  }

  private FileSystemSchemaRegistryService(StaticConfig config)
  {
    super();
    _curSchemaSet = new VersionedSchemaSet();
    _fsSchemaSetProvider = new FileSystemVersionedSchemaSetProvider(Arrays.asList(config.getSchemaDir()));
    _resourceSchemaSetProvider = new ResourceVersionedSchemaSetProvider(this.getClass().getClassLoader());
    _schemaSetRefreshThread = null;
    _config = config;
  }

  /**
   * Starts the thread that periodically ({@link Config#getRefreshPeriodMs()} refreshes the schema set.
   * @return true if started
   */
  public boolean startSchemasRefreshThread()
  {
    if (_config.getRefreshPeriodMs() <= 0 || null != _schemaSetRefreshThread)
    {
      return false;
    }
    LOG.info("Starting schema refresh thread");

    _stopRefreshThread.set(false);
    _schemaSetRefreshThread = new Thread(new SchemaSetRefreshThread(), "SchemaRefreshThread");
    _schemaSetRefreshThread.setDaemon(true);
    _schemaSetRefreshThread.start();

    return true;
  }

  public void stopSchemasRefreshThread()
  {
    if (null == _schemaSetRefreshThread) return;

    LOG.info("Stopping schema refresh thread");
    _stopRefreshThread.set(true);
    _schemaSetRefreshThread.interrupt();
    while (_schemaSetRefreshThread.isAlive())
    {
      try
      {
        _schemaSetRefreshThread.join();
      }
      catch (InterruptedException ie) {}
    }
  }

  /**
   * Registers a schema represented by its JSON schema definition. Note that the schema will not be
   * persisted on disk. If there is a schema registered with the same name, it's version will be
   * increased by one.
   *
   * IMPORTANT: If you don't {@link FileSystemSchemaRegistryService#stopSchemasRefreshThread()},
   * the schema will be overwritten.
   */
  @Override
  public void registerSchema(VersionedSchema schema) throws DatabusException
  {
    super.registerSchema(schema);
  }

  private void initializeSchemaSet()
  {
    LOG.info("initializing schema registry");
    refreshSchemaSet();
    if (0 > _lastRefreshTs)
    {
      LOG.info("loading schemas from resources ");
      _curSchemaSet = _resourceSchemaSetProvider.loadSchemas();
      LOG.info("schemas from resources loaded");
    }
  }

  private void refreshSchemaSet()
  {
    LOG.info("refreshing schema registry");
    File schemaDir = _config.getSchemaDir();
    if (!_config.isFallbackToResources() && ! schemaDir.exists())
    {
      LOG.warn("schema dir not found:" + _config.getSchemaDir());
      return;
    }

    if (schemaDir.exists())
    {
      _curSchemaSet = _fsSchemaSetProvider.loadSchemas();
      _lastRefreshTs = System.currentTimeMillis();
    }
    else LOG.info("skipping not existant schema directory: " + schemaDir.getAbsolutePath());
    LOG.info("schema registry refreshed");
  }

  private class SchemaSetRefreshThread implements Runnable
  {

    public SchemaSetRefreshThread()
    {
    }

    @Override
    public void run()
    {
      while (! _stopRefreshThread.get())
      {
        try
        {
          Thread.sleep(_config.getRefreshPeriodMs());
        }
        catch (InterruptedException ie)
        {//do nothing
        }
        refreshSchemaSet();
      }

      LOG.info("Quitting schema refresh thread");
    }

  }

  public static class StaticConfig
  {

    private final File _schemaDir;
    private final long _refreshPeriodMs;
    private final boolean _enabled;
    private final boolean _fallbackToResources;

    public StaticConfig(File schemaDir, long schemasRefreshPeriodMs, boolean enabled,
                        boolean fallbackToResources)
    {
      super();
      _schemaDir = schemaDir;
      _refreshPeriodMs = schemasRefreshPeriodMs;
      _enabled = enabled;
      _fallbackToResources = fallbackToResources;
    }

    public File getSchemaDir()
    {
      return _schemaDir;
    }

    public long getRefreshPeriodMs()
    {
      return _refreshPeriodMs;
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    /** Try to load schemas from resources if directory is not available. */
    public boolean isFallbackToResources()
    {
      return _fallbackToResources;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    public static final String DEFAULT_FS_SCHEMA_REGISTRY_PATH = "schemas_registry";
    public static final long DEFAULT_FS_SCHEMA_REGISTRY_REFRESH_MS = 3600000;

    private String _schemaDir;
    private long _refreshPeriodMs;
    private boolean _enabled;
    private boolean _fallbackToResources;

    /** Default constructor. Uses the system properties for initialization. */
    public Config()
    {
      _schemaDir = DEFAULT_FS_SCHEMA_REGISTRY_PATH;
      _refreshPeriodMs = DEFAULT_FS_SCHEMA_REGISTRY_REFRESH_MS;
      _enabled = true;
      _fallbackToResources = true;
    }

    /**
     * Obtains the directory where the schemas are stored.
     * @return the absolute path of the directory
     */
    public String getSchemaDir()
    {
      return _schemaDir;
    }

    /**
     * Changes setting with the directory where the schemas are stored.
     * @param schemaDir         the new setting value
     */
    public void setSchemaDir(String schemaDir)
    {
      _schemaDir = schemaDir;
    }

    /**
     * Obtains the interval at which the schema registry will be synced with the file system.
     * @return the interval in ms
     */
    public long getRefreshPeriodMs()
    {
      return _refreshPeriodMs;
    }

    /**
     * Changes the interval at which the schema registry will be synced with the file system
     * @param schemasRefreshPeriodMs        the interval in ms; a value <= 0 disables the refresh;
     */
    public void setRefreshPeriodMs(long schemasRefreshPeriodMs)
    {
      _refreshPeriodMs = schemasRefreshPeriodMs;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      File schemaDirFile = new File(_schemaDir);
      if (_enabled && !_fallbackToResources && ! schemaDirFile.exists())
      {
        throw new InvalidConfigException("Schemas dir not found: " + _schemaDir);
      }
      return new StaticConfig(schemaDirFile, _refreshPeriodMs, _enabled, _fallbackToResources);
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    public void setEnabled(boolean enabled)
    {
      _enabled = enabled;
    }

    public boolean isFallbackToResources()
    {
      return _fallbackToResources;
    }

    public void setFallbackToResources(boolean fallbackToResources)
    {
      _fallbackToResources = fallbackToResources;
    }
  }

}
