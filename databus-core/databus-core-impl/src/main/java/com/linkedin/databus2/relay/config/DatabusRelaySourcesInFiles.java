package com.linkedin.databus2.relay.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;


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

/**
 *
 * Data structure that saves and retrieves relay configurations per physical source in the file system.
 * This is not thread safe.
 *
 */
public class DatabusRelaySourcesInFiles implements DatabusRelaySources
{

  protected static final Logger LOG = Logger.getLogger(DatabusRelaySourcesInFiles.class.getName());
  final HashMap<String,PhysicalSourceConfig> _configs;
  final private String _dir;

  /**
   *
   * @param sourceDir : a directory from which all .json files are interpreted to be serialized PhysicalSourcesConfig objects
   * if directory does not exist an exception is thrown; if the path is a file; then an exception is thrown.
   *
   */
  public DatabusRelaySourcesInFiles(String sourceDir) throws Exception
  {
    File dir = new File(sourceDir);
    if (dir.isDirectory())
    {
      _dir = sourceDir;
      _configs = new HashMap<String,PhysicalSourceConfig> ();
    }
    else
    {
      throw new IOException("Source directory error: path=  " + sourceDir + " exists=" + dir.exists() +  " isDirectory=" + dir.isDirectory());
    }
  }

  /**
   * Given that _dir exists; either a source dir or
   * @return list of files
   */
  protected File[] getFiles()
  {
    File dir = new File(_dir);
    File[] listFiles = dir.listFiles(new FilenameFilter()
      {
        @Override
        public boolean accept(File dir, String name)
        {
          return name.endsWith(".json") || name.endsWith(".JSON");
        }
      });
    return listFiles;
  }

  protected ArrayList<PhysicalSourceConfig> getConfigs()
  {
    File[] listFiles = getFiles();
    ArrayList<PhysicalSourceConfig> configs = new ArrayList<PhysicalSourceConfig> ();
    for (File f:listFiles)
    {
      try
      {

        PhysicalSourceConfig pConfig = PhysicalSourceConfig.fromFile(f);
        //not sure if this always returns an object; that's why the check is present
        if (pConfig != null)
        {
          configs.add(pConfig);
        }
        else
        {
          LOG.warn("Unable to deserialize " + f.getAbsolutePath() + " Skipping!" );
        }
      }
      catch (Exception e)
      {
        LOG.warn("Skipping file: " + f.getAbsolutePath() + " Exception: " + e.getMessage());
      }
    }
    return configs;
  }

  @Override
  public boolean add(String sourceName, PhysicalSourceConfig config)
  {
    if (!_configs.containsKey(sourceName))
    {
      _configs.put(sourceName, config);
      return true;
    }
    LOG.warn("Source " + sourceName + " already exists! Not adding config. Please remove entry using remove()");
    return false;
  }

  @Override
  public PhysicalSourceConfig get(String sourceName)
  {
    return _configs.get(sourceName);
  }

  protected void clear()
  {
    _configs.clear();
  }

  public void removePersistedEntries()
  {
    File[] files = getFiles();
    for (File f:files)
    {
      f.delete();
    }
  }

  @Override
  public boolean save()
  {
    boolean ret = true;
    for (Entry<String,PhysicalSourceConfig> entry: _configs.entrySet())
    {
        String fileName = "sources-" + entry.getKey() + ".json";
        String filePath  = _dir + "/" + fileName;
        if (entry.getValue()==null)
        {
          LOG.warn("Sourcename " + entry.getKey() + " has null value. Not persisting.");
        }
        else
        {
          try
          {
            FileOutputStream file  = new FileOutputStream(filePath,false);
            String val = entry.getValue().toString();
            file.write(val.getBytes());
            file.close();
          }
          catch (Exception e)
          {
            LOG.warn("Cannot save " + filePath +  " for source " + entry.getKey());
            ret = false;
          }
        }
    }
    return ret;
  }


  @Override
  public boolean load()
  {
    clear();
    ArrayList<PhysicalSourceConfig> configs = getConfigs();
    for (PhysicalSourceConfig conf : configs)
    {
      add(conf.getName(),conf);
    }
    return true;
  }

  @Override
  public boolean removeAll()
  {
    clear();
    return true;
  }

  @Override
  public PhysicalSourceConfig[] getAll()
  {
    PhysicalSourceConfig[] pConfigs = new PhysicalSourceConfig[_configs.size()];
    int i=0;
    for (Object o: _configs.values().toArray())
    {
      pConfigs[i++] = (PhysicalSourceConfig) o;
    }
    return pConfigs;
  }

  @Override
  public boolean remove(String sourceName)
  {
    if (_configs.containsKey(sourceName))
    {
      _configs.remove(sourceName);
      return true;
    }
    return false;
  }

  @Override
  public int size()
  {
    return _configs.size();
  }



}
