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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.util.IdNamePair;

/** Keeps a bi-directional mapping from a source name to and from source id. */
public class SourceIdNameRegistry
{
  public static final String MODULE = SourceIdNameRegistry.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final Hashtable<String, ChangeListener> _listeners;
  private volatile HashMap<String, LogicalSource> _nameIndex;
  private volatile HashMap<Integer, LogicalSource> _idIndex;

  public SourceIdNameRegistry()
  {
    _listeners = new Hashtable<String, SourceIdNameRegistry.ChangeListener>();
    _nameIndex = new HashMap<String, LogicalSource>();
    _idIndex = new HashMap<Integer, LogicalSource>();
  }

  public SourceIdNameRegistry(Collection<LogicalSource> initPairs)
  {
    this();
    update(initPairs);
  }

  public static SourceIdNameRegistry createFromIdNamePairs(Collection<IdNamePair> idNamePairs)
  {
    SourceIdNameRegistry result = new SourceIdNameRegistry();
    result.updateFromIdNamePairs(idNamePairs);
    return result;
  }

  /** Return the id associated with the name or null if none exists */
  public Integer getSourceId(String sourceName)
  {
    LogicalSource pair = _nameIndex.get(sourceName);
    return null != pair ? pair.getId() : null;
  }

  public String getSourceName(Integer id)
  {
    LogicalSource pair = _idIndex.get(id);
    return null != pair ? pair.getName() : null;
  }

  public LogicalSource getSource(Integer id)
  {
    return _idIndex.get(id);
  }

  public LogicalSource getSource(String sourceName)
  {
    return _nameIndex.get(sourceName);
  }

  public Collection<LogicalSource> getAllSources()
  {
    return _idIndex.values();
  }

  public void updateFromIdNamePairs(Collection<IdNamePair> newPairs)
  {
    ArrayList<LogicalSource> srcCollection = new ArrayList<LogicalSource>(newPairs.size());
    for (IdNamePair pair: newPairs) srcCollection.add(new LogicalSource(pair.getId().intValue(),
                                                                        pair.getName()));
    update(srcCollection);
  }

  public void update(Collection<LogicalSource> newPairs)
  {
    HashMap<String, LogicalSource> newNameIndex = new HashMap<String, LogicalSource>((int)(newPairs.size() * 1.3));
    HashMap<Integer, LogicalSource> newIdIndex = new HashMap<Integer, LogicalSource>((int)(newPairs.size() * 1.3));
    for (LogicalSource pair: newPairs)
    {
      newNameIndex.put(pair.getName(), pair);
      newIdIndex.put(pair.getId(), pair);
    }

    synchronized (this)
    {
      _nameIndex = newNameIndex;
      _idIndex = newIdIndex;

      if (LOG.isDebugEnabled())
      {
          LOG.debug("sources updated: " + _idIndex.values());
      }

      for (Map.Entry<String, ChangeListener> listenerEntry: _listeners.entrySet())
      {
        try
        {
          listenerEntry.getValue().onRegistryChange();
        }
        catch (RuntimeException re)
        {
          LOG.error("error in listener callback [" + listenerEntry.getKey() + "]: " + re.getMessage(),
                    re);
        }
      }
    }
  }

  public synchronized void addListener(String listenerName, ChangeListener listener)
  {
    if (_listeners.contains(listenerName)) throw new RuntimeException("duplicate listener name:" + listenerName);
    _listeners.put(listenerName, listener);
  }

  public synchronized void removeListener(String listenerName)
  {
    _listeners.remove(listenerName);
  }

  public synchronized void removeListener(ChangeListener listener)
  {
    for (Map.Entry<String, ChangeListener> l: _listeners.entrySet())
    {
      if (listener == l.getValue()) _listeners.remove(l.getKey());
    }
  }

  /** Defines the callback interface for listeners who care about changes to the registry */
  public static interface ChangeListener
  {
    /** A callback when the registry contents changed */
    void onRegistryChange();
  }
}
