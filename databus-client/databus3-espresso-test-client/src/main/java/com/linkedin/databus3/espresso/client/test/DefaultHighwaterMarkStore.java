package com.linkedin.databus3.espresso.client.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.I0Itec.zkclient.DataUpdater;
import com.google.common.base.Joiner;

import com.linkedin.helix.ZNRecord;

public abstract class DefaultHighwaterMarkStore
{
  protected HighwaterMarkBackingStore<ZNRecord> _backingStore;

  public DefaultHighwaterMarkStore(HighwaterMarkBackingStore<ZNRecord> backingStore)
  {
    _backingStore = backingStore;
  }
  
  protected EspressoSCN getLatestSCN(ZNRecord property, Partition partition) throws Exception
  {
    Iterator<? extends HighwaterMarkEntry> entriesIterator = getEntriesIterator(property, partition);
    if (entriesIterator.hasNext())
    {
      return entriesIterator.next().getSCN();
    }
    
    return null;
  }
  
  protected String getLatestMaster(ZNRecord property, Partition partition) throws Exception
  {
    Iterator<? extends HighwaterMarkEntry> entriesIterator = getEntriesIterator(property, partition);
    if (entriesIterator.hasNext())
    {
      return entriesIterator.next().getMaster();
    }

    return null;
  }


  protected String getMasterForGeneration(ZNRecord property, Partition partition, int generation) throws Exception
  {
    Iterator<? extends HighwaterMarkEntry> entriesIterator = getEntriesIterator(property, partition);
    while (entriesIterator.hasNext())
    {
      HighwaterMarkEntry entry = entriesIterator.next();
      if (entry.getSCN().getGeneration() == generation)
      {
        return entry.getMaster();
      }
    }
    return null;
  }

  protected abstract Iterator<? extends HighwaterMarkEntry> getEntriesIterator(ZNRecord property, Partition partition);

  
  protected Map<String, String> lookupMap(ZNRecord property,
                                          Partition partition,
                                          String scnStr,
                                          String instance)
  {
    if (property != null)
    {
      String key =
          Joiner.on("_").join(partition.toString(), scnStr, instance);

      return property.getMapField(key);
    }

    return null;
  }

  protected void write(String path, HighwaterMarkEntry entry) throws Exception
  {
    EntriesDataUpdater updater = new EntriesDataUpdater(path, entry);
    _backingStore.put(path, updater);
  }
  
  public void write(String path, ZNRecord currentRecord, HighwaterMarkEntry entry) throws Exception
  {
    EntriesDataUpdater updater = new EntriesDataUpdater(path, entry);
    ZNRecord updatedProperty = updater.getUpdatedProperty(entry, path, currentRecord);
    _backingStore.put(path, updatedProperty);
  }

  protected abstract int getMaxEntries();


  protected List<String> getInstancesList(ZNRecord property, Partition partition)
  {
    if (property != null)
    {
      return property.getListField(partition.toString() + "_instances");
    }
    return null;
  }

  protected List<String> getSCNList(ZNRecord property, Partition partition)
  {
    if (property != null)
    {
      return property.getListField(partition.toString());
    }

    return null;
  }
  
  protected class EntriesDataUpdater implements DataUpdater<ZNRecord>
  {
    private final String _path;
    private final HighwaterMarkEntry _entry;

    public EntriesDataUpdater(String path, HighwaterMarkEntry entry)
    {
      _path = path;
      _entry = entry;
    }
    
    @Override
    public ZNRecord update(ZNRecord currentData)
    {
      ZNRecord updatedProperty = getUpdatedProperty(_entry, _path, currentData);
      cleanupOldHwmEntries(updatedProperty,
                           _entry.getPartition().toString(),
                           getMaxEntries());    
      
      return updatedProperty;
    }
    
    protected ZNRecord getUpdatedProperty(HighwaterMarkEntry entry,
                                          String path,
                                          ZNRecord property)
    {
      if (property == null)
      {
        property = new ZNRecord(path);
      }

      updateScnList(property, entry);
      updateInstanceList(property, entry);
      updateMapField(property, entry);

      return property;
    }
    
    protected void updateMapField(ZNRecord property, HighwaterMarkEntry entry)
    {
      String key =
          Joiner.on("_").join(entry.getPartition().toString(),
                              entry.getSCN().getSCN(),
                              entry.getUpdater());
      property.setMapField(key, entry.getMap());
    }

    protected void updateScnList(ZNRecord property, HighwaterMarkEntry entry)
    {
      List<String> scnList = getSCNList(property, entry.getPartition());
      if (scnList == null)
      {
        scnList = new ArrayList<String>();
      }
      scnList.add(0, "" + entry.getSCN().getSCN());
      setSCNList(property, entry.getPartition(), scnList);
    }

    protected void updateInstanceList(ZNRecord property, HighwaterMarkEntry entry)
    {
      List<String> instanceList = getInstancesList(property, entry.getPartition());
      if (instanceList == null)
      {
        instanceList = new ArrayList<String>();
      }
      instanceList.add(0, entry.getUpdater());
      setInstancesList(property, entry.getPartition(), instanceList);
    }

    protected void cleanupOldHwmEntries(ZNRecord property,
                                        String partitionStr,
                                        int maxEntries)
    {
      if (property != null)
      {
        EspressoStateUnitKey stateUnitKey = new EspressoStateUnitKey(partitionStr);
        Partition partition =
            new Partition(stateUnitKey.getDBName(), stateUnitKey.getPartitionId());
        List<String> scnList = getSCNList(property, partition);
        List<String> instances = getInstancesList(property, partition);

        Map<String, Map<String, String>> mapFields = property.getMapFields();
        if (scnList != null && scnList.size() > maxEntries && instances != null
            && instances.size() == scnList.size() && mapFields != null)
        {
          for (int i = maxEntries; i < scnList.size(); i++)
          {
            mapFields.remove(Joiner.on("_").join(partition,
                                                 scnList.get(i),
                                                 instances.get(i)));
          }
          property.setMapFields(mapFields);

          scnList.subList(maxEntries, scnList.size()).clear();
          instances.subList(maxEntries, instances.size()).clear();

          setSCNList(property, partition, scnList);
          setInstancesList(property, partition, instances);
        }
      }
    }

    protected void setInstancesList(ZNRecord property,
                                    Partition partition,
                                    List<String> instances)
    {
      property.setListField(partition.toString() + "_instances", instances);
    }

    protected void setSCNList(ZNRecord property, Partition partition, List<String> scnList)
    {
      property.setListField(partition.toString(), scnList);
    }

  }
  
  protected class EntriesIterator implements Iterator<Map<String, String>>
  {
    private Iterator<String> _scnListIterator  = null;
    private Iterator<String> _instanceIterator = null;
    private List<String>     _scnList;
    private List<String>     _instancesList;
    private ZNRecord         _property;
    private final Partition  _partition;

    public EntriesIterator(ZNRecord property, Partition partition)
    {
      _partition = partition;
      _property = property;
      if (_property != null)
      {
        _scnList = getSCNList(_property, _partition);
        _instancesList = getInstancesList(_property, _partition);
      }

      if (_scnList != null && _instancesList != null)
      {
        _scnListIterator = _scnList.iterator();
        _instanceIterator = _instancesList.iterator();
      }
    }

    @Override
    public boolean hasNext()
    {
      return (_scnListIterator != null && _scnListIterator.hasNext()
          && _instanceIterator != null && _instanceIterator.hasNext());
    }

    @Override
    public Map<String, String> next()
    {
      if (!hasNext())
      {
        throw new NoSuchElementException("Cannot call next() : no more entries");
      }

      String scnStr = _scnListIterator.next();
      String instance = _instanceIterator.next();
      return lookupMap(_property, _partition, scnStr, instance);
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }
}
