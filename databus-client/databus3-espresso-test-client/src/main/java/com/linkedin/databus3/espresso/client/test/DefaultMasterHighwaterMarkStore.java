package com.linkedin.databus3.espresso.client.test;

import java.util.Iterator;

//import com.linkedin.espresso.storagenode.cluster.DefaultHighwaterMarkStore.EntriesDataUpdater;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordBucketizer;

public class DefaultMasterHighwaterMarkStore extends DefaultHighwaterMarkStore implements
    MasterHighwaterMarkStore
{
  public static final int MAX_MASTER_HWM_ENTRIES = 10;
  protected final String    _basePath;
  private ZNRecordBucketizer _bucketizer;

  public DefaultMasterHighwaterMarkStore(HighwaterMarkBackingStore<ZNRecord> backingStore, String basePath)
  {
    super(backingStore);
    _basePath = basePath;
    _bucketizer = new ZNRecordBucketizer(1);
  }

  @Override
  public EspressoSCN getSCNForGenerationStart(Partition partition, int generation) throws Exception
  {
    Iterator<MasterHighwaterMarkEntry> entriesIterator = getEntriesIterator(partition);
    while (entriesIterator.hasNext())
    {
      MasterHighwaterMarkEntry entry = entriesIterator.next();
      if (entry.getSCN().getGeneration() == generation)
      {
        return entry.getSCN();
      }
    }
    return null;
  }

  @Override
  public EspressoSCN getSCNForGenerationEnd(Partition partition, int generation) throws Exception
  {
    Iterator<MasterHighwaterMarkEntry> entriesIterator = getEntriesIterator(partition);
    while (entriesIterator.hasNext())
    {
      MasterHighwaterMarkEntry entry = entriesIterator.next();
      EspressoSCN scn = entry.getPrevGenerationEnd();
      if (scn != null && scn.getGeneration() == generation)
      {
        return scn;
      }
    }
    return null;
  }
  
  @Override
  public EspressoSCN getSCNForGeneration(Partition partition, int generation) throws Exception
  {
	  
    EspressoSCN scn = getSCNForGenerationEnd(partition, generation);
    if(scn == null)
    {
      scn = getSCNForGenerationStart(partition, generation);
    }
    
    return scn;
  }

  @Override
  public Iterator<MasterHighwaterMarkEntry> getEntriesIterator(ZNRecord property, Partition partition)
  {
    return new MasterEntriesIterator(property, partition);
  }

  @Override
  protected int getMaxEntries()
  {
    return MAX_MASTER_HWM_ENTRIES;
  }

  @Override
  public void write(MasterHighwaterMarkEntry entry) throws Exception
  {
    write(getPath(entry.getPartition()), entry);
  }
  
  @Override
  public void write(ZNRecord currentRecord, MasterHighwaterMarkEntry entry) throws Exception
  {
    write(getPath(entry.getPartition()), currentRecord, entry);
  }
  
  private String getPath(Partition partition)
  {
    String bucketName = _bucketizer.getBucketName(partition.toString());
    String dbName = partition.getDbName();
    if (bucketName != null)
    {
      return String.format("%s/%s/%s", _basePath, dbName, bucketName);
    }
    return String.format("%s/%s", _basePath, dbName);
  }

  public String getPathOld(String dbName)
  {
    return String.format("%s/%s", _basePath, dbName);
  }

  @Override
  public MasterHighwaterMarkEntry getLatestEntry(Partition partition) throws Exception
  {
    Iterator<MasterHighwaterMarkEntry> iterator = getEntriesIterator(partition);
    if(iterator.hasNext())
    {
      return iterator.next();
    }
    
    return null;
  }

  @Override
  public MasterHighwaterMarkEntry prepareNewEntry(Partition partition,
                                                   EspressoSCN prevGenEndSCN,
                                                   EspressoSCN newGenStartSCN,
                                                   String prevMaster,
                                                   String updater,
                                                   String sessionID)
  {
    return new MasterHighwaterMarkEntry(partition,
                                        prevGenEndSCN,
                                        newGenStartSCN,
                                        prevMaster,
                                        updater,
                                        sessionID,
                                        System.currentTimeMillis());
  }

  @Override
  public EspressoSCN getLatestSCN(Partition partition) throws Exception
  {
    return getLatestSCN(getProperty(partition), partition);
  }

  @Override
  public String getLatestMaster(Partition partition) throws Exception
  {
    return getLatestMaster(getProperty(partition), partition);
  }

  @Override
  public String getMasterForGeneration(Partition partition, int generation) throws Exception
  {
    return getMasterForGeneration(getProperty(partition), partition, generation);
  }

  @Override
  public Iterator<MasterHighwaterMarkEntry> getEntriesIterator(Partition partition) throws Exception
  {
    return getEntriesIterator(getProperty(partition), partition);
  }
  
  @Override
  public ZNRecord getProperty(Partition partition) throws Exception
  {
    return _backingStore.get(getPath(partition));
  }
  
  private class MasterEntriesIterator implements Iterator<MasterHighwaterMarkEntry>
  {
    private EntriesIterator _iterator;
    public MasterEntriesIterator(ZNRecord property, Partition partition)
    {
      _iterator = new EntriesIterator(property, partition);
    }
    @Override
    public boolean hasNext()
    {
      return _iterator.hasNext();
    }
    
    @Override
    public MasterHighwaterMarkEntry next()
    {
      return new MasterHighwaterMarkEntry(_iterator.next());
    }
    
    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }
}
