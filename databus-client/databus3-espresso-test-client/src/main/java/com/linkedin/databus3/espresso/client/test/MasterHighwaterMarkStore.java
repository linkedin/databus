package com.linkedin.databus3.espresso.client.test;

import java.util.Iterator;
import com.linkedin.helix.ZNRecord;

//all the getters can return null if the entries don't exist 
public interface MasterHighwaterMarkStore
{
  public EspressoSCN getLatestSCN(Partition partition) throws Exception;

  public EspressoSCN getSCNForGenerationStart(Partition partition, int generation) throws Exception;

  public EspressoSCN getSCNForGenerationEnd(Partition partition, int generation) throws Exception;
  
  //convenience method - return getSCNForGenerationEnd() if non-null. otherwise, return getSCNForGenerationStart()
  public EspressoSCN getSCNForGeneration(Partition partition, int generation) throws Exception;

  public String getLatestMaster(Partition partition) throws Exception;
  
  public String getMasterForGeneration(Partition partition, int generation) throws Exception;
  
  public Iterator<MasterHighwaterMarkEntry> getEntriesIterator(Partition partition) throws Exception;
  
  public ZNRecord getProperty(Partition partition) throws Exception;
  
  public MasterHighwaterMarkEntry getLatestEntry(Partition partition) throws Exception;
  
  public MasterHighwaterMarkEntry prepareNewEntry(Partition partition,
                                                  EspressoSCN prevGenEndSCN,
                                                  EspressoSCN newGenStartSCN,
                                                  String prevMaster,
                                                  String updater,
                                                  String sessionID);

  public void write(MasterHighwaterMarkEntry entry) throws Exception;
  
  public void write(ZNRecord currentRecord, MasterHighwaterMarkEntry entry) throws Exception;

}
