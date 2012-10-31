package com.linkedin.databus3.espresso.client.test;

import java.util.Map;


/**
 * Represent a master high water mark entry
 * @author sjagadis
 *
 */
public class MasterHighwaterMarkEntry extends HighwaterMarkEntry
{

  public MasterHighwaterMarkEntry(Partition partition,
                                  EspressoSCN prevGenEndSCN,
                                  EspressoSCN newGenStartSCN,
                                  String prevMaster,
                                  String updater,
                                  String sessionID,
                                  long timestamp)
  {
    super(partition, newGenStartSCN, updater, sessionID, timestamp);
    _map.put("Master", updater); //XXX: Remove this after fixing dependencies
    if(prevMaster != null)
    {
      _map.put("PrevMaster", prevMaster);
    }
    
    if(prevGenEndSCN != null)
    {
      _map.put("PrevGenEndSCN", prevGenEndSCN.toString());
    }
  }
  
  MasterHighwaterMarkEntry(Map<String, String> map)
  {
    super(map);
  }

  /**
   * 
   * @return master for the previous generation if any. null if there is no previous generation
   */
  public String getPrevMaster()
  {
    return _map.get("PrevMaster");
  }

  /**
   * 
   * @return SCN corresponding to end of previous generation. null if no prev gen
   */
  public EspressoSCN getPrevGenerationEnd()
  {
    return (_map.containsKey("PrevGenEndSCN") ? EspressoSCN.parseSCN(_map.get("PrevGenEndSCN")) : null);
  }

  /**
   * @return the master for the current generation
   */
  @Override
  public String getMaster()
  {
    return getUpdater();
  }
}
