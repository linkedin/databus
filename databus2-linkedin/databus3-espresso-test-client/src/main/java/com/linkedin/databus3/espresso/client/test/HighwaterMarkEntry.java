package com.linkedin.databus3.espresso.client.test;

import java.util.HashMap;
import java.util.Map;

public abstract class HighwaterMarkEntry
{

  protected Map<String, String> _map;

  public HighwaterMarkEntry(Partition partition, EspressoSCN scn, String updater, String sessionID, long timestamp)
  {
    _map = new HashMap<String, String>();
    _map.put("Partition", partition.toString());
    _map.put("SCN", "" + scn.toString());
    _map.put("Updater", updater);
    _map.put("Session", sessionID);
    _map.put("Timestamp", "" + timestamp);
  }
  
  protected HighwaterMarkEntry(Map<String, String> map)
  {
    _map = map;
  }

  public String getUpdater()
  {
    return _map.get("Updater");

  }

  public Partition getPartition()
  {
    EspressoStateUnitKey key = new EspressoStateUnitKey(_map.get("Partition"));
    return new Partition(key.getDBName(), key.getPartitionId());
  }

  public String getSessionID()
  {
    return _map.get("Session");
  }

  public long getTimestamp()
  {
    return Long.parseLong(_map.get("Timestamp"));
  }

  public EspressoSCN getSCN()
  {
    return EspressoSCN.parseSCN(_map.get("SCN"));
  }
  
  public Map<String, String> getMap()
  {
    return _map;
  }
  
  public abstract String getMaster();
  
  @Override
  public String toString()
  {
    return _map.toString();
  }
}
