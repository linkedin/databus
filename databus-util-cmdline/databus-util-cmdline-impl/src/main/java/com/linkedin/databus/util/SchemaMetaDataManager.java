package com.linkedin.databus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;


/**
 *
 * This Class manages 2 meta files and is responsible for generating SourceIds for new sources.
 *
 * physical_logical_src_map.json :
 *    This is an json map containing mapping between schema name and the list of logical sources that are databusified
 *    eg:   "slink" : [ "com.linkedin.events.slink.Shortlinks" ],
 *
 *
 * idToName.map :
 *    This file contains mapping from sourceId to logical source names
 *     e.g : 2601:com.linkedin.events.slink.Shortlinks
 *
 */
public class SchemaMetaDataManager
{
  private Short _maxSrcId = -1;
  private SourceIdNameRegistry _idNameRegistry = new SourceIdNameRegistry();
  private Map<String, TreeSet<String>> _physicalToLogicalSrcMap = null;

  private final String _idNameMapFile;
  private final String _physicalToLogicalSrcMapFile;

  public static final String SRC_ID_NAME_MAP_FILE =  "idToName.map";
  public static final String PHYSICAL_TO_LOGICAL_SRC_MAP_FILE =  "physical_logical_src_map.json";

  public SchemaMetaDataManager(String schemaRegistryLocation) throws IOException
  {
    _idNameMapFile = schemaRegistryLocation + "/" +  SRC_ID_NAME_MAP_FILE;
    _physicalToLogicalSrcMapFile = schemaRegistryLocation + "/" + PHYSICAL_TO_LOGICAL_SRC_MAP_FILE;
    populatePhysicalToLogicalSrcMap();
    populateSrcNameIdMap();
  }

  public void store() throws IOException
  {
    persistPhysicalToLogicalSrcMap();
    persistSrcNameIdMap();
  }

  private void populatePhysicalToLogicalSrcMap() throws IOException
  {
    FileInputStream fStream = null;
    try
    {
      fStream = new FileInputStream(new File(_physicalToLogicalSrcMapFile));

      ObjectMapper m = new ObjectMapper();
      _physicalToLogicalSrcMap = m.readValue(fStream,new TypeReference<TreeMap<String,TreeSet<String>>>(){});
    } finally {
      if ( null != fStream)
        fStream.close();
    }
  }

  private void persistPhysicalToLogicalSrcMap() throws IOException
  {
    ObjectMapper m = new ObjectMapper();
    String str = m.defaultPrettyPrintingWriter().writeValueAsString(_physicalToLogicalSrcMap);
    File tmpFile = File.createTempFile("phyToLogicalSrc", ".json.tmp");

    FileWriter oStream = null;
    try
    {
      oStream = new FileWriter(tmpFile);
      oStream.append(str);
    } finally {
      if ( null != oStream)
        oStream.close();
    }
    File destFile = new File(_physicalToLogicalSrcMapFile);
    boolean success = tmpFile.renameTo(destFile);

    if  ( ! success )
      throw new RuntimeException("Unable to persist the mapping json file !!");
  }

  private void populateSrcNameIdMap()
    throws IOException
  {
    int ln = 0;
    File file = new File(_idNameMapFile);
    BufferedReader reader = null;

    try
    {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

      String line = null;
      List<IdNamePair> pairCollection = new ArrayList<IdNamePair>();
      Set<String> srcNames = new HashSet<String>();
      Set<Short> srcIds = new HashSet<Short>();

      while ( (line = reader.readLine()) != null)
      {
        line = line.trim();
        ln++;

        // omit  comments
        if (line.startsWith("#"))
          continue;

        // if line has comments elsewhere, omit comtents to the right of it.
        String[] parts = line.split("#");
        line = parts[0];

        String[] toks = line.split(":");

        if (toks.length != 2)
          throw new RuntimeException("SrcIdToName map file (" + file + ") is corrupted at line number :" + ln);

        short srcId = new Short(toks[0]);
        String srcName = toks[1];
        _maxSrcId = (short)Math.max(_maxSrcId, srcId);
        if (srcIds.contains(srcId))
        {
          throw new RuntimeException("Duplicate SrcId (" + srcId + ") present in the file :" + _idNameMapFile);
        } else if (srcNames.contains(srcName)) {
          throw new RuntimeException("Duplicate Source name (" + srcName + ") present in the file :" + _idNameMapFile);
        }
        srcIds.add(srcId);
        srcNames.add(srcName);
        pairCollection.add(new IdNamePair(Long.valueOf(srcId), srcName));
      }
      _idNameRegistry.updateFromIdNamePairs(pairCollection);
    } finally {
      if ( null != reader)
        reader.close();
    }
  }

  private void persistSrcNameIdMap() throws IOException
  {
    File tmpFile = File.createTempFile("srcIdToName", ".map.tmp");

    FileWriter oStream = null;

    try
    {
      oStream = new FileWriter(tmpFile);

      TreeSet<LogicalSource>  srcs = new TreeSet<LogicalSource>(new Comparator<LogicalSource>()
          {

        @Override
        public int compare(LogicalSource o1, LogicalSource o2)
        {
          return o1.getId().compareTo(o2.getId());
        }
          });

      srcs.addAll(_idNameRegistry.getAllSources());

      for(LogicalSource k : srcs)
      {
        oStream.append(k.getId().toString());
        oStream.append(":");
        oStream.append(k.getName());
        oStream.append("\n");
      }
    } finally {
      if (null != oStream)
        oStream.close();
    }
    File destFile = new File(_idNameMapFile);
    boolean success = tmpFile.renameTo(destFile);

    if  ( ! success )
      throw new RuntimeException("Unable to persist the mapping file !!");
  }

  public short updateAndGetNewSrcId(String dbName, String srcName)
     throws DatabusException
  {
    _maxSrcId++;

    dbName = dbName.toLowerCase(Locale.ENGLISH);

    if ( null != _idNameRegistry.getSource(srcName))
      throw new DatabusException("Source Name (" + srcName + ") already available in the schema registry !!");

    TreeSet<String> s = null;
    if (_physicalToLogicalSrcMap.containsKey(dbName))
    {
       s = _physicalToLogicalSrcMap.get(dbName);
    } else {
      s = new TreeSet<String>();
      _physicalToLogicalSrcMap.put(dbName,s);
    }

    s.add(srcName);
    IdNamePair pair = new IdNamePair(Long.valueOf(_maxSrcId), srcName);
    LogicalSource src = new LogicalSource(pair);
    List<LogicalSource> newSources = new ArrayList<LogicalSource>();
    newSources.add(src);
    _idNameRegistry.add(newSources);

    return _maxSrcId;
  }

  public List<String> getManagedSourcesForDB(String db)
  {
     Set<String> srcs =  _physicalToLogicalSrcMap.get(db);

     if ( null == srcs)
       return null;

     List<String> srcNames  = new ArrayList<String>(srcs);

     Collections.sort(srcNames, new Comparator<String>() {
         @Override
         public int compare(String o1, String o2)
         {
           Integer srcId1 = _idNameRegistry.getSourceId(o1);
           Integer srcId2 = _idNameRegistry.getSourceId(o2);

           return srcId1.compareTo(srcId2);
         }
       });

     return srcNames;
  }

  public short getSrcId(String srcName)
  {
    LogicalSource src = _idNameRegistry.getSource(srcName);

    if (src != null)
      return src.getId().shortValue();

    throw new RuntimeException("Unable to find the source id for the source name (" + srcName + ")");
  }

  public Map<String, TreeSet<String>> getDbToSrcMap()
  {
    return _physicalToLogicalSrcMap;
  }
}
