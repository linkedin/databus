package com.linkedin.databus.core;
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class DbusEventBufferMetaInfo {
  public static final Logger LOG = Logger.getLogger(DbusEventBufferMetaInfo.class);
    /**
   * helper class for buffer serialization
   */
  public static class BufferInfo {
    public static final String DELIMITER = ",";
    public int _pos;
    public int _limit;
    public int _cap;
    BufferInfo(int pos, int limit, int cap) {
      _pos = pos; _limit = limit; _cap = cap;
    }
    BufferInfo(String fromString) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException { // string format is "pos,limit,capacity"
      String [] info = fromString.split(DELIMITER);
      if(info.length != 3)
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException("parsing BufferInfo failed for " + fromString);
      try {
      _pos = Integer.parseInt(info[0]);
      _limit = Integer.parseInt(info[1]);
      _cap = Integer.parseInt(info[2]);
      } catch (NumberFormatException e) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(
              "parsing BufferInfo failed for " + fromString + " " + e.getLocalizedMessage());
      }
    }
    public int getLimit() { return _limit;}
    public int getPos() { return _pos; }
    public int getCapacity() { return _cap;}
    @Override
    public String toString() {
      return new String(_pos + DELIMITER + _limit + DELIMITER + _cap);
    }
  }

    public static class DbusEventBufferMetaInfoException extends IOException {
      public DbusEventBufferMetaInfoException(String string) {
        super(string);
      }
    
      public DbusEventBufferMetaInfoException(DbusEventBufferMetaInfo mi, String string) {
        super("[" + mi.toString() + "]:" + string);
      }
      private static final long serialVersionUID = 1L;
    }

    private static final int META_INFO_VERSION = 1;
    private static final char KEY_VALUE_SEP = ' ';
    private final Map<String, String> _info = new HashMap<String, String>(100);
    private boolean _valid = false;
    private File _file;
    public DbusEventBufferMetaInfo(File metaFile) {
      _file = metaFile;
    }
    public boolean isValid() {
      return _valid;
    }
    public String getSessionId() {
      return getVal("sessionId");
    }
    public void setSessionId(String sid) {
      setVal("sessionId", sid);
    }
    public DbusEventBufferMetaInfo.BufferInfo getReadBufferInfo() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
      return new DbusEventBufferMetaInfo.BufferInfo(getVal("readBufInfo"));
    }
    public DbusEventBufferMetaInfo.BufferInfo getScnIndexBufferInfo() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
      return new DbusEventBufferMetaInfo.BufferInfo(getVal("scnIndexBufferInfo"));
    }
    public void setScnIndexBufferInfo(DbusEventBufferMetaInfo.BufferInfo bi) {
      setVal("scnIndexBufferInfo", bi.toString());
    }
    public void setReadBufferInfo(DbusEventBufferMetaInfo.BufferInfo bi) {
      setVal("readBufInfo", bi.toString());
    }
    public void setVal(String key, String val) {
      _info.put(key, val);
    }
    String getVal(String key) {
      String val = _info.get(key);
//      if(val == null)
  //      return "";
      return val;
    }

    public long getLong(String key) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
      long l;
      try {
        l =  Long.parseLong(getVal(key));
      } catch (NumberFormatException e) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(this, "key="+key + " msg= " + e.getLocalizedMessage());
      }
      return l;
    }
    public int getInt(String key) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
      int i;
      try {
        i = Integer.parseInt(getVal(key));
      } catch (NumberFormatException e) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(this, e.getLocalizedMessage());
      }
      return i;
    }
    public boolean getBool(String key) {
      return Boolean.parseBoolean(getVal(key));
    }
    /**
     * load key/values form the file
     * line is separated by the 'space'
     * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
     */
    public boolean loadMetaInfo() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException{
      //
      FileReader fr;
      BufferedReader br = null;
      _valid = false;
      boolean debugEnabled = DbusEventBuffer.LOG.isDebugEnabled();
      try {
        fr = new FileReader(_file);
        br = new BufferedReader(fr);

        DbusEventBuffer.LOG.info("loading metaInfoFile " + _file);
        _info.clear();
        String line = br.readLine();
        while(line != null) {
          if(line.isEmpty() || line.charAt(0) == '#')
            continue;

          // format is key' 'val
          int idx = line.indexOf(KEY_VALUE_SEP);
          if(idx < 0) {
            DbusEventBuffer.LOG.warn("illegal line in metaInfoFile. line=" + line);
            continue;
          }
          String key = line.substring(0, idx);
          String val = line.substring(idx+1);
          _info.put(key, val);
          if(debugEnabled)
            DbusEventBuffer.LOG.debug("\tkey=" + key + "; val=" + val);
          line = br.readLine();
          _valid = true;
        }
      } catch (IOException e) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(this, "cannot read metaInfoFile:  " + e.getLocalizedMessage());
      } finally {
        if(br != null)
          try {
            br.close();
          } catch (IOException e) {
            DbusEventBuffer.LOG.warn("faild to close " + _file);
          }
      }

      int version = getInt("version");
      if(version != META_INFO_VERSION)
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(this,
              "metaInfoFile version doesn't match. Please remove the metafile and restart");

      if(isMetaFileOlderThenMMappedFiles(getSessionId())) {
        _valid = false; //not valid file - don't use
      }
      
      return _valid;
    }
    
    /**
     * 
     * @param sessionId
     * @return true if mmaped fiels have changed after metaInfo file
     */
    private boolean isMetaFileOlderThenMMappedFiles(String sessionId) {
      
      if (sessionId == null)
        return false;// valid case, no session is ok
      
      // one extra check is to verify that the session directory that contains the actual buffers
      // was not modified after the file was saved
      long metaFileModTime = _file.lastModified();
      LOG.debug(_file + " mod time: " + metaFileModTime);
      
      // check the directory first
      File sessionDir = new File(_file.getParent(), sessionId);
      long sessionDirModTime = sessionDir.lastModified();
      if(sessionDirModTime > metaFileModTime) {
        LOG.error("Session dir " + sessionDir + " seemed to be modified AFTER metaFile " + _file);
        return true;
      }
      
      // check each file in the directory
      String mmappedFiles[] = sessionDir.list();
      if(mmappedFiles == null) {
        LOG.error("There are no mmaped files in the session directory: " + sessionDir);
        return true;
      }
      
      for(String fName : mmappedFiles) {
        File f = new File(sessionDir, fName);
        long modTime = f.lastModified();
        LOG.debug(f + " mod time: " + modTime);
        if(modTime > metaFileModTime) {
          LOG.error("MMapped file " + f + "(" + modTime +  ") seemed to be modified AFTER metaFile "
        + _file + "(" + metaFileModTime + ")");
          return true;
        }
      }
      return false;// valid file - go ahead and use it
    }


    /**
     * reads cap, pos and limit for each buffer
     * @return
     * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
     */
    public DbusEventBufferMetaInfo.BufferInfo[] getBuffersInfo() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
      int bufNum = 0;
      try {
        bufNum = Integer.parseInt(_info.get("ByteBufferNum"));
      } catch (NumberFormatException e) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(this, e.getLocalizedMessage());
      }
      String bufInfoAll = _info.get("ByteBufferInfo");

      String [] buffersInfo = bufInfoAll.split(" ");
      if(buffersInfo.length != bufNum) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(this, "bufNum " + bufNum + " doesn't match bufInfo size [" + bufInfoAll + "]");
      }
      DbusEventBufferMetaInfo.BufferInfo [] bInfos = new DbusEventBufferMetaInfo.BufferInfo[bufNum];
      for(int i=0; i<buffersInfo.length; i++) {
        bInfos[i] = new DbusEventBufferMetaInfo.BufferInfo(buffersInfo[i]);
      }
      return bInfos;
    }

    public void saveAndClose() throws IOException {
      // update version
      setVal("version", Integer.toString(META_INFO_VERSION));

      if(_file.exists()) {
        File renameTo = new File(_file.getAbsoluteFile() + "." + System.currentTimeMillis());
        _file.renameTo(renameTo);
        DbusEventBuffer.LOG.warn("metaInfoFile " + _file + " exists. it is renambed to " + renameTo);
      }
      FileWriter fw = new FileWriter(_file);
      BufferedWriter bw = new BufferedWriter(fw);

      for(Map.Entry<String, String> e : _info.entrySet()) {
        bw.write(e.getKey() + KEY_VALUE_SEP + e.getValue());
        bw.newLine();
      }
      bw.close();
    }

    @Override
    public String toString() {
      return _file.getAbsolutePath();
    }
    
    
  }
