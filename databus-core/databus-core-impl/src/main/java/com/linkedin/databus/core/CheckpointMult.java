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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.data_model.PhysicalPartition;

/**
 * Constructs a checkpoint for multiple buffers.
 * Essentially it is a list of single buffer checkpoints mapped by physical partition
 * 
 */
public class CheckpointMult
{
  public static final String MODULE = DbusEventBufferMult.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private Map<PhysicalPartition, Checkpoint> _pPart2Checkpoint = new HashMap<PhysicalPartition, Checkpoint>();
  private static ObjectMapper _mapper = new ObjectMapper();
  private static String CURSOR_PARTITION_KEY = "cursorPartition";

  /**
   * _cursorPartition has the last partition from which an event was sent (could be partial or full window)
   * to the receiver over a channel.
   *
   * To avoid upgrade problems, we do not serialize _cursorPartition in the map, but we deserialize it if
   * we see it.
   *
   * The cursorPartition is only used as a hint, and its absense will not affect the correctness. Specifically,
   * it is expected that the cursorPartition is ignored if any one of the checkpoints indicates that a partial
   * window was sent.
   */
  private PhysicalPartition _cursorPartition;

  public CheckpointMult() {
    _cursorPartition = null;
  }
  /**
   * reconstruct Mult checkpoint from a string representation
   * @param Mult checkpoint serialization string
   * @return CheckpointMult object
   */
  @SuppressWarnings("unchecked")
  public CheckpointMult(String checkpointString)
  throws JsonParseException, JsonMappingException, IOException
  {
    // TODO - catch specific exceptions, but what to return?? create an empty one??
    if (null != checkpointString) {
      // json returns Map between "pSrcId" and 'serialized string' of Checkpoint
      Map<String, String> map = _mapper.readValue(
                            new ByteArrayInputStream(checkpointString.getBytes()), Map.class);
      boolean debugEnabled = LOG.isDebugEnabled();
      for(Entry<String, String> m : map.entrySet()) {
        if (m.getKey().equals(CURSOR_PARTITION_KEY)) {
          _cursorPartition = PhysicalPartition.createFromJsonString(m.getValue());
          continue;
        } else if (!m.getKey().startsWith("{")) {
          // Ignore anything we don't understand.
          if (debugEnabled) {
            LOG.debug("Ignoring checkpoint mult key" + m.getKey());
          }
          continue;
        }
        PhysicalPartition pPart = PhysicalPartition.createFromJsonString(m.getKey());
        String cpString = m.getValue();//serialized checkpoint
        Checkpoint cp = new Checkpoint(cpString);
        if(debugEnabled)
          LOG.debug("CPMULT constructor: pPart="+pPart + ";cp="+cp);
        _pPart2Checkpoint.put(pPart, cp);
      }
    }
  }

  /**
   * returns checkpoint for a specific physical partition
   * @param pPart
   * @return checkpoint for the partition
   */
  public Checkpoint getCheckpoint(PhysicalPartition pPart) {
    return _pPart2Checkpoint.get(pPart);
  }

  /**
   * adds a new checkpoint
   * @param pPart
   * @param cp
   */
  public void addCheckpoint(PhysicalPartition pPart, Checkpoint cp) {
    _pPart2Checkpoint.put(pPart, cp);
  }

  /**
   * serialize CheckpointMult into the stream
   * @param stream
   */
  void serialize(OutputStream outStream) throws JsonGenerationException,
  JsonMappingException,
  IOException
  {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // first convert checkpointmult into a map
    Map<String, String> map = new HashMap<String, String>();
    boolean debugEnabled = LOG.isDebugEnabled();
    for(Entry<PhysicalPartition, Checkpoint> e: _pPart2Checkpoint.entrySet()) {
      baos.reset();
      Checkpoint cp = e.getValue();
      cp.serialize(baos);
      String pPartJson = e.getKey().toJsonString();
      map.put(pPartJson, baos.toString());
      if(debugEnabled)
        LOG.debug("phSourId=" + e.getKey() + ";cp =" + baos.toString());
    }
    _mapper.writeValue(outStream, map);
  }

  @Override
  public String toString() {
    ByteArrayOutputStream bs = new ByteArrayOutputStream();
    try {
      serialize(bs);
    } catch (IOException e) {
      LOG.warn("toString failed", e);
    }
    return bs.toString();
  }

  public int getNumCheckponts() {
    return _pPart2Checkpoint.size();
  }

  public PhysicalPartition getCursorPartition()
  {
    return _cursorPartition;
  }

  public void setCursorPartition(PhysicalPartition cursorPartition)
  {
    _cursorPartition = cursorPartition;
  }

}
