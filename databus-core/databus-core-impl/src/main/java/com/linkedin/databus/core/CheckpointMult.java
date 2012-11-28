package com.linkedin.databus.core;

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

  public CheckpointMult() {

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
}
