package com.linkedin.databus.core;

import java.nio.ByteBuffer;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import java.io.IOException;


/**
 * A set of static utility functions that work irrespective of event version.
 * TODO Potential candidates to move into this class:
 *   getErrorEventFromDbusEvent()
 *   getCheckpointFromEvent()
 *   createCheckpointEvent()
 *
 *
 */
public class DbusEventUtils
{
  public static boolean isControlSrcId(short srcId)
  {
    return srcId < 0;
  }
  /**
   * Utility method to extract a checkpoint from a DbusEvent
   * Note: Ensure that this is a Checkpoint event before calling this method.
   * @param event
   * @return
   */
  public static Checkpoint getCheckpointFromEvent(DbusEvent event)
  {
    assert (event.isCheckpointMessage());
    ByteBuffer valueBuffer = event.value();
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);
    try
    {
      Checkpoint newCheckpoint = new Checkpoint(new String(valueBytes));
      return newCheckpoint;
    }
    catch (JsonParseException e)
    {
      throw new RuntimeException(e);
    }
    catch (JsonMappingException e)
    {
      throw new RuntimeException(e);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }

  }

  /**
   * Utility method to extract a SCN regress message from a DbusEvent
   * Note: Ensure that this is a SCNRegressMessage event before calling this method.
   * @param event
   * @return
   */
  public static SCNRegressMessage getSCNRegressFromEvent(DbusEvent event)
  {
    assert (event.isSCNRegressMessage());
    ByteBuffer valueBuffer = event.value();
    byte[] valueBytes = new byte[valueBuffer.limit()];
    valueBuffer.get(valueBytes);
    SCNRegressMessage newRegressMsg = SCNRegressMessage.getRegressMessage(new String(valueBytes));
    return newRegressMsg;
  }


  // TODO Consider moving appendEvent to buffer into this class as a static method.
}
