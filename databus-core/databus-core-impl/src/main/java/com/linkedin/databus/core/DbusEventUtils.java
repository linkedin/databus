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


import java.io.IOException;
import java.nio.ByteBuffer;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;


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
  public static boolean isControlSrcId(int srcId)
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
