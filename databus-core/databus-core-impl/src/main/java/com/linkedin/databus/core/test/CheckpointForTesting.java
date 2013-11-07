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
package com.linkedin.databus.core.test;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.databus.core.Checkpoint;

//TODO need to investigate why we need this. Most likely a test is trying to do something funky. We need to fix the test
//and get rid of this class.
/**
 * A helper class that provides access to protected members of Checkpoint. This class should be
 * used for testing purposes only.
 */
public class CheckpointForTesting extends Checkpoint
{
  private static final long serialVersionUID = 1L;

  public CheckpointForTesting()
  {
    super();
  }

  public CheckpointForTesting(String serializedCheckpoint) throws JsonParseException,
      JsonMappingException,
      IOException
  {
    super(serializedCheckpoint);
  }

  @Override
  public void setSnapshotSource(int sourceIndex, String sourceName)
  {
    super.setSnapshotSource(sourceIndex, sourceName);
  }

  @Override
  public void setCatchupSource(int sourceIndex, String sourceName)
  {
    super.setCatchupSource(sourceIndex, sourceName);
  }

}
