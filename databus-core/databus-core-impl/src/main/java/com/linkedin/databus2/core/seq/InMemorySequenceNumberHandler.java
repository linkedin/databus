package com.linkedin.databus2.core.seq;
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


import com.linkedin.databus2.core.DatabusException;

/** Simple sequence number handler where the sequence number stored only in memory */
public class InMemorySequenceNumberHandler implements MaxSCNReaderWriter
{
  private volatile long _seqNumber;

  /** Constructor with initial sequence number -1 */
  public InMemorySequenceNumberHandler()
  {
    this(-1);
  }

  public InMemorySequenceNumberHandler(long initValue)
  {
    _seqNumber = initValue;
  }

  @Override
  public long getMaxScn() throws DatabusException
  {
    return _seqNumber;
  }

  @Override
  public void saveMaxScn(long endOfPeriod) throws DatabusException
  {
    _seqNumber = endOfPeriod;
  }

}
