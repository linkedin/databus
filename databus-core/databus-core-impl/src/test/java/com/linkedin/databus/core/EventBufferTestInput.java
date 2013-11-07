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


import java.lang.reflect.Field;

import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.test.DbusEventCorrupter.EventCorruptionType;
import com.linkedin.databus2.core.AssertLevel;
/**
 *
 * @author snagaraj
 * Test input parameters for TestDbusEventBuffer.runConstEventsReaderWriter
 */
public class EventBufferTestInput
{

  public EventBufferTestInput() {
    _corruptionType = com.linkedin.databus.core.test.DbusEventCorrupter.EventCorruptionType.NONE;
    _corruptIndexList = new int[0];
    _consQueuePolicy = _prodQueuePolicy = QueuePolicy.OVERWRITE_ON_WRITE;
    _payloadSize = 20;
    _numEvents = 100;
    _windowSize  = 20;
    _batchSize = _numEvents/2 ;
    _sharedBufferSize = _numEvents* 5;
    _stagingBufferSize = _sharedBufferSize;
    _producerBufferSize = _sharedBufferSize;
    _individualBufferSize = _sharedBufferSize;
    _indexSize = _sharedBufferSize / 2;
    _deleteInterval = 0;
    _testName = "UnknownTest";
  }

  /**
   * @param numEvents : total number of events requested;
   * @param windowSize : event window size: #events;
   * @param payloadSize : size of data/payload in event: this controls the size of the event
   * @param sharedBufferSize : size of event buffer at the reader/consumer end : #events;
   * @param stagingBufferSize : internal readbuffer: size in #events
   * @param producerBufferSize : size of buffer at writer end size in #events;
   * @param individualBufferSize : size of contiguous buffer : #events;
   * @param batchSize : batch size requested by writer while writing to pipe; specified in terms of #events:
   * @param indexSize : size of index ; specified in terms of #events;
   * @param corruptionType : if error simulation is desired; type of error
   * @param corruptIndexList : position of events (in vector offsets) that will be tainted by the specified error type
   * @param consQueuePolicy : consumer queue policy : BLOCKING/OVERWRITE
   * @param prodQueuePolicy : producer queue policy  : BLOCKING/OVERWRITE
   * @param deleteInterval : size of deleteInterval for consumer; 0 means never delete; spec in terms of #events
   */
  public EventBufferTestInput(int numEvents,
                              int windowSize,
                              int payloadSize,
                              int sharedBufferSize,
                              int stagingBufferSize,
                              int producerBufferSize,
                              int individualBufferSize,
                              int batchSize,
                              int indexSize,
                              EventCorruptionType corruptionType,
                              int[] corruptIndexList,
                              QueuePolicy consQueuePolicy,
                              QueuePolicy prodQueuePolicy,
                              int deleteInterval)
  {
    _corruptionType = corruptionType;
    _corruptIndexList = corruptIndexList;
    _consQueuePolicy = consQueuePolicy;
    _prodQueuePolicy = prodQueuePolicy;
    _batchSize = batchSize;
    _stagingBufferSize = stagingBufferSize;
    _windowSize = windowSize;
    _sharedBufferSize = sharedBufferSize;
    _numEvents = numEvents;
    _payloadSize = payloadSize;
    _producerBufferSize = producerBufferSize;
    _individualBufferSize = individualBufferSize;
    _indexSize = indexSize;
    _deleteInterval = deleteInterval;

  }

  @Override
  public String toString() {
    //this is probably expensive; but this is a test class where flexibility + extensibility is king
    Class<? extends EventBufferTestInput> classname = this.getClass();
    StringBuilder builder = new StringBuilder();
    Field[] fields = classname.getDeclaredFields();
    for (int i=0; i < fields.length; ++i) {
        Field f = fields[i];
        try
        {
          builder.append(f.getName())
          .append("=")
          .append(f.get(this))
          .append(";");
        }
        catch (IllegalArgumentException e)
        {
          e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
          e.printStackTrace();
        }
    }
    return builder.toString();
  }

  public EventCorruptionType getCorruptionType()
  {
    return _corruptionType;
  }

  public int[] getCorruptIndexList()
  {
    return _corruptIndexList;
  }

  public int getBatchSize()
  {
    return _batchSize;
  }

  public int getStagingBufferSize()
  {
    return _stagingBufferSize;
  }

  public int getWindowSize()
  {
    return _windowSize;
  }

  public int getSharedBufferSize()
  {
    return _sharedBufferSize;
  }

  public int getNumEvents()
  {
    return _numEvents;
  }

  public int getPayloadSize()
  {
    return _payloadSize;
  }

  public int getProducerBufferSize()
  {
    return _producerBufferSize;
  }

  public int getIndividualBufferSize()
  {
    return _individualBufferSize;
  }

  public QueuePolicy getConsQueuePolicy()
  {
    return _consQueuePolicy;
  }

  public QueuePolicy getProdQueuePolicy()
  {
    return _prodQueuePolicy;
  }

  public int getIndexSize() {
    return _indexSize;
  }

  public int getDeleteInterval() {
    return _deleteInterval;
  }

  public EventBufferTestInput  setCorruptionType(EventCorruptionType corruptionType)
  {
    _corruptionType = corruptionType;
    return this;
  }

  public EventBufferTestInput setCorruptIndexList(int[] corruptIndexList)
  {
    _corruptIndexList = corruptIndexList;
    return this;
  }

  public EventBufferTestInput setConsQueuePolicy(QueuePolicy consQueuePolicy)
  {
    _consQueuePolicy = consQueuePolicy;
    return this;
  }

  public EventBufferTestInput setProdQueuePolicy(QueuePolicy prodQueuePolicy)
  {
    _prodQueuePolicy = prodQueuePolicy;
    return this;
  }

  public EventBufferTestInput setBatchSize(int batchSize)
  {
    _batchSize = batchSize;
    return this;
  }

  public EventBufferTestInput setStagingBufferSize(int stagingBufferSize)
  {
    _stagingBufferSize = stagingBufferSize;
    return this;
  }

  public EventBufferTestInput setWindowSize(int windowSize)
  {
    _windowSize = windowSize;
    return this;
  }

  public EventBufferTestInput setSharedBufferSize(int sharedBufferSize)
  {
    _sharedBufferSize = sharedBufferSize;
    return this;
  }

  public EventBufferTestInput setNumEvents(int numEvents)
  {
    _numEvents = numEvents;
    return this;
  }

  public EventBufferTestInput setPayloadSize(int payloadSize)
  {
    _payloadSize = payloadSize;
    return this;
  }

  public EventBufferTestInput setProducerBufferSize(int producerBufferSize)
  {
    _producerBufferSize = producerBufferSize;
    return this;
  }

  public EventBufferTestInput setIndividualBufferSize(int individualBufferSize)
  {
    _individualBufferSize = individualBufferSize;
    return this;
  }

  public EventBufferTestInput setIndexSize(int sz) {
    _indexSize = sz;
    return this;
  }

  public EventBufferTestInput setDeleteInterval(int deleteInterval) {
    _deleteInterval = deleteInterval;
    return this;
  }

  public AssertLevel getConsBufferAssertLevel()
  {
    return null != _consBufferAssertLevel ? _consBufferAssertLevel : AssertLevel.NONE;
  }

  public EventBufferTestInput setConsBufferAssertLevel(AssertLevel consBufferAssertLevel)
  {
    _consBufferAssertLevel = consBufferAssertLevel;
    return this;
  }

  public AssertLevel getProdBufferAssertLevel()
  {
    return null != _prodBufferAssertLevel ? _prodBufferAssertLevel : AssertLevel.ALL;
  }

  public EventBufferTestInput setProdBufferAssertLevel(AssertLevel prodBufferAssertLevel)
  {
    _prodBufferAssertLevel = prodBufferAssertLevel;
    return this;
  }

  /**
   * @return the test name
   */
  public String getTestName()
  {
    return _testName;
  }

  /**
   * @param testName  change the test name (for debugging purposes)
   */
  public EventBufferTestInput setTestName(String testName)
  {
    _testName = testName;
    return this;
  }

  private EventCorruptionType _corruptionType;
  private int[]  _corruptIndexList;
  private QueuePolicy _consQueuePolicy;
  private QueuePolicy _prodQueuePolicy;
  private AssertLevel _consBufferAssertLevel;
  private AssertLevel _prodBufferAssertLevel;
  private int _batchSize;
  private int _stagingBufferSize;
  private int _windowSize;
  private int _sharedBufferSize;
  private int _numEvents;
  private int _payloadSize;
  private int _producerBufferSize;
  private int _individualBufferSize;
  private int _indexSize;
  private int _deleteInterval;
  private String _testName;
}
