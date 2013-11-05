package com.linkedin.databus.core.test;
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
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.util.BufferPosition;


/**
 * A helper class to access private variables of DbusEventBuffer in unit tests.
 */
public class DbusEventBufferReflector
{
  private final DbusEventBuffer _evb;
  private final BufferPosition _cwp;
  private final BufferPosition _tail;
  private final BufferPosition _head;
  private final ByteBuffer[] _buffers;
  private final DbusEventFactory _eventFactory;

  public DbusEventBufferReflector(DbusEventBuffer evb)
  throws NoSuchFieldException, IllegalAccessException
  {
    _evb = evb;
    _cwp = setBufferPositionField("_currentWritePosition");
    _tail = setBufferPositionField("_tail");
    _head = setBufferPositionField("_head");
    _buffers = setBuffers();
    _eventFactory = setEventFactory();
    validateStaticIdGenerator();
  }

  public DbusEventBuffer getDbusEventBuffer() {
    return _evb;
  }

  private void validateStaticIdGenerator() throws NoSuchFieldException
  {
    Field field = DbusEventBuffer.class.getDeclaredField("_sessionIdGenerator");
    int modifier = field.getModifiers();
    if (!Modifier.isStatic(modifier))
    {
      throw new RuntimeException("Session Id Generator in DbusEventBuffer must be static");
    }
  }

  public BufferPosition getCurrentWritePosition()
  {
    return _cwp;
  }

  public BufferPosition getHead()
  {
    return _head;
  }

  public BufferPosition getTail()
  {
    return _tail;
  }

  public ByteBuffer getBuffer(int idx)
  {
    return _buffers[idx].asReadOnlyBuffer();
  }

  public DbusEventFactory getEventFactory()
  {
    return _eventFactory;
  }

  public boolean validateBuffer()
  {
    // If the current write position and head are in different buffers, then the byte buffer
    // that has the current write position should have a limit set to capacity.
    int cwpIndex = _cwp.bufferIndex();
    if (_head.bufferIndex() != cwpIndex)
    {
      if (_buffers[cwpIndex].capacity() !=  _buffers[cwpIndex].limit())
      {
        return false;
      }
    }

    // cwp >= tail > head (unless buffer is empty, in which case (head == tail)
    if (_cwp.getPosition() < _tail.getPosition())
    {
      return false;
    }

    if (_tail.getPosition() < _head.getPosition())
    {
      return false;
    }

    if (_tail.getPosition() == _head.getPosition() && !_evb.empty())
    {
      return false;
    }

    // genID of these should not differ by more than 1.
    if (_cwp.bufferGenId() - _tail.bufferGenId() > 1)
    {
      return false;
    }

    if (_tail.bufferGenId() - _head.bufferGenId() > 1)
    {
      return false;
    }

    return true;
  }

  private ByteBuffer[] setBuffers()
  throws NoSuchFieldException, IllegalAccessException
  {
    Field field = DbusEventBuffer.class.getDeclaredField("_buffers");
    field.setAccessible(true);
    return (ByteBuffer[])field.get(_evb);
  }

  private BufferPosition setBufferPositionField(String fieldName)
  throws NoSuchFieldException, IllegalAccessException
  {
    Field field = DbusEventBuffer.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (BufferPosition)field.get(_evb);
  }

  private DbusEventFactory setEventFactory()
  throws NoSuchFieldException, IllegalAccessException
  {
    Field field = DbusEventBuffer.class.getDeclaredField("_eventFactory");
    field.setAccessible(true);
    return (DbusEventFactory)field.get(_evb);
  }
}
