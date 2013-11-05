package com.linkedin.databus.core.util;
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


import java.nio.ByteBuffer;

import org.apache.log4j.Logger;


/**
 * "Addresses" used in EventBuffer are encoded with 3 elements:
 * <pre>
 *  1. GenId : The rotation level on which this address is relevant
 *  2. Index : The buffer in the List of ByteBuffers where the address is present
 *  3. Offset : Offset into the buffer.
 *
 *  63                      n                          m                        0
 *  -----------------------------------------------------------------------------
 *  |                       |                          |                        |
 *  |    GenId              |       Index              |  Offset                |
 *  |                       |                          |                        |
 *  -----------------------------------------------------------------------------
 * </pre>
 *
 *  In the above diagram,
 *  m bits are assigned to Offset, which implies the size of individual buffer <= 2^m;
 *  n-m bits are assigned to Index, implying the number of buffers <= 2 ^(n-m);
 *  and the rest of the bits (except the sign bit) are for GenId.
 *
 *  More info on GenId:
 *     Each time the event buffer wraps around, we increment the global genId. So,
 *     by keeping the genId encoded in the most significant bit positions, we can get
 *     the natural temporal ordering of events in the address-space.
 *
 * <pre>
 *     ================ Important NOTE  =======================
 *
 *      BufferPosition values are not resilient to event buffer restarts.  So DO NOT use
 *      the raw values directly in checkpointing and other places that are expected to have
 *      a longer lifetime than one run of the relay or client containing the event buffer.
 *
 *    =========================================================
 * </pre>
 */

public class BufferPosition
{
  public static final String MODULE = BufferPosition.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private volatile long  _address;
  private final BufferPositionParser _parser;
  private final ByteBuffer[]  _buffers;


  /**
   * @return true if the position is negative
   */
  public boolean init()
  {
    return _parser.init(_address);
  }

  /**
   * Assigns this position from another position.
   *
   * @param rhs the position from which to copy
   */
  public void copy (BufferPosition rhs )
  {
    _address = rhs.getPosition();
  }

  /**
   * Removes the genId and returns the address part (index + offset).
   *
   * @return the address (with the genId stripped)
   */
  public long getRealPosition()
  {
    return _parser.address(_address);
  }

  /**
   * @return the index encoded in the contained position
   */
  public int bufferIndex()
  {
    return _parser.bufferIndex(_address);
  }

  /**
   * @return the offset encoded in the contained position
   */
  public int bufferOffset()
  {
    return _parser.bufferOffset(_address);
  }

  /**
   * @return the genId encoded in the contained position
   */
  public long bufferGenId()
  {
    return 0 <= _address ? _parser.bufferGenId(_address) : 0;
  }

  /**
   * Sets the position (including the genId).
   *
   * @param the position to be set
   */
  public void setPosition(long address)
  {
    _address = address;
  }

  /**
   * Sets the position with a given genId, buffer index, and buffer offset.
   */
  public void setPosition(long genId, int index, int offset)
  {
    _address = _parser.encode(genId, index, offset);
  }

  /**
   * @return the position in the eventBuffer, including the genId
   */
  public long getPosition()
  {
    return _address;
  }

  /**
   * @param parser   BufferPositionParser
   * @param buffers  list of ByteBuffers in the event buffer
   */
  public BufferPosition(BufferPositionParser parser, ByteBuffer[] buffers)
  {
    _parser  = parser;
    _buffers = buffers;
  }

  /**
   * @param pos      genId position
   * @param parser   BufferPositionParser
   * @param buffers  list of ByteBuffers in the event buffer
   */
  public BufferPosition(long pos, BufferPositionParser parser, ByteBuffer[] buffers)
  {
    _parser  = parser;
    _buffers = buffers;
    setPosition(pos);
  }

  @Override
  public String toString()
  {
    return _parser.toString(_address, _buffers);
  }

  /**
   * Increments the genId in the stored address by "increment".
   *
   * @return the position with the incremented genId
   */
  public long incrementGenId()
  {
    _address = _parser.incrementGenId(_address);
    return _address;
  }

  /**
   * Moves the position to the beginning of the next ByteBuffer.
   * If we reach the end of the event buffer, the genId will be incremented by
   * one and the index and offset reset to zero.
   *
   * @return the position with the incremented index
   */
  public long incrementIndex()
  {
    _address = _parser.incrementIndex(_address, _buffers);
    return _address;
  }

  /**
   * Increments the offset by "increment".
   * If we reach the end of the current bytebuffer, the index will be
   * incremented and the offset reset to zero.
   *
   * @param the increment value
   * @return the position with the incremented offset
   */
  public long incrementOffset(int increment)
  {
    _address = _parser.incrementOffset(_address, increment, _buffers);
    return _address;
  }

  /**
   * Checks if the stored address refers to valid position in the buffer (<= limit).
   * If the address is at the limit of one buffer, the index gets incremented.
   *
   * @return position pointing to a valid location
   */
  public long sanitize()
  {
    _address = _parser.sanitize(_address, _buffers);
    return _address;
  }

  /**
   * Checks if the stored address refers to valid position in the buffer (<= limit).
   *
   * If okToRegress and position  >= limit, increments index.
   * If !okToRegress and position > limit, throws runtime exception.
   *
   * @return the position pointing to a valid location
   */
  public long sanitize(boolean okToRegress)
  {
    _address = _parser.sanitize(_address, _buffers, okToRegress);
    return _address;
  }

  @Override
  public int hashCode()
  {
    return (int)_address;
  }

  @Override
  public boolean equals(Object obj)
  {
    return (obj instanceof BufferPosition) && (_address == ((BufferPosition)obj).getPosition());
  }

  /**
   * Compares for equality between two positions.
   *
   * @param the position to check for equality
   * @return true if the underlying position is equal else false
   */
  public boolean equals(BufferPosition position)
  {
    return (_address == position.getPosition());
  }

  /**
   * Compares for equality between two positions, ignoring genId.
   *
   * @param position to check for equality
   * @return true if the underlying position (without genId) is equal else false
   */
  public boolean equalsIgnoreGenId(BufferPosition position)
  {
    return (getRealPosition() == position.getRealPosition());
  }

  /**
   * Ensures the position points to the start of some data in the ByteBuffers or reaches the end of
   * the data.
   */
  public void skipOverFreeSpace()
  {
    ByteBuffer curBuf = _buffers[bufferIndex()];
    for(int iterNum = 0;iterNum < _buffers.length && bufferOffset() >= curBuf.limit(); ++iterNum)
    {
      incrementIndex();
      curBuf = _buffers[bufferIndex()];
    }
  }

}
