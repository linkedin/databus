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

import com.linkedin.databus.core.DatabusRuntimeException;

public class BufferPositionParser
{
  public static final String MODULE = BufferPositionParser.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final int    NUM_BITS_IN_LONG = 64;

  private final long _offsetMask;
  private final long _indexMask;
  private final long _genIdMask;
  private final int  _offsetShift;
  private final int  _indexShift;
  private final int  _genIdShift;

  private final long _totalBufferSize;

  /**
   * BufferPosition contains 3 elements
   *
   * a) Offset - the offset within a single bufferIndex
   * b) Index  - the index to the byteBuffer in the EventBuffer
   * c) GenId  - The number of rotations the eventBuffer has seen
   *
   * IndividualBufferSize determines the number of bits to be used for offset
   * numIndices determines the number of bits to be used for index lookup
   * The rest of the bits are used to store genIds
   *
   * @param individualBufferSize the number of bytes each byteBuffer can hold
   * @param numIndices the number of buffers in the EventBuffer
   *
   */
  public BufferPositionParser(int individualBufferSize, int numIndices)
  {
    /* Expect the input args to be +ve */
    assert(individualBufferSize > 0);
    assert(numIndices > 0);

    LOG.info("Individual Buffer Size: " + Long.toHexString(individualBufferSize));
    LOG.info("Num Buffers: " + numIndices);

    int offsetLength = Long.toBinaryString(individualBufferSize - 1).length();
    int indexLength = Long.toBinaryString(numIndices - 1).length();

    _offsetShift = 0;
    _indexShift = offsetLength;
    _genIdShift = offsetLength + indexLength;

    long signedBitMask =  Long.MAX_VALUE;
    long signMask = ~signedBitMask;

    LOG.info("Offset Length: " + offsetLength);

    _offsetMask = ~(signMask >> (NUM_BITS_IN_LONG - offsetLength - 1)) ;

    //System.out.println("Signed Mask is: " + Long.toHexString(_offsetMask));

    _indexMask = ((~(signMask >> (NUM_BITS_IN_LONG - offsetLength - indexLength - 1))) ^ _offsetMask) ;

    _genIdMask = (Long.MAX_VALUE ^ _indexMask ^ _offsetMask);
    _totalBufferSize = individualBufferSize * numIndices;

    LOG.info("buffer position for the EventBuffer: " + toString());
  }

  /**
   * @return the offset mask used for parsing the offset
   */
  public long getOffsetMask()
  {
    return _offsetMask;
  }

  /**
   * @return the index mask used for parsing the index
   */
  public long getIndexMask()
  {
    return _indexMask;
  }

  /**
   * @return the genId Mask used for parsing the genId
   */
  public long getGenIdMask()
  {
    return _genIdMask;
  }

 /**
  * @return the offset shift used for parsing the offset
  */
  public int getOffsetShift()
  {
    return _offsetShift;
  }

  /**
   * @return the index shift used for parsing the index
   */
  public int getIndexShift()
  {
    return _indexShift;
  }

  /**
   * @return the genId shift used for parsing the genId
   */
  public int getGenIdShift()
  {
    return _genIdShift;
  }

  /**
   * @return true if position < 0
   */
  public boolean init(long position)
  {
      return (position < 0);
  }

  public long encode(long genId, int index, int offset)
  {
    final long shiftedOffset = ((long)offset) << _offsetShift;
    if (offset < 0 || shiftedOffset > _offsetMask)
    {
      throw new DatabusRuntimeException("invalid position offset: " + offset);
    }
    final long shiftedIndex = ((long)index) << _indexShift;
    if (index < 0 || shiftedIndex > _indexMask)
    {
      throw new DatabusRuntimeException("invalid position index: " + index);
    }
    final long shiftedGenId = (genId) << _genIdShift;
    if (genId < 0 || shiftedGenId > _genIdMask)
    {
      throw new DatabusRuntimeException("invalid position gen-id: " + genId);
    }

    final long pos = shiftedGenId | shiftedIndex | shiftedOffset;
    return pos;
  }

  /**
   * Sets the offset in the position.
   *
   * @param position  position where offset needs to be set
   * @param offset    offset to be set
   * @return the position with offset set
   */
  public long setOffset(long position, int offset)
  {
    return encode(bufferGenId(position), bufferIndex(position), offset);
  }

  /**
   * Sets the index in the position.
   *
   * @param position  old position
   * @param index     index to be set
   * @return the position with the new index
   */
  public long setIndex(long position, int index)
  {
    return encode(bufferGenId(position), index, bufferOffset(position));
  }

  /**
   * Sets the GenId in the position.
   *
   * @param position  old position
   * @param genId     GenId to be set
   * @return the buffer position with the new genId
   */
  public long setGenId(long position, long genId)
  {
    return encode(genId, bufferIndex(position), bufferOffset(position));
  }

  /**
   * Removes the genId and returns the address part (index + offset).
   *
   * @param position  position whose address needs to be parsed
   * @return the address component of the position
   */
  public long address(long position)
  {
    return setGenId(position,0);
  }

  /**
   * Gets the bufferIndex of the position.
   *
   * @param position  encoded position
   * @return the index encoded in the position
   */
  public int bufferIndex(long position)
  {
    long index =  ((position & _indexMask) >> _indexShift);
    return (int) index;
  }

  /**
   * Gets the index in the position.
   *
   * @param position  encoded position
   * @return the offset encoded in the position
   */
  public int bufferOffset(long position)
  {
    int offset = (int) ((position & _offsetMask) >> _offsetShift);
    return offset;
  }

  /**
   * Gets the GenId in the position.
   *
   * @param position  encoded position
   * @return the genId encoded in the position
   */
  public long bufferGenId(long position)
  {
    long genId =  ((position & _genIdMask) >> _genIdShift);
    return genId;
  }

  /**
   * Increments the GenId stored in the position by 1 and resets the index and offset to 0.
   *
   * @param position position to be incremented
   * @return the incremented position
   */
  public long incrementGenId(long currentPosition)
  {
    return encode(bufferGenId(currentPosition) + 1, 0, 0);
  }

  /**
   * Generates the gen-id position at the beginning of the next ByteBuffer.
   *
   * @param position position to be incremented
   * @param buffers the list of buffers in the eventBuffer which is the universe for the position
   * @return the incremented position
   */
  public long incrementIndex(long currentPosition, ByteBuffer[] buffers)
  {
    final int bufferIndex = bufferIndex(currentPosition);
    final int nextIndex = (bufferIndex + 1) % buffers.length;
    final long nextGenId = (0 == nextIndex) ? bufferGenId(currentPosition) + 1 :
                           bufferGenId(currentPosition);
    return encode(nextGenId, nextIndex, 0);
  }

  /**
   * Increments the offset stored in the position.
   *
   * @param position   position to be incremented
   * @param increment  the increment value
   * @param buffers    list of buffers, which is the universe for the position
   * @return the incremented position
   */
  public long incrementOffset(long currentPosition,
                              int increment,
                              ByteBuffer[] buffers)
  {
    return incrementOffset(currentPosition, increment, buffers, false, false);
  }

  /**
   * Increments the offset stored in the position.
   *
   * @param position   position to be incremented
   * @param increment  the increment value
   * @param buffers    list of buffers, which is the universe for the position
   * @param noLimit    ignore the buffer limit and use its capacity value instead
   * @return the incremented position
   */
  public long incrementOffset(long currentPosition,
                              int increment,
                              ByteBuffer[] buffers,
                              boolean noLimit)
  {
    return incrementOffset(currentPosition, increment, buffers, false, noLimit);
  }

  private long incrementOffset(long currentPosition,
                               int increment,
                               ByteBuffer[] buffers,
                               boolean okToRegress,
                               boolean noLimit)
  {
    //System.out.println("Asked to increment " + toString(currentPosition) + " by " + increment);
    int offset = bufferOffset(currentPosition);
    int bufferIndex = bufferIndex(currentPosition);
    int currentBufferLimit = buffers[bufferIndex].limit();
    int currentBufferCapacity = buffers[bufferIndex].capacity();
    //System.out.println("Offset = " + offset + ", BufferIndex = " + bufferIndex + ", currentBufferLimit = " + currentBufferLimit + ", currentBufferCapacity = " + currentBufferCapacity);

    if (noLimit)
    {
      currentBufferLimit = currentBufferCapacity;
    }

    int proposedOffset = offset + increment;
    if (proposedOffset < currentBufferLimit)
    {
      return (currentPosition + increment); // Safe to do this because offsets are the LSB's
      // alternately
      // return setOffset(currentPosition, proposedOffset);
    }

    if (proposedOffset == currentBufferLimit)
    {
      // move to the next buffer's position 0
      return incrementIndex(currentPosition, buffers);
    }


    if (okToRegress)
    {
      return incrementIndex(currentPosition, buffers);
    }

    // proposedOffset > currentBufferLimit and not okToRegress.... weird

    LOG.error("proposedOffset " + proposedOffset + " is greater than " + currentBufferLimit + " capacity = " + currentBufferCapacity);
    LOG.error("currentPosition = " + toString(currentPosition) + " increment = " + increment);
    throw new DatabusRuntimeException("Error in _bufferOffset");  // not changing to "buffer position" since widely recognized error
  }

  /**
   * Advances the given position to point to valid data in the buffer (<= limit).
   *
   * If position == limit, index gets incremented (and possibly wrapped, i.e.,
   * genId incremented and index/offset reset).  If position > limit, throws
   * runtime exception.
   *
   * @param currentPosition  the position to be sanitized (== normalized, i.e., point
   *                         at valid data rather than at the start of invalid data)
   * @param buffers          list of bytebuffers in the eventBuffer
   * @return position pointing to valid data
   */
  public long sanitize(long currentPosition, ByteBuffer[] buffers)
  {
    return incrementOffset(currentPosition, 0, buffers, false, false);
  }

  /**
   * Advances the given position to point to valid data in the buffer (<= limit).
   *
   * If position == limit, or if okToRegress and position > limit, index gets
   # incremented (and possibly wrapped, i.e., genId incremented and index/offset
   * reset).  If !okToRegress and position > limit, throws runtime exception.
   *
   * @param currentPosition  the position to be sanitized (== normalized, i.e., point
   *                         at valid data rather than at the start of invalid data)
   * @param buffers          list of bytebuffers in the eventBuffer
   * @param okToRegress      ??
   * @return position pointing to valid data
   */
  public long sanitize(long currentPosition, ByteBuffer[] buffers, boolean okToRegress)
  {
    return incrementOffset(currentPosition, 0, buffers, okToRegress, false);
  }

  /**
   * Interpret the position in a human-readable way.
   *
   * @param position position to be converted to String
   * @return the descriptive version of the elements stored in the position
   */
  public String toString(long position)
  {
    return toString(position, null);
  }

  /**
   * Interpret the position in a human-readable way, including ByteBuffer limit/capacity.
   *
   * @param position position to be converted to String
   * @param buffers  the list of ByteBuffers composing the event buffer, which is the universe for the position
   * @return the descriptive version of the elements stored in the position
   */
  public String toString(long position, ByteBuffer[] buffers)
  {
    if (position < 0)
    {
      return "["+position+"]";
    }
    else
    {
      final int index = bufferIndex(position);
      StringBuilder sb = new StringBuilder();
      sb.append(position)
        .append(":[GenId=")
        .append(bufferGenId(position))
        .append(";Index=")
        .append(index);
      if (buffers != null && index >= 0 && index < buffers.length)  // defer any out-of-range exception to end
      {
        sb.append("(lim=")
          .append(buffers[index].limit())
          .append(",cap=")
          .append(buffers[index].capacity())
          .append(")");
      }
      sb.append(";Offset=")
        .append(bufferOffset(position))
        .append("]");
      if (buffers != null && (index < 0 || index >= buffers.length))
      {
        throw new DatabusRuntimeException("invalid position index: " + sb.toString());
      }
      return sb.toString();
    }
  }

  @Override
  public String toString()
  {
    return "BufferPositionParser [_offsetMask=" + _offsetMask + ", _indexMask=" + _indexMask +
           ", _genIdMask=" + _genIdMask + ", _offsetShift=" + _offsetShift +
           ", _indexShift=" + _indexShift + ", _genIdShift=" + _genIdShift +
           ", _totalBufferSize=" + _totalBufferSize + "]";
  }

  /**
   * Asserts for the 2 conditions:
   * 1. An end position (including genId) is greater than or equal to the start (including genId).
   * 2. The difference between the end and start is not greater than the EVB space.
   */
  public void assertSpan(long start, long end, boolean isDebugEnabled)
  {
    long diff = end - start;
    double maxSpan = Math.pow(2, _genIdShift);
    StringBuilder msg = null;

    if ((diff < 0) || (diff > maxSpan) || isDebugEnabled)
    {
      msg = new StringBuilder();
      msg.append("Assert Span: Start is: " + toString(start) + ", End: " +toString(end));
      msg.append(", Diff: " + diff + ", MaxSpan: " + maxSpan + ", totalBufferSize: " + _totalBufferSize);

      if ((diff < 0) || (diff > maxSpan))
      {
        LOG.fatal("Span Assertion failed: " + msg.toString());
        throw new RuntimeException(msg.toString());
      }
      else
      {
        LOG.debug(msg.toString());
      }
    }
  }

}
