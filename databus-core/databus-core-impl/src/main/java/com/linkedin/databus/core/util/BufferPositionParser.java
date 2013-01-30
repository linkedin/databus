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

public class BufferPositionParser
{
  public static final String MODULE = BufferPositionParser.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final int    NUM_BITS_IN_LONG    = 64;

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
   * @param buffers EventBuffer's storage array
   *
   */
  public BufferPositionParser(int individualBufferSize, int numIndices)
  {
    /* Expect the input args to be +ve */
    assert(individualBufferSize > 0);
    assert(numIndices > 0);

    LOG.info("Individual Buffer Size :" + Long.toHexString(individualBufferSize));
    LOG.info("Num Buffers :" + numIndices);

    int offsetLength = Long.toBinaryString(individualBufferSize - 1).length();
    int indexLength = Long.toBinaryString(numIndices - 1).length();

    _offsetShift = 0;
    _indexShift = offsetLength;
    _genIdShift = offsetLength + indexLength;

    long signedBitMask =  Long.MAX_VALUE;
    long signMask = ~signedBitMask;

    LOG.info("Offset Length :" + offsetLength);

    _offsetMask = ~(signMask >> (NUM_BITS_IN_LONG - offsetLength - 1)) ;

    //System.out.println("Signed Mask is :" + Long.toHexString(OffsetMask));

    _indexMask = ((~(signMask >> (NUM_BITS_IN_LONG - offsetLength - indexLength - 1))) ^ _offsetMask) ;
    //indexMask &= signedBitMask;

    _genIdMask = (Long.MAX_VALUE ^ _indexMask ^ _offsetMask);
    _totalBufferSize = individualBufferSize * numIndices;

    LOG.info("_bufferOffset for the EventBuffer :" + toString());
  }


  /*
   * @return the OffsetMask used for parsing the offset
   */
  public long getOffsetMask() {
    return _offsetMask;
  }

  /*
   * @return the index mask used for parsing the index
   */
  public long getIndexMask() {
    return _indexMask;
  }


  /*
   * @return the genId Mask used for parsing the genId
   */
  public long getGenIdMask() {
    return _genIdMask;
  }


 /*
  * @return the offset shift used for parsing the offset
  */
  public int getOffsetShift() {
    return _offsetShift;
  }

  /*
   * @return the index shift used for parsing the index
   */
  public int getIndexShift() {
    return _indexShift;
  }

  /*
   * @return the genId shift used for parsing the genId
   */
  public int getGenIdShift() {
    return _genIdShift;
  }

  /*
   * @return true if position < 0
   */
  public boolean init(long position)
  {
      return (position < 0);
  }

  /*
   * sets the offset in the position
   *
   * @param position  position where offset needs to be set
   * @param offset    offset to be set
   *
   * @return the position with offset set
   */
  public long setOffset(long position, int offset)
  {
      long newPosition = (bufferGenId(position)) << _genIdShift;
      newPosition |= ((long)bufferIndex(position)) << _indexShift;
      newPosition |= offset;
      return newPosition;
  }

  /*
   * Set the index in the Position
   *
   * @param position : Old Position
   * @param index : Index to be set
   * @return the Position with the new index
   */
  public long setIndex(long position, int index)
  {
      long newPosition = (bufferGenId(position)) << _genIdShift;
      newPosition |= ((long)index) << _indexShift;
      newPosition |= bufferOffset(position);
      return newPosition;
  }

  /*
   * Set the GenId in the position
   *
   * @param position : Old position
   * @param genId : GenId to be set
   * @return the _bufferOffset with the new genId
   */
  public long setGenId(long position, long genId)
  {
    long newPosition =   (genId) << _genIdShift;
    newPosition |= ((long)bufferIndex(position)) << _indexShift;
    newPosition |= bufferOffset(position);
    return newPosition;
  }

  /*
   * Removes the genId and returns the address part (index + offset)
   *
   * @param position Position whose address needs to be parsed
   * @return the address component of  the position
   */
  public long address(long position)
  {
    return setGenId(position,0);
  }

  /*
   * Get the bufferIndex of the position
   * @param position : position
   * @return the index encoded in the position
   */
  public int bufferIndex(long position)
  {
    long index =  ((position & _indexMask) >> _indexShift);
    return (int) index;
  }

  /*
   * Get the index in the position
   * @param position :
   * @return the offset encoded in the position
   */
  public int bufferOffset(long position)
  {
    int offset = (int) ((position & _offsetMask) >> _offsetShift);
    return offset;
  }

  /*
   * Get the genId in the position
   * @param position :
   * @return the genId encoded in the position
   */
  public long bufferGenId(long position)
  {
    long genId =  ((position & _genIdMask) >> _genIdShift);
    return genId;
  }

  /*
   * increment the GenId stored in the position by 1
   *
   * @param position position to be incremented
   * @return the incremented position
   */
  public long incrementGenId(long currentPosition)
  {
      long genId = bufferGenId(currentPosition);
      long newPosition = setGenId(currentPosition, genId + 1);
      return setOffset(setIndex(newPosition, 0), 0);
  }


  /*
   * Generates the gen-id position at the beginning of the next ByteBuffer.
   *
   * @param position position to be incremented
   * @param buffers the list of buffers in the eventBuffer which is the universe for the position
   * @return the incremented position
   */
  public long incrementIndex(long currentPosition, ByteBuffer[] buffers)
  {
    int bufferIndex = bufferIndex(currentPosition);
    int nextIndex = (bufferIndex +1) % buffers.length;

    long newPosition = currentPosition;
    if ( nextIndex <= bufferIndex )
    {
        //Wrap Around. Increment GenId
        newPosition = incrementGenId(currentPosition);
        //LOG.info("GenId incremented : OldPosition -" + toString(currentPosition) + ", NewPosition =" + toString(newPosition));
    }

    return (setOffset(
                      setIndex(newPosition, nextIndex),
                      0)
           );
  }

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
      //System.out.println("Asked to increment " + _bufferOffset.toString(currentPosition) + " by " + increment);
      int offset = bufferOffset(currentPosition);
      int bufferIndex = bufferIndex(currentPosition);
      int currentBufferLimit = buffers[bufferIndex].limit();
      int currentBufferCapacity = buffers[bufferIndex].capacity();
      //System.out.println("Offset = " + offset + " BufferIndex = " + bufferIndex + " currentBufferLimit = " + buffers.get(bufferIndex).limit() + " currentBufferCapacity = " + buffers.get(bufferIndex).capacity());

      if ( noLimit)
    	  currentBufferLimit = currentBufferCapacity;

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
      throw new RuntimeException("Error in _bufferOffset");
  }

  /*
   * @param position position to be convert to String
   * @return the descriptive version of the elements stored in the position
   *
   */
  public String toString(long position)
  {
     if (position < 0)
     {
        return "["+position+"]";
     }
     else {
        StringBuilder sb = new StringBuilder();
        sb.append(position)
        .append(":[GenId=")
        .append(bufferGenId(position))
        .append(";Index=")
        .append(bufferIndex(position))
        .append(";Offset=")
        .append(bufferOffset(position))
        .append("]");
        return sb.toString();
     }
  }

  /*
   * increment the offset stored in the position
   *
   * @param position position to be incremented
   * @param increment the increment value
   * @param buffers List of buffers which is the universe for the position
   *
   * @return the incremented position
   */
  public long incrementOffset(  long currentPosition,
                                int increment,
                                ByteBuffer[] buffers)
  {
      return incrementOffset(currentPosition, increment, buffers, false, false);
  }

  /*
   * Checks if the stored address refers to valid position in the buffer (<= limit).
   *
   * if position  == limit, index gets incremented.
   * otherwise if position > limit, throws runtime exception
   *
   * @param currentPosition the position to be sanitized
   * @param buffers  List of buffer in the eventBuffer
   * @return the position pointing to a valid location
   */
  public long sanitize(long currentPosition, ByteBuffer[] buffers)
  {
      return incrementOffset(currentPosition, 0, buffers, false, false);
  }

  /*
   * Checks if the stored address refers to valid position in the buffer (<= limit).
   *
   * If okToRegress and if position  >= limit, index gets incremented.
   * otherwise if position > limit, throws runtime exception
   *
   * @param currentPosition the position to be sanitized
   * @param buffers  List of buffer in the eventBuffer
   * @return the position pointing to a valid location
   */
  public long sanitize(long currentPosition, ByteBuffer[] buffers, boolean okToRegress)
  {
      return incrementOffset(currentPosition, 0, buffers, okToRegress, false);
  }

  @Override
  public String toString() {
	return "BufferPositionParser [_offsetMask=" + _offsetMask + ", _indexMask="
			+ _indexMask + ", _genIdMask=" + _genIdMask + ", _offsetShift="
			+ _offsetShift + ", _indexShift=" + _indexShift + ", _genIdShift="
			+ _genIdShift + ", _totalBufferSize=" + _totalBufferSize + "]";
  }

  /*
   * Asserts for the 2 conditions:
   * 1. An end position (including genId) is greater than or equal to the start (including genId).
   * 2. The difference between the end and start is not greater than the EVB space.
   */
  public void assertSpan(long start, long end, boolean isDebugEnabled)
  {
    long diff = end - start;
    double maxSpan = Math.pow(2, _genIdShift);
    StringBuilder msg = null;

    if ( (diff < 0) || (diff > maxSpan) || isDebugEnabled)
    {
      msg = new StringBuilder();
      msg.append("Assert Span: Start is :" + toString(start) + ", End :" +toString(end));
      msg.append(", Diff :" + diff + ", MaxSpan :" + maxSpan + ", totalBufferSize :" + _totalBufferSize);

      if ((diff < 0) || (diff > maxSpan))
      {
    	  LOG.fatal("Span Assertion failed :" + msg.toString());
    	  throw new RuntimeException(msg.toString());
      } else {
          LOG.debug(msg.toString());
      }
    }
  }
}
