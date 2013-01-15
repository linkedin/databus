package com.linkedin.databus.core.util;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;


/**
 * "Addresses" used in EventBuffer is encoded with 3 elements
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
 *
 *  In the above diagram
 *  m bits are assigned to offset which implies the size of individual buffer <= 2^m
 *  n-m bits are assigned to index implying the number of buffers <= 2 ^(n-m)
 *  the rest of the bits (except the sign bit) is for genId
 *
 *  More info on GenId:
 *     Each time the event-buffer wraps around, we increment the global genId. So,
 *     by keeping the genId encoded in the most significant bit positions, we can get
 *     the natural temporal ordering of events in the address-space.
 *
 *     ================ Important NOTE  =======================
 *
 *      The bufferPosition values are not resilient to Buffer restarts. So, DO NOT use the
 *      raw values directly in check-pointing and other places which is expected to have a longer
 *      lifetime than one run of Relay/Client containing the eventbuffer
 *
 *    =========================================================
 */

public class BufferPosition
{
  public static final String MODULE = BufferPosition.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private volatile long  _address;
  private final BufferPositionParser _parser;
  private final ByteBuffer[]  _buffers;


  /*
   * @return true if the position is -ve
   *
   */
  public boolean init()
  {
    return _parser.init(_address);
  }

  /*
   * Assigns this position from another position
   *
   * @param rhs the position to copy from
   */
  public void copy (BufferPosition rhs )
  {
    _address = rhs.getPosition();
  }

  /*
   * Removes the genId and returns the address part (index + offset)
   *
   * @return the address (with the genId stripped)
   */
  public long getRealPosition()
  {
    return _parser.address(_address);
  }


  /*
   * @return the index encoded in the contained position
   *
   */
  public int bufferIndex()
  {
    return _parser.bufferIndex(_address);
  }

  /*
   * @return the offset encoded in the contained position
   */
  public int bufferOffset()
  {
    return _parser.bufferOffset(_address);
  }

  /*
   * @return the genId encoded in the contained position
   */
  public long bufferGenId()
  {
    return 0 <= _address ? _parser.bufferGenId(_address) : 0;
  }

  /*
   * Sets the Position (including GenId)
   *
   * @param the position to be set
   */
  public void setPosition(long address)
  {
    _address = address;
  }

  /*
   * @return the position in the eventBuffer including the genId
   */
  public long getPosition()
  {
    return _address;
  }

  /*
   * @param parser  : BufferPositionParser
   * @param buffers : List of byteBuffers in the EventBuffer
   */
  public BufferPosition(BufferPositionParser parser, ByteBuffer[] buffers)
  {
    _parser  = parser;
    _buffers = buffers;
  }

  @Override
  public String toString() {
    return _parser.toString(_address);
  }

  /*
   * Increment the GenId in the stored address by "increment"
   *
   * @return the position containing the incrementedGenId
   */
  public long incrementGenId()
  {
      _address = _parser.incrementGenId(_address);
      return _address;
  }

  /*
   * Moves the position to the beginning of the next ByteBuffer.
   * If we reach the end of the bufferIndex, the genId will be incremented by 1 and index/offset 
   * reset.
   *
   * @return the position with the incremented index
   */
  public long incrementIndex()
  {
    _address = _parser.incrementIndex(_address, _buffers);
    return _address;
  }

  /*
   * Increment the offset by "increment".
   * If reached the end of the current buffer, this method will increment the index and offset reset.
   *
   * @param the increment value
   * @return the position with the incremented offset
   */
  public long incrementOffset(int increment)
  {
      _address = _parser.incrementOffset(_address, increment, _buffers);
      return _address;
  }

  /*
   * Checks if the stored address refers to valid position in the buffer (<= limit).
   * If the address is at the limit of one buffer, index gets incremented.
   *
   * @return the position pointing to a valid location
   */
  public long sanitize()
  {
      _address = _parser.sanitize(_address, _buffers);
      return _address;
  }

  /*
   * Checks if the stored address refers to valid position in the buffer (<= limit).
   *
   * If okToRegress, then if position  >= limit, index gets incremented.
   * otherwise if position > limit, throws runtime exception
   *
   * @return the position pointing to a valid location
   */
  public long sanitize(boolean okToRegress)
  {
    _address = _parser.sanitize(_address, _buffers, okToRegress);
    return _address;
  }

  @Override
  public boolean equals(Object obj)
  {
	  if ( obj instanceof BufferPosition)
	  {
		  if ( _address == ((BufferPosition)obj).getPosition())
			  return true;
	  }
	  return false;
  }

  @Override
  public int hashCode()
  {
    return (int)_address;
  }

  /*
   * Compares for equality between 2 positions
   *
   * @param the position to check for equality
   * @return true if the underlying position is equal else false
   */
  public boolean equals(BufferPosition position)
  {
	  return (_address == position.getPosition());
  }

  /*
   * Compares the equality ignoring genId
   *
   * @param position to check for equality
   *
   * @return true if the underlying position (without genId) is equal else false
   */
  public boolean equalsIgnoreGenId(BufferPosition position)
  {
	  return (getRealPosition() == position.getRealPosition());
  }

}
