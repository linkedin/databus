package com.linkedin.databus2.core.seq;

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
