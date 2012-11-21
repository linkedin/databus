package com.linkedin.databus2.core.seq;

import com.linkedin.databus2.core.DatabusException;

/** Factory for in-memory sequence number handlers */
public class InMemorySequenceNumberHandlerFactory implements SequenceNumberHandlerFactory
{
  private final long _initSeqNumber;

  public InMemorySequenceNumberHandlerFactory(long initSeqNumber)
  {
    _initSeqNumber = initSeqNumber;
  }

  @Override
  public MaxSCNReaderWriter createHandler(String id) throws DatabusException
  {
    return new InMemorySequenceNumberHandler(_initSeqNumber);
  }
}
