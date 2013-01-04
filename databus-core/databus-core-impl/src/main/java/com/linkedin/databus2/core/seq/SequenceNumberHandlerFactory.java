package com.linkedin.databus2.core.seq;

import com.linkedin.databus2.core.DatabusException;

/** A factory for creating handlers for persisting sequence numbers */
public interface SequenceNumberHandlerFactory
{
  /** Creates a new handler with the specified id */
  MaxSCNReaderWriter createHandler(String id) throws DatabusException;
}
