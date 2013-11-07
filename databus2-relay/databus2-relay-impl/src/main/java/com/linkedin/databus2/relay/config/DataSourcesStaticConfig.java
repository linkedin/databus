package com.linkedin.databus2.relay.config;

import com.linkedin.databus2.core.seq.MaxSCNReaderWriterStaticConfig;

/** Static configuration for a collection of data sources*/
public class DataSourcesStaticConfig
{
  private final MaxSCNReaderWriterStaticConfig _sequenceNumbersHandler;

  public DataSourcesStaticConfig(MaxSCNReaderWriterStaticConfig sequenceNumbersHandler)
  {
    super();
    _sequenceNumbersHandler = sequenceNumbersHandler;
  }

  /**
   * The static configuration for the handler responsible for persisting the sequence
   * numbers for different sources */
  public MaxSCNReaderWriterStaticConfig getSequenceNumbersHandler()
  {
    return _sequenceNumbersHandler;
  }

}
