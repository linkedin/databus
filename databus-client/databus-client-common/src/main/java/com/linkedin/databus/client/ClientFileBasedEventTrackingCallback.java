package com.linkedin.databus.client;

import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.FileBasedEventTrackingCallback;

/**
 * Support dumping of event value
 * @author dzhang
 *
 */
public class ClientFileBasedEventTrackingCallback extends FileBasedEventTrackingCallback
{
  public ClientFileBasedEventTrackingCallback(String filename, boolean append)
  {
    this(filename, append, 0);
  }

  public ClientFileBasedEventTrackingCallback(String filename, boolean append, int numEventsPerBatch)
  {
    super(filename, append, numEventsPerBatch);
  }

  public void dumpEventValue(DbusEvent event, DbusEventDecoder eventDecoder)
  {
    if (!event.isEndOfPeriodMarker())
    {
      eventDecoder.dumpEventValueInJSON(event, _writeChannel);
    }
  }

}


