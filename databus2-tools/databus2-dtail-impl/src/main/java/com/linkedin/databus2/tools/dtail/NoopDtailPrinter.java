package com.linkedin.databus2.tools.dtail;

import java.io.OutputStream;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;

public class NoopDtailPrinter extends DtailPrinter
{

  public NoopDtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out)
  {
    super(client, conf, out);
  }

  @Override
  public ConsumerCallbackResult printEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

}
