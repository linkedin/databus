package com.linkedin.databus.client.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;

public class PersonConsumer extends AbstractDatabusCombinedConsumer
{
  public static final Logger LOG = Logger.getLogger(PersonConsumer.class.getName());

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return processEvent(e, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                 DbusEventDecoder eventDecoder)
  {
    return processEvent(e, eventDecoder);
  }

  private ConsumerCallbackResult processEvent(DbusEvent e,
                                              DbusEventDecoder eventDecoder)
  {
    GenericRecord decodedEvent = eventDecoder.getGenericRecord(e, null);
    String firstName = (String)decodedEvent.get("firstName");
    String lastName = (String)decodedEvent.get("lastName");
    Long birthDate = (Long)decodedEvent.get("birthDate");
    String deleted = (String)decodedEvent.get("deleted");

    LOG.info("FirstName: " + firstName + " lastName: " + lastName + " birthDate: " + birthDate + " deleted: " + deleted);
    return ConsumerCallbackResult.SUCCESS;
  }

}
