package com.linkedin.databus.client.pub;

import java.util.List;

/*
 * Application call-back interface for processing Databus V1 event objects
 */
public interface DatabusLegacyConsumerCallback<V1>
{
    /* Process one V1 data Event */
    ConsumerCallbackResult onEvent(DatabusEventHolder<V1> event);
  
    /* Process a list of V1 data Events */
    ConsumerCallbackResult onEventsBatch(List<DatabusEventHolder<V1>> events);
    
    /* Process one V1 bootstrap Event */
    ConsumerCallbackResult onBootstrapEvent(DatabusEventHolder<V1> event);
    
    /* Process a list of V1 bootstrap Events */
    ConsumerCallbackResult onBootstrapEventsBatch(List<DatabusEventHolder<V1>> events);
}
