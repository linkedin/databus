package com.linkedin.databus.client.pub;

import java.util.List;

/*
 * Interface for converting Databus V2 Event Objects to V1 Event Objects
 * 
 */
public interface DatabusLegacyEventConverter<V1, V2>
{
  /*
   * Convert a V2 Event object to a V1  event Object
   */
  DatabusEventHolder<V1> convert(DatabusEventHolder<V2> event);
  
  
  /*
   * Convert a List<V2> Event objects  to a List of V1 events objects
   */
  List<DatabusEventHolder<V1>> convert(List<DatabusEventHolder<V2>> events);
}
