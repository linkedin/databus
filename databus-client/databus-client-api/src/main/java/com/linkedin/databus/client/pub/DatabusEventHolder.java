package com.linkedin.databus.client.pub;

import com.linkedin.databus.core.DbusEventKey;

public class DatabusEventHolder<E>
{
  private E event;
  private long scn;
  private long timestamp;
  private DbusEventKey key;
  
 public E getEvent()
 {
   return event;
 }
 
 public void setEvent(E event)
 {
   this.event = event;
 }
 
 public long getScn()
 {
   return scn;
 }
 
 public void setScn(long scn)
 {
   this.scn = scn;
 }
 
 public long getTimestamp() 
 {
	return timestamp;
 }

public void setTimestamp(long timestamp) 
{
	this.timestamp = timestamp;
}

public DbusEventKey getKey() 
{
	return key;
}

public void setKey(DbusEventKey key) 
{
	this.key = key;
}

public DatabusEventHolder(E event, long scn)
 {
   super();
   this.event = event;
   this.scn = scn;
 }
  
 public DatabusEventHolder()
 {}
}
