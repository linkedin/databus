package com.linkedin.databus.client.pub;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
