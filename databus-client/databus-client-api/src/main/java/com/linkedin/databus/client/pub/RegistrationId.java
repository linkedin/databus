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



public class RegistrationId {
	private String _id;
	
	public RegistrationId( String id )
	{
		/**
		 * Cannot pass in null for an empty string for an id
		 */
		if (id == null || id.isEmpty())
		{
			throw new IllegalArgumentException();
		}
		_id = id;
		return;
	}
	
	public String getId()
	{
		return _id;
	}
	
	public void setId(String id)
	{
		_id = id;
	}
	
	@Override
	public String toString()
	{
		return _id;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) 
			return true;
		if ( !(obj instanceof RegistrationId) )
			return false;
		RegistrationId rid = (RegistrationId) obj;
		return _id.equals(rid.getId());

	}
	
	@Override
	public int hashCode()
	{
		return _id.hashCode();
	}
	
}
