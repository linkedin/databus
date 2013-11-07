package com.linkedin.databus2.schemas;
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


/**
 * Represents a logical object to uniquely identify a schema
 * Given a databusSourceName and a version, there must exist a unique schema
 * i.e., given (EspressoDB.IdNamePair,1) => unique schema
 * @author pganti
 *
 */
public class VersionedTableSchemaLookup {

	private String _databusSourceName;
	private int  _version;
	
	public VersionedTableSchemaLookup(String databusSourceName, int version) {
		super();
		_databusSourceName = databusSourceName;
		_version = version;
	}

	public String getDatabusSourceName() {
		return _databusSourceName;
	}

	public int getVersion() {
		return _version;
	}
	
	@Override
	public String toString()
	{
		return "(" + getDatabusSourceName() + ","  + getVersion() + ")";
	}

	@Override
	public boolean equals(Object o)
	{
		if (null == o || ! (o instanceof VersionedTableSchemaLookup)) return false;
		VersionedTableSchemaLookup other = (VersionedTableSchemaLookup)o;
		return _databusSourceName.equals(other.getDatabusSourceName()) && _version == other.getVersion();
	}

	@Override
	public int hashCode()
	{
		return _databusSourceName.hashCode() << 16 + _version;
	}
}
