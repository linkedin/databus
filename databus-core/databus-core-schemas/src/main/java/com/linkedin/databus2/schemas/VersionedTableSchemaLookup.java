package com.linkedin.databus2.schemas;

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
