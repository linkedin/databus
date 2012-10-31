package com.linkedin.databus.client.pub;


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
