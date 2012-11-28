package com.linkedin.databus.core.data_model;

/*
 * Physical Source Role
 */
public class Role
{
	  public static final int UNKNOWN = 0;
	  public static final int MASTER = 1;
	  public static final int SLAVE = 2;
	  public static final int OFFLINE =3;
	  public static final int ERROR = 4;
	  public static final int ANY = 5;

	  public static final String UNKNOWN_STR = "UNKNOWN";
	  public static final String MASTER_STR = "MASTER";
	  public static final String SLAVE_STR = "SLAVE";
	  public static final String OFFLINE_STR = "OFFLINE";
	  public static final String ERROR_STR = "ERROR";
	  public static final String ANY_STR = "ANY";

	  
	  final int _roleVal;
	  final String _role;
	  
	  public Role(int role)
	  {
		  _roleVal = role;
		  _role = toString();
	  }
	  
	  public Role(String roleStr)
	  {
		  _roleVal = getRoleVal(roleStr);
		  _role = toString();
	  }

	  private int getRoleVal(String roleStr)
	  {
		  if (roleStr.equals(MASTER_STR))
		  	return MASTER;
		  else if (roleStr.equals(SLAVE_STR))
			return SLAVE;
		  else if (roleStr.equals(OFFLINE_STR))
			return OFFLINE;
		  else if (roleStr.equals(ERROR_STR))
			  return ERROR;
		  else if (roleStr.equals(ANY_STR))
			  return ANY;
		  else
			  return UNKNOWN;
	  }			

	  public String getRole()
	  {
		  return _role;
	  }

	  private int getRoleVal()
	  {
		  return _roleVal;
	  }
	  
	  public boolean checkIfMaster()
	  {
		  return _roleVal == MASTER;
	  }

	  public boolean checkIfSlave()
	  {
		  return _roleVal == SLAVE;
	  }

	  public boolean checkIfOffline()
	  {
		  return _roleVal == OFFLINE;
	  }

	  public boolean checkIfAny()
	  {
		  return _roleVal == ANY;
	  }

	  public boolean checkIfError()
	  {
		  return _roleVal == ERROR;
	  }

	  public boolean checkIfUnknown()
	  {
		  return _roleVal == UNKNOWN;
	  }

	  @Override
	  public int hashCode()
	  {
		  return _roleVal;
	  }
	  
	  @Override
	  public boolean equals(Object other)
	  {
	    if (null == other || !(other instanceof Role)) return false;
	    Role otherRole = (Role) other;
	    if (_roleVal == otherRole.getRoleVal())
	    	return true;
	    if (_roleVal == ANY || otherRole.getRoleVal() == ANY)
	    	return true;
	    return false;

	  }

	  @Override
	  public String toString()
	  {
		  if (getRoleVal() == MASTER)
			return MASTER_STR;
		  else if (getRoleVal() == SLAVE)
			return SLAVE_STR;
		  else if (getRoleVal() == OFFLINE)
			return OFFLINE_STR;
		  else if (getRoleVal() == ANY)
			return ANY_STR;
		  else if (getRoleVal() == ERROR)
			return ERROR_STR;
		  else
			return UNKNOWN_STR;
	  }
	  
	  public static Role createUnknown()
	  {
		  Role r = new Role(UNKNOWN);
		  return r;
	  }
};

