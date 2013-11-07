package com.linkedin.databus.core.cmclient;
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


import java.text.ParseException;

import com.linkedin.databus.core.data_model.Role;

/**
 * A class encapsulating the keys received from Helix, as applicable to the relay use-case
 * Provides 
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */

public class ResourceKey 
  implements Comparable<ResourceKey>
{
	private int NUM_SUBKEYS = 4;
	
	private String _physicalSource;
	private String _physicalPartition;
	private LogicalPartitionRepresentation _logicalPartitionRepresentation;
	private Role _role;
	
	public ResourceKey(String resourceKey)
	throws ParseException
	{
		/**
		 * For example: Resource "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER" 
		 * PS : "ela4-db1-espresso.prod.linkedin.com_1521"
         * PP : "bizProfile"
		 * LP :  "p1_1"
		 * state: "MASTER"
		 */
		String[] parts = resourceKey.split(",");
		if (parts.length != NUM_SUBKEYS)
			throw new ParseException("Resource key " + resourceKey + " has " + parts.length + " parts instead of " + NUM_SUBKEYS, 0);
		_physicalSource = parts[0];
		_physicalPartition = parts[1];
		_logicalPartitionRepresentation = new LogicalPartitionRepresentation(parts[2]);		
		_role = new Role(parts[3]);		
	}

	public String getPhysicalSource() {
		return _physicalSource;
	}

	public String getPhysicalPartition() {
		return _physicalPartition;
	}

	public String getLogicalPartition() {
		return _logicalPartitionRepresentation.getLogicalPartition();
	}
	public int getLogicalPartitionNumber() {
		return _logicalPartitionRepresentation.getPartitionNum();
	}
	public int getLogicalSchemaVersion() {
		return _logicalPartitionRepresentation.getSchemaVersion();
	}

	public com.linkedin.databus.core.data_model.Role getRole() {
		return _role;
	}
	public String getRoleString() {
	  return _role.toString();
	}
	
	public boolean isMaster() {
		return _role.checkIfMaster();
	}

	public boolean isValidRole() {
		return (_role.checkIfMaster() || _role.checkIfSlave());
	}
	
	@Override
	public String toString() {
		return "ResourceKey [_physicalSource=" + _physicalSource
				+ ", _physicalPartition=" + _physicalPartition
				+ ", _logicalPartitionRepresentation="
				+ _logicalPartitionRepresentation + ", _role=" + _role
				+ "]";
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + NUM_SUBKEYS;
		result = prime
				* result
				+ ((_logicalPartitionRepresentation == null) ? 0
						: _logicalPartitionRepresentation.hashCode());
		result = prime
				* result
				+ ((_physicalPartition == null) ? 0 : _physicalPartition
						.hashCode());
		result = prime * result
				+ ((_physicalSource == null) ? 0 : _physicalSource.hashCode());
		result = prime * result + ((_role == null) ? 0 : _role.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResourceKey other = (ResourceKey) obj;
		if (NUM_SUBKEYS != other.NUM_SUBKEYS)
			return false;
		if (_logicalPartitionRepresentation == null) {
			if (other._logicalPartitionRepresentation != null)
				return false;
		} else if (!_logicalPartitionRepresentation
				.equals(other._logicalPartitionRepresentation))
			return false;
		if (_physicalPartition == null) {
			if (other._physicalPartition != null)
				return false;
		} else if (!_physicalPartition.equals(other._physicalPartition))
			return false;
		if (_physicalSource == null) {
			if (other._physicalSource != null)
				return false;
		} else if (!_physicalSource.equals(other._physicalSource))
			return false;
		if (!_role.equals(other._role))
			return false;
		return true;
	}

	@Override
	public int compareTo(ResourceKey o) 
	{
		String s1 = "" + _physicalSource + _physicalPartition + getLogicalPartition();
		String s2 = "" + o.getPhysicalSource() + o.getPhysicalPartition() + o.getLogicalPartition();
		return s1.compareTo(s2);
	}
	
	
}
