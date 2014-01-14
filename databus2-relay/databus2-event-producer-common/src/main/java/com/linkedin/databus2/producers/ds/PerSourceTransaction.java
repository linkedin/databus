package com.linkedin.databus2.producers.ds;

import java.util.HashSet;
import java.util.Set;

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
 *
 * Instance containing the set of avro records corresponding to one logical source (table/view)
 * in a single batch/transaction
 */
public class PerSourceTransaction
{
	/**
	 * Databus Source Id of the Source
	 */
	private final int _srcId;

	/**
	 * DbChangeEntry set for this source in the current txn. The changeEntry is keyed by the pKeys.
	 * So, A given key appears only once in the PerSourceTransactionObject
	 */
	private Set<DbChangeEntry> _dbChangeEntrySet;

	public PerSourceTransaction(int srcId, Set<DbChangeEntry> dbChangeEntrySet)
	{
		this._srcId = srcId;
		this._dbChangeEntrySet = dbChangeEntrySet;

		if ( null == _dbChangeEntrySet)
			_dbChangeEntrySet = new HashSet<DbChangeEntry>();
	}

	public PerSourceTransaction(int srcId) {
		this(srcId, null);
	}

	public Set<DbChangeEntry> getDbChangeEntrySet() {
		return _dbChangeEntrySet;
	}

	/**
	 * Add a DB changeEntry to the PerSourceTransaction.
	 * If an entry exist for the same key(s), it will be overwritten with the passed value
	 *
	 * @param dbChangeEntry Change Entry to be added
	 */
	public void mergeDbChangeEntrySet(DbChangeEntry dbChangeEntry)
	{
		if ( _dbChangeEntrySet.contains(dbChangeEntry))
		{
			_dbChangeEntrySet.remove(dbChangeEntry);
		}

		_dbChangeEntrySet.add(dbChangeEntry);
	}

	/**
	 * Get source id of the source for which this perSourceTxn object corresponds to
	 * @return sourceId
	 */
	public int getSrcId() {
		return _srcId;
	}

	/**
	 *
	 * Get the max Scn among all dbChangeEntry set
	 * @return
	 */
	public long getScn()
	{
	  long maxScn = -1;

	  for (DbChangeEntry c : _dbChangeEntrySet)
	  {
	    maxScn = Math.max(maxScn,c.getScn());
	  }
	  return maxScn;
	}
}
