package com.linkedin.databus2.producers.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
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
 * Transaction instance containing the PerSourceTxnEntries and all meta-data
 * corresponding to it.
 */
public class Transaction
{
	/**
	 * Collections of PerSourceTransactions keyed by databus source id.
	 */
	private Map<Integer, PerSourceTransaction> _perSourceTxnEntries;

	/**
	 * Size of Transaction in the source
	 */
	private long _sizeInBytes;

	/**
	 * Transaction Read time latency
	 */
	private long _txnReadLatencyNanos;

	/**
	 * Transaction's timestamp at source
	 */
	private long _txnNanoTimestamp;

	private long ignoredSourceScn = -1;


	public long getIgnoredSourceScn() {
		return ignoredSourceScn;
	}

	public void setIgnoredSourceScn(long ignoredSourceScn) {
		this.ignoredSourceScn = ignoredSourceScn;
	}


	public Transaction()
	{
		_perSourceTxnEntries = new HashMap<Integer,PerSourceTransaction>();
	}

	/**
	 * Used for incrementally building transaction object. Old DbCHange objects corresponding
	 * to a primary key will be overwritten by the new dbCHange entries.
	 *
	 * @param newPerSourceTxn New Transaction instance to merge.
	 */
	public void mergePerSourceTransaction(PerSourceTransaction newPerSourceTxn)
	{
		int srcId = newPerSourceTxn.getSrcId();
		if (_perSourceTxnEntries.containsKey(srcId))
		{
			PerSourceTransaction oldPerSourceTxn = _perSourceTxnEntries.get(srcId);

			for (DbChangeEntry dbe : newPerSourceTxn.getDbChangeEntrySet())
			{
				oldPerSourceTxn.mergeDbChangeEntrySet(dbe);
			}
		}
		else
		{
			// No such entry, just add the PerSourceTransaction for the source to map
			_perSourceTxnEntries.put(srcId, newPerSourceTxn);
		}

	}

	/**
	 * Get the Per Source Transaction object corresponding to the source-id.
	 *
	 * @param srcId Source Id
	 * @return
	 */
	public PerSourceTransaction getPerSourceTransaction(int srcId)
	{
		return _perSourceTxnEntries.get(srcId);
	}

	/**
	 * Get byte size of the transaction as seen in the source log/DB
	 *
	 * @return
	 */
	public long getSizeInBytes()
    {
      return _sizeInBytes;
    }

    public void setSizeInBytes(long _sizeInBytes)
    {
      this._sizeInBytes = _sizeInBytes;
    }

    /**
     * Get Latency for capturing this transaction
     * @return
     */
    public long getTxnReadLatencyNanos()
    {
      return _txnReadLatencyNanos;
    }

    public void setTxnReadLatencyNanos(long _txnReadLatencyNanos)
    {
      this._txnReadLatencyNanos = _txnReadLatencyNanos;
    }

    /**
     * Get Timestamp (nano) of this transaction
     * @return
     */
    public long getTxnNanoTimestamp()
    {
      return _txnNanoTimestamp;
    }

    public void setTxnNanoTimestamp(long _txnNanoTimestamp)
    {
      this._txnNanoTimestamp = _txnNanoTimestamp;
    }

    /**
	 * Returns the list of PerSourceTransactions in the order of the SourceIds.
	 * @return PerSourceTransaction list
	 */
	public List<PerSourceTransaction> getOrderedPerSourceTransactions()
	{
		List<PerSourceTransaction> txns = new ArrayList<PerSourceTransaction>(_perSourceTxnEntries.values());
		Collections.sort(txns, new Comparator<PerSourceTransaction>() {

			@Override
			public int compare(PerSourceTransaction o1, PerSourceTransaction o2) {
				return new Integer(o1.getSrcId()).compareTo(o2.getSrcId());
			}
		});
		return txns;
	}

	/**
	 * Get Max Scn among all dbChange Entries in this transaction object
	 * @return
	 */
	public long getScn()
	{
	  long maxScn = -1;

	  for (PerSourceTransaction t : _perSourceTxnEntries.values())
	  {
	    maxScn = Math.max(maxScn,t.getScn());
	  }
	  return maxScn;
	}
}
