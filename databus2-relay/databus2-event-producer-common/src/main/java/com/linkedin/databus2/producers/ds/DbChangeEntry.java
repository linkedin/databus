package com.linkedin.databus2.producers.ds;

import com.linkedin.databus2.schemas.SchemaId;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.core.DbusOpcode;

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
 * Instance containing the avro record and related meta-data corresponding to a change entry seen from the Source
 * The field PKey is used as identifying column (hashcode/equals) for a DBChange Entry
 */
public class DbChangeEntry {

	/**
	 * SCN for this change entry
	 */
	private final long _scn;

	/**
	 * Timestamp in nanos for this change entry
	 */
	private final long _timestampInNanos;

	/**
	 * The change event in Avro record
	 */
	private final GenericRecord _record;

	/**
	 * OpCode of this change entry
	 */
	private final DbusOpcode _opCode;

	/**
	 * Flag to identify if the event is replicated across Colo or generated locally
	 */
	private final boolean _isReplicated;

	/**
	 * Avro schema used to generate the generic record.
	 */
	private final Schema _schema;

	/**
	 * Avro schema md5 id.
	 */
	private final SchemaId _schemaId;


	/**
	 * Primary Key(s) corresponding to the entry
	 */
	private final List<KeyPair> _pkeys;

	public long getScn() {
		return _scn;
	}

	public GenericRecord getRecord() {
		return _record;
	}

	public DbusOpcode getOpCode() {
		return _opCode;
	}

	public boolean isReplicated() {
		return _isReplicated;
	}

	public Schema getSchema() {
		return _schema;
	}

	public SchemaId getSchemaId() {
		return _schemaId;
	}

	public List<KeyPair> getPkeys() {
		return _pkeys;
	}

	public long getTimestampInNanos() {
		return _timestampInNanos;
	}

	@Override
	/**
	 * DBChange Entry is keyed by the Pkey in containers.
	 */
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_pkeys == null) ? 0 : _pkeys.hashCode());
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
		DbChangeEntry other = (DbChangeEntry) obj;
		if (_pkeys == null) {
			if (other._pkeys != null)
				return false;
		} else if (!_pkeys.equals(other._pkeys))
			return false;
		return true;
	}

	public DbChangeEntry(long scn, long timestampNanos, GenericRecord record, DbusOpcode opCode,
			boolean isReplicated, Schema schema, SchemaId schemaId, List<KeyPair> pkeys) {
		super();
		this._scn = scn;
		this._timestampInNanos = timestampNanos;
		this._record = record;
		this._opCode = opCode;
		this._isReplicated = isReplicated;
		this._schema = schema;
		this._schemaId = schemaId;
		this._pkeys = pkeys;
	}
}
