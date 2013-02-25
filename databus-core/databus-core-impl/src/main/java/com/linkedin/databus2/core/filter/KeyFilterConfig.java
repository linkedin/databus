package com.linkedin.databus2.core.filter;
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


import java.util.ArrayList;

/**
 * @author bvaradar
 */
public class KeyFilterConfig
{
	/*
	 * IDConfig is a list of IDConfigEntry
	 */
	public static class IDConfig
	{
		public static final String PREFIX = "[";
		public static final String DELIMITER = ",";
		public static final String SUFFIX = "]";

		public ArrayList<IDConfigEntry> idConfigs;

		public IDConfig()
		{
		}

		public static IDConfig fromString(String entryStr)
		{
			if ( null == entryStr)
				return null;

			String entry = entryStr.trim();

			if ((! entry.startsWith(PREFIX)) || (! entry.endsWith(SUFFIX)))
			{
				throw new RuntimeException("IDConfig missing PREFIX/SUFFIX. Config should be of the format : [ <IDConfigEntry1>, <IDConfigEntry2> ...]");
			}

			ArrayList<IDConfigEntry> idConfigs = new ArrayList<IDConfigEntry>();

			// Remove prefix and suffix and split
			String[] vals = entry.substring(1, entry.length() - 1).split(DELIMITER);

			for (String v : vals)
			{
				idConfigs.add(IDConfigEntry.fromString(v));
			}

			IDConfig idConf = new IDConfig();
			idConf.setIdConfigs(idConfigs);
			return idConf;
		}

		/**
		 * @return the idConfigs
		 */
		public ArrayList<IDConfigEntry> getIdConfigs() {
			return idConfigs;
		}

		/**
		 * @param idConfigs the idConfigs to set
		 */
		public void setIdConfigs(ArrayList<IDConfigEntry> idConfigs) {
			this.idConfigs = idConfigs;
		}

		@Override
    public String toString()
		{
			StringBuilder str = new StringBuilder();
			str.append(PREFIX);
			boolean first = true;
			for( IDConfigEntry id : idConfigs)
			{
				if ( !first) str.append(DELIMITER);
				first = false;
				str.append(id.toString());
			}
			str.append(SUFFIX);
			return str.toString();
		}

		public boolean matches(long id)
		{
			boolean match = false;

			for( IDConfigEntry idConfig : idConfigs)
			{
				match = idConfig.matches(id);

				if ( match)
					return true;
			}

			return false;
		}
	}


	/*
	 * IDConfigEntry can be of 2 formats
	 * Single : <id>
	 * Range : <id1> - <id2>
	 */
	public static class IDConfigEntry
	{
		public static final String RANGE_DELIMITER = "-";
		public enum Type
		{
			SINGLE,
			RANGE
		};

		private long idMin;
		private long idMax;
		private Type type;

		public IDConfigEntry()
		{

		}

		public long getIdMin() {
			return idMin;
		}

		public void setIdMin(long idMin) {
			this.idMin = idMin;
		}

		public long getIdMax() {
			return idMax;
		}

		public void setIdMax(long idMax) {
			this.idMax = idMax;
		}


		public Type getType() {
			return type;
		}


		public void setType(Type type) {
			this.type = type;
		}


		public static IDConfigEntry fromString(String entryStr)
		{
			IDConfigEntry idConf = new IDConfigEntry();

			String entry = entryStr.trim();

			if ( entry.contains(RANGE_DELIMITER))
			{
				idConf.setType(Type.RANGE);
				String[] vals = entry.split(RANGE_DELIMITER);
				long v1 = Long.parseLong(vals[0]);
				long v2 = Long.parseLong(vals[1]);
				idConf.setIdMin(v1);
				idConf.setIdMax(v2);

				if (v1 > v2)
					throw new RuntimeException("IDConfigEntry is invalid. idMin is greater than idMax. Entry :" + entry);
			}  else {
				idConf.setType(Type.SINGLE);
				long val = Long.parseLong(entry);
				idConf.setIdMin(val);
				idConf.setIdMax(val);
			}
			return idConf;
		}


		@Override
    public String toString()
		{
			if (type == Type.RANGE)
				return idMin + RANGE_DELIMITER + idMax;
			else
				return "" + idMin;
		}


		public boolean matches(long id)
		{
			if (type == Type.SINGLE)
			{
				return id == idMin;
			} else {
				return ( (id >= idMin) && ( id <= idMax));
			}

		}
	}

}
