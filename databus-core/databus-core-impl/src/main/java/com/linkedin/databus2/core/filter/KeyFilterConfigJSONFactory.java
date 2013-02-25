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


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.databus2.core.filter.KeyFilterConfigHolder.PartitionType;



/*
 *Factory to create FilterConfigHolder objects from a JSON String
 *
 *The version of JSON used currently does not support automatic parsing of polymorphic classes.
 *So, their instantiation has to be done by hand in the code below.
 *
 */
public class KeyFilterConfigJSONFactory
{
	public static final String MODULE = KeyFilterConfigJSONFactory.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private static final String partitionTypeFieldName = "partitionType";
	private static final String filterConfigFieldName = "filters";

	/*
	 * Converts a JSON String to DbusKeyFilter object
	 *
	 * @param String containing JSON representation of DbusKeyFilter object
	 * @return DbusKeyFilter object
	 *
	 * @throws
	 *   JSONException if JSON Parser is unable to parse the data
	 *   IOException if there is any IO errors while parsing
	 */
	public static DbusKeyFilter parseDbusKeyFilter(String s)
	  throws JSONException, IOException
	{
	  JSONObject obj = new JSONObject(s);
	  ObjectMapper mapper = new ObjectMapper();

	  return getDbusKeyFilter(obj, mapper);
	}

	/*
	 * Converts a generic JSONObject to DbusKeyFilter object
	 *
	 * @param1 generic JSONObject of the DbusKeyFilter
	 * @param2 JSON ObjectMapper used to parse the JSONObject
	 * @return DbusKeyFilter object
	 *
	 * @throws
	 *   JSONException if JSON Parser is unable to parse the data
	 *   IOException if there is any IO errors while parsing
	 */
	private static DbusKeyFilter getDbusKeyFilter(JSONObject obj, ObjectMapper mapper)
	  throws JSONException, IOException
	{
		  String type = obj.getString(partitionTypeFieldName);
		  JSONArray array = null;

		  DbusKeyFilter configHolder = new DbusKeyFilter();
		  configHolder.setPartitionType(PartitionType.valueOf(type));

		  if (! obj.has(filterConfigFieldName) || obj.isNull(filterConfigFieldName))
		  {
			  return configHolder;
		  }

		  array = obj.getJSONArray(filterConfigFieldName);

		  ArrayList<DbusFilter> filters = new ArrayList<DbusFilter>();

		  for ( int i = 0; i < array.length(); i++)
		  {
			  filters.add(getDbusFilter(array.getString(i), configHolder.getPartitionType(), mapper));
		  }

		  configHolder.setFilters(filters);
		  return configHolder;
	}


	/*
	 * Converts a JSON String to DbusFilter object
	 *
	 * @param1 String containing JSON representation of DbusKeyFilter object
	 * @param2 PartitionType of the Filter
	 * @param3 JSON ObjectMapper used to parse the JSONObject
	 * @return DbusKeyFilter object
	 *
	 * @throws
	 *   JSONException if JSON Parser is unable to parse the data
	 *   IOException if there is any IO errors while parsing
	 */
	private static DbusFilter getDbusFilter(String objStr, PartitionType type, ObjectMapper objMapper)
	  throws JSONException, IOException
	{
		DbusFilter filter = null;

		if ( type == PartitionType.MOD )
		{
			filter = objMapper.readValue(objStr, KeyModFilter.class);
		} else if ( type == PartitionType.RANGE ) {
			filter = objMapper.readValue(objStr, KeyRangeFilter.class);
		}

		return filter;
	}

	/*
	 * Converts a JSON String to  a map of SourceIds to DbusKeyFilter
	 *
	 * @param String containing JSON representation of DbusKeyFilter object
	 * @return Map of SourceIds to DbusKeyFilter
	 *
	 * @throws
	 *   JSONException if JSON Parser is unable to parse the data
	 *   IOException if there is any IO errors while parsing
	 */
	@SuppressWarnings("unchecked")
  public static Map<Long, DbusKeyFilter> parseSrcIdFilterConfigMap(String s)
	  throws JSONException, IOException
	{
		HashMap<Long,DbusKeyFilter> filterConfigMap = new HashMap<Long,DbusKeyFilter>();

		HashMap<Long, JSONObject> genericMap = new HashMap<Long,JSONObject>();

		ObjectMapper mapper = new ObjectMapper();

	    try
	    {
	    	JSONObject obj2 = new JSONObject(s);
	    	Iterator<String> itr = obj2.keys();
	    	while (itr.hasNext())
	    	{
	    		String k = itr.next();
	    		Long key = Long.valueOf(k);
	    		genericMap.put(key, obj2.getJSONObject(key.toString()));
	    	}
	    } catch (Exception ex) {
	    	LOG.error("Got exception while parsing filterConfig", ex);
	    	throw new JSONException(ex);
	    }

	    Iterator<Entry<Long, JSONObject>> itr = genericMap.entrySet().iterator();

	    while ( itr.hasNext())
	    {
	    	Entry<Long, JSONObject> obj = itr.next();

	    	filterConfigMap.put(obj.getKey(), getDbusKeyFilter(obj.getValue(),mapper));
	    }

		return filterConfigMap;
	}

}
