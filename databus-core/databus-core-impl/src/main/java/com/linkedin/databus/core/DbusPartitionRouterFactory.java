package com.linkedin.databus.core;
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


import java.util.HashMap;
import java.util.Map;

public class DbusPartitionRouterFactory
{
  private Map<String, DbusPartitionRouter> _routerMap = null;

  private static final DbusPartitionRouterFactory _factory = new DbusPartitionRouterFactory();
  
  public static final String CONSTANT_TYPE = "constant";
  public static final String HASH_TYPE     = "hash";
  
  private DbusPartitionRouterFactory()
  {
    _routerMap = new HashMap<String,DbusPartitionRouter>();
  }
  
  public static DbusPartitionRouterFactory getInstance()
  {
    return _factory;
  }
  
  public synchronized DbusPartitionRouter createRouter(String source, 
		  											   String partitionFunction,
		  											   int numBuckets)
    throws Exception
  {
    DbusPartitionRouter router = _routerMap.get(source);
    
    if ( null == router )
    {
      router = createNewRouter(source,partitionFunction,numBuckets);
      _routerMap.get(source);
    }
    return router;
  }
  
  private DbusPartitionRouter createNewRouter(String source, 
		  									  String partitionFunction,
		  									  int numBuckets)
    throws Exception
  {
     DbusPartitionRouter router = null;
     if (partitionFunction.startsWith(CONSTANT_TYPE))
     {
       String[] confs = partitionFunction.split(":");
       int partition = Integer.parseInt(confs[1]);
       router = new DbusConstantPartitionRouter(partition);
     } else if ( partitionFunction.startsWith(HASH_TYPE)) {
    	int index = HASH_TYPE.length();
    	String hashConf = partitionFunction.substring(index);
    	router = new DbusHashPartitionRouter(hashConf, numBuckets);
     } else {
       throw new Exception("Unknown Partition Function - " + partitionFunction);
     }
     return router;
  }
}
