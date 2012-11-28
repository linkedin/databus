package com.linkedin.databus.core;

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
