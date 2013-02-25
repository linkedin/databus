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


import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder;
import com.linkedin.databus2.core.filter.KeyFilterConfigJSONFactory;
import com.linkedin.databus2.core.filter.KeyModFilterConfig;
import com.linkedin.databus2.core.filter.KeyRangeFilterConfig;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;


/**
 * @author bvaradar
 *
 * Unit Test for DbusKeyCompositeFilter
 */
public class TestDbusKeyCompositeFilter
{

    @Test
    public void testDbusKeyCompositeFilter()
      throws Exception
    {
      KeyFilterConfigHolder.Config partConf1 =  new KeyFilterConfigHolder.Config();
      partConf1.setType("MOD");
      KeyModFilterConfig.Config modConf1 = new KeyModFilterConfig.Config();
      modConf1.setNumBuckets(100);
      modConf1.setBuckets("[0,0]");
      partConf1.setMod(modConf1);

      KeyFilterConfigHolder.Config partConf2 =  new KeyFilterConfigHolder.Config();
      partConf2.setType("RANGE");
      KeyRangeFilterConfig.Config rangeConf = new KeyRangeFilterConfig.Config();
      rangeConf.setSize(100);
      rangeConf.setPartitions("[3-4,3-4]");
      partConf2.setRange(rangeConf);

      HashMap<Long, KeyFilterConfigHolder> partConfigMap = new HashMap<Long, KeyFilterConfigHolder>();
      partConfigMap.put(1L, new KeyFilterConfigHolder(partConf1.build()));
      partConfigMap.put(2L, new KeyFilterConfigHolder(partConf2.build()));

      List<Long> keys1 = new ArrayList<Long>();
      List<Long> keys2 = new ArrayList<Long>();
      List<Long> keys3 = new ArrayList<Long>();

      for (long i = 0 ; i < 1000 ;i++)
      {
          keys1.add(i);
          keys2.add(i);
          keys3.add(i);
      }

      List<DbusEvent> dbusEvents = new ArrayList<DbusEvent>();

      generateEvents(1000, (short)1, keys1, dbusEvents );
      generateEvents(1000, (short)2, keys2, dbusEvents );
      generateEvents(1000, (short)3, keys3, dbusEvents );

      List<DbusEvent> expPassedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> expFailedEvents = new ArrayList<DbusEvent>();

      System.out.println("TOTAL Events :" + dbusEvents.size());

      for (DbusEvent event :  dbusEvents)
      {
        long key = event.key();
        int srcId = event.srcId();
        long bktId = key%100;

        if ( (srcId == 1) && (bktId ==0))
            expPassedEvents.add(event);
        else if ((srcId == 2) && ( (key >= 300) && ( key < 500)))
          expPassedEvents.add(event);
        else if ( srcId == 3)
          expPassedEvents.add(event);
        else
          expFailedEvents.add(event);
      }

      DbusKeyCompositeFilter filter = new DbusKeyCompositeFilter(partConfigMap);
      filter.dedupe();
      List<DbusEvent> passedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> failedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
         if ( filter.allow(event))
         {
           passedEvents.add(event);
         } else {
           failedEvents.add(event);
         }
      }

      System.out.println("Passed Event Size :" + passedEvents.size());
      System.out.println("Failed Event Size :" + failedEvents.size());

      assertEquals("Passed Size", expPassedEvents.size(), passedEvents.size());
      assertEquals("Failed Size", expFailedEvents.size(), failedEvents.size());

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Passed Element " + i, expPassedEvents.get(i), passedEvents.get(i));
      }

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Failed Element " + i, expFailedEvents.get(i), failedEvents.get(i));
      }

      ObjectMapper objMapper = new ObjectMapper();
      String objStr = objMapper.writeValueAsString(filter.getFilterMap());

      System.out.println("CompositeKeyFilter :" + objStr);

      Map<Long, DbusKeyFilter> map2 = KeyFilterConfigJSONFactory.parseSrcIdFilterConfigMap(objStr);
      String objStr2 = objMapper.writeValueAsString(filter.getFilterMap());

      System.out.println("CompositeKeyFilter2 :" + objStr2);
      assertEquals("CompositeKeys: JSON Serialization Test", objStr, objStr2);

      //String objStr3 = "{\"filterMap\":{\"40\":{\"partitionType\":\"RANGE\",\"filters\":[{\"keyRange\":{\"start\":100,\"end\":200}},{\"keyRange\":{\"start\":300,\"end\":500}},{\"keyRange\":{\"start\":100,\"end\":200}},{\"keyRange\":{\"start\":300,\"end\":500}}]}}}";
      //DbusKeyCompositeFilter f = KeyFilterJSONFactory.parseKeyCompositeFilter(objStr3);
      //System.out.println("Deserialized Filter is :" + f);

      String objStr4 = "{\"40\":{\"partitionType\":\"RANGE\",\"filters\":[{\"keyRange\":{\"start\":100,\"end\":200}},{\"keyRange\":{\"start\":300,\"end\":500}},{\"keyRange\":{\"start\":100,\"end\":200}},{\"keyRange\":{\"start\":300,\"end\":500}}]}}}";
      Map<Long, DbusKeyFilter> map3 = KeyFilterConfigJSONFactory.parseSrcIdFilterConfigMap(objStr4);
      DbusKeyCompositeFilter f2 = new DbusKeyCompositeFilter();
      f2.setFilterMap(map3);
      System.out.println("Deserialized Filter is (before dedupe): " + f2);
      f2.dedupe();
      System.out.println("Deserialized Filter is (after dedupe): " + f2);
    }

    @Test
    public void testDbusKeyNoneFilter()
      throws Exception
    {

      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
      partConf.setType("NONE");

      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
      List<DbusEvent> dbusEvents = new ArrayList<DbusEvent>();
      List<Long> keys = new ArrayList<Long>();
      for (long i = 0 ; i < 1000 ;i++)
      {
          keys.add(i);
      }

      generateEvents(1000, (short)1, keys, dbusEvents );


      List<DbusEvent> passedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> failedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
         if ( filter.allow(event))
         {
           passedEvents.add(event);
         } else {
           failedEvents.add(event);
         }
      }

      System.out.println("Passed Event Size :" + passedEvents.size());
      System.out.println("Failed Event Size :" + failedEvents.size());

      assertEquals("Passed Size", dbusEvents.size(), passedEvents.size());
      assertEquals("Failed Size",0, failedEvents.size());

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Passed Element " + i, dbusEvents.get(i), passedEvents.get(i));
      }

      ObjectMapper objMapper = new ObjectMapper();
      String objStr = objMapper.writeValueAsString(filter);

      System.out.println("KeyFilter :" + objStr);

      DbusKeyFilter filter2 = KeyFilterConfigJSONFactory.parseDbusKeyFilter(objStr);
      String objStr2 = objMapper.writeValueAsString(filter2);

      System.out.println("KeyFilter2 :" + objStr2);
      assertEquals("KeyNoneFilter: JSON Serialization Test", objStr, objStr2);
    }
    
    @Test
    public void testDbusKeyModFilterErrors()
      throws Exception
    {
        //Error Config : MaxBucket is more than numBuckets
        boolean isException = false;
        try
        {
            KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
            partConf.setType("MOD");
            KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
            modConf.setNumBuckets(5);
            modConf.setBuckets("[0,3-9]"); //invalid config
            partConf.setMod(modConf);
            DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
        } catch (InvalidConfigException ie) {
       	 ie.printStackTrace();
       	 isException = true;
        }
        
        assertEquals("Got Exception for invalid Config (MaxBucket is more than numBuckets) ", true, isException);
       	
        //Error Case : Min Bucket is more than maxBucket
        isException = false;
        try
        {
            KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
            partConf.setType("MOD");
            KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
            modConf.setNumBuckets(50);
            modConf.setBuckets("[0,9-3]"); //invalid config
            partConf.setMod(modConf);
            DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
        } catch (InvalidConfigException ie) {
       	 ie.printStackTrace();
       	 isException = true;
        }
        assertEquals("Got Exception for invalid Config (Min Bucket is more than maxBucket) ", true, isException);

        
        //Error Case : numBuckets is negative
        isException = false;
        try
        {
            KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
            partConf.setType("MOD");
            KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
            modConf.setNumBuckets(-5);
            modConf.setBuckets("[0]"); //invalid config
            partConf.setMod(modConf);
            DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
        } catch (InvalidConfigException ie) {
       	 ie.printStackTrace();
       	 isException = true;
        }
        assertEquals("Got Exception for invalid Config numBuckets is negative) ", true, isException);    
        
        
        //Error Case : minBucket is negative
        isException = false;
        try
        {
            KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
            partConf.setType("MOD");
            KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
            modConf.setNumBuckets(50);
            modConf.setBuckets("[-5,1-3]"); //invalid config
            partConf.setMod(modConf);
            DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
        } catch (InvalidConfigException ie) {
       	 ie.printStackTrace();
       	 isException = true;
        }
        assertEquals("Got Exception for invalid Config (minBucket is negative) ", true, isException);            
    }
    
    @Test
    public void testDbusKeyModFilter()
      throws Exception
    {
      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
      partConf.setType("MOD");
      KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
      modConf.setNumBuckets(100);
      modConf.setBuckets("[0,3-4]");
      partConf.setMod(modConf);
      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));

      List<DbusEvent> dbusEvents = new ArrayList<DbusEvent>();
      List<Long> keys = new ArrayList<Long>();
      for (long i = 0 ; i < 1000 ;i++)
      {
          keys.add(i);
      }

      generateEvents(1000, (short)1, keys, dbusEvents );

      List<DbusEvent> expPassedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> expFailedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
        long bkt = event.key()%100;
        if ( (bkt == 0) || ( (bkt >= 3) && ( bkt < 5)))
          expPassedEvents.add(event);
        else
          expFailedEvents.add(event);
      }

      List<DbusEvent> passedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> failedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
         if ( filter.allow(event))
         {
           passedEvents.add(event);
         } else {
           failedEvents.add(event);
         }
      }

      System.out.println("Passed Event Size :" + passedEvents.size());
      System.out.println("Failed Event Size :" + failedEvents.size());

      assertEquals("Passed Size", expPassedEvents.size(), passedEvents.size());
      assertEquals("Failed Size", expFailedEvents.size(), failedEvents.size());

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Passed Element " + i, expPassedEvents.get(i), passedEvents.get(i));
      }

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Failed Element " + i, expFailedEvents.get(i), failedEvents.get(i));
      }

      ObjectMapper objMapper = new ObjectMapper();
      String objStr = objMapper.writeValueAsString(filter);

      System.out.println("KeyModFilter :" + objStr);

      DbusKeyFilter filter2 = KeyFilterConfigJSONFactory.parseDbusKeyFilter(objStr);
      String objStr2 = objMapper.writeValueAsString(filter2);

      System.out.println("KeyModFilter2 :" + objStr2);
      assertEquals("KeyModFilter JSON Serialization Test", objStr, objStr2);
    }

    @Test
    // Case when string keys have numeric values
    public void testDbusKeyModFilterWithStringKeys1()
      throws Exception
    {
      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
      partConf.setType("MOD");
      KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
      modConf.setNumBuckets(100);
      modConf.setBuckets("[0,3-4]");
      partConf.setMod(modConf);
      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));

      // String Keys with numeric values
      List<DbusEvent> dbusEvents = new ArrayList<DbusEvent>();
      List<String> keys = new ArrayList<String>();
      for (long i = 0 ; i < 1000 ;i++)
      {
          keys.add(new Long(i).toString());
      }

      generateStringEvents(1000, (short)1, keys, dbusEvents );

      List<DbusEvent> expPassedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> expFailedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
        long bkt = new Long(new String(event.keyBytes()))%100;
        if ( (bkt == 0) || ( (bkt >= 3) && ( bkt < 5)))
          expPassedEvents.add(event);
        else
          expFailedEvents.add(event);
      }

      List<DbusEvent> passedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> failedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
         if ( filter.allow(event))
         {
           passedEvents.add(event);
         } else {
           failedEvents.add(event);
         }
      }

      System.out.println("Passed Event Size :" + passedEvents.size());
      System.out.println("Failed Event Size :" + failedEvents.size());

      assertEquals("Passed Size", expPassedEvents.size(), passedEvents.size());
      assertEquals("Failed Size", expFailedEvents.size(), failedEvents.size());

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Passed Element " + i, expPassedEvents.get(i), passedEvents.get(i));
      }

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Failed Element " + i, expFailedEvents.get(i), failedEvents.get(i));
      }

      ObjectMapper objMapper = new ObjectMapper();
      String objStr = objMapper.writeValueAsString(filter);

      System.out.println("KeyModFilter :" + objStr);

      DbusKeyFilter filter2 = KeyFilterConfigJSONFactory.parseDbusKeyFilter(objStr);
      String objStr2 = objMapper.writeValueAsString(filter2);

      System.out.println("KeyModFilter2 :" + objStr2);
      assertEquals("KeyModFilter JSON Serialization Test", objStr, objStr2);
    }
    
    
    @Test
    // Case when string keys have non-numeric values
    public void testDbusKeyModFilterWithStringKeys2()
      throws Exception
    {
      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
      partConf.setType("MOD");
      KeyModFilterConfig.Config modConf = new KeyModFilterConfig.Config();
      modConf.setNumBuckets(100);
      modConf.setBuckets("[0,3-4]");
      partConf.setMod(modConf);
      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));

      // String Keys with numeric values
      List<DbusEvent> dbusEvents = new ArrayList<DbusEvent>();
      List<String> keys = new ArrayList<String>();
      for (long i = 0 ; i < 1000 ;i++)
      {
    	  keys.add(i + "_1000"); //contains non-numeric char
      }
      
      generateStringEvents(1000, (short)1, keys, dbusEvents );

      List<DbusEvent> expPassedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> expFailedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
        long bkt = new Long(Math.abs(new String(event.keyBytes()).hashCode()))%100;
        if ( (bkt == 0) || ( (bkt >= 3) && ( bkt < 5)))
          expPassedEvents.add(event);
        else
          expFailedEvents.add(event);
      }

      List<DbusEvent> passedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> failedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
         if ( filter.allow(event))
         {
           passedEvents.add(event);
         } else {
           failedEvents.add(event);
         }
      }

      System.out.println("Passed Event Size :" + passedEvents.size());
      System.out.println("Failed Event Size :" + failedEvents.size());

      assertEquals("Passed Size", expPassedEvents.size(), passedEvents.size());
      assertEquals("Failed Size", expFailedEvents.size(), failedEvents.size());

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Passed Element " + i, expPassedEvents.get(i), passedEvents.get(i));
      }

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Failed Element " + i, expFailedEvents.get(i), failedEvents.get(i));
      }

      ObjectMapper objMapper = new ObjectMapper();
      String objStr = objMapper.writeValueAsString(filter);

      System.out.println("KeyModFilter :" + objStr);

      DbusKeyFilter filter2 = KeyFilterConfigJSONFactory.parseDbusKeyFilter(objStr);
      String objStr2 = objMapper.writeValueAsString(filter2);

      System.out.println("KeyModFilter2 :" + objStr2);
      assertEquals("KeyModFilter JSON Serialization Test", objStr, objStr2);
    }
    
    @Test
    public void testDbusKeyRangeFilterErrors()
      throws Exception
    {
    	boolean isException = false;
    	//Error Case: Range Size is negative
    	try
    	{
    	      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
    	      partConf.setType("RANGE");
    	      KeyRangeFilterConfig.Config rangeConf = new KeyRangeFilterConfig.Config();
    	      rangeConf.setSize(-1);
    	      rangeConf.setPartitions("[0,3-4]");
    	      partConf.setRange(rangeConf);
    	      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    	} catch (InvalidConfigException ice) {
    		isException = true;
    	}
        assertEquals("Got Exception for invalid Config (Range Size is negative) ", true, isException);            

        isException = false;
    	//Error Case: min is greater than max
    	try
    	{
    	      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
    	      partConf.setType("RANGE");
    	      KeyRangeFilterConfig.Config rangeConf = new KeyRangeFilterConfig.Config();
    	      rangeConf.setSize(100);
    	      rangeConf.setPartitions("[0,5-4]");
    	      partConf.setRange(rangeConf);
    	      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    	} catch (InvalidConfigException ice) {
    		isException = true;
    	}
        assertEquals("Got Exception for invalid Config (min is greater than max) ", true, isException);   
        
        isException = false;
    	//Error Case: min is -ve
    	try
    	{
    	      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
    	      partConf.setType("RANGE");
    	      KeyRangeFilterConfig.Config rangeConf = new KeyRangeFilterConfig.Config();
    	      rangeConf.setSize(100);
    	      rangeConf.setPartitions("[-3,2-4]");
    	      partConf.setRange(rangeConf);
    	      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));
    	} catch (InvalidConfigException ice) {
    		isException = true;
    	}
        assertEquals("Got Exception for invalid Config (min is greater than max) ", true, isException);   
    }
    
    
    @Test
    public void testDbusKeyRangeFilter()
      throws Exception
    {
      KeyFilterConfigHolder.Config partConf =  new KeyFilterConfigHolder.Config();
      partConf.setType("RANGE");
      KeyRangeFilterConfig.Config rangeConf = new KeyRangeFilterConfig.Config();
      rangeConf.setSize(100);
      rangeConf.setPartitions("[0,3-4]");
      partConf.setRange(rangeConf);
      DbusKeyFilter filter = new DbusKeyFilter(new KeyFilterConfigHolder(partConf.build()));

      List<DbusEvent> dbusEvents = new ArrayList<DbusEvent>();
      List<Long> keys = new ArrayList<Long>();
      for (long i = 0 ; i < 1000 ;i++)
      {
          keys.add(i);
      }

      generateEvents(1000, (short)1, keys, dbusEvents );

      List<DbusEvent> expPassedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> expFailedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
        long key = event.key();
        if ( (key < 100) || ( (key >= 300) && ( key < 500)))
          expPassedEvents.add(event);
        else
          expFailedEvents.add(event);
      }

      List<DbusEvent> passedEvents = new ArrayList<DbusEvent>();
      List<DbusEvent> failedEvents = new ArrayList<DbusEvent>();

      for (DbusEvent event :  dbusEvents)
      {
         if ( filter.allow(event))
         {
           passedEvents.add(event);
         } else {
           failedEvents.add(event);
         }
      }

      System.out.println("Passed Event Size :" + passedEvents.size());
      System.out.println("Failed Event Size :" + failedEvents.size());

      assertEquals("Passed Size", expPassedEvents.size(), passedEvents.size());
      assertEquals("Failed Size", expFailedEvents.size(), failedEvents.size());

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Passed Element " + i, expPassedEvents.get(i), passedEvents.get(i));
      }

      for ( int i = 0; i < passedEvents.size(); i++ )
      {
        assertEquals("Failed Element " + i, expFailedEvents.get(i), failedEvents.get(i));
      }

      ObjectMapper objMapper = new ObjectMapper();
      String objStr = objMapper.writeValueAsString(filter);

      System.out.println("KeyRangeFilter :" + objStr);

      DbusKeyFilter filter2 = KeyFilterConfigJSONFactory.parseDbusKeyFilter(objStr);
      String objStr2 = objMapper.writeValueAsString(filter2);

      System.out.println("KeyRangeFilter2 :" + objStr2);
      assertEquals("KeyRangeFilter JSON Serialization Test", objStr, objStr2);

    }

	private void generateEvents(int numEvents,
								short srcId,
								List<Long> keys,
								List<DbusEvent> eventVector)
	{
		int maxEventSize = 1024;
		int payloadSize = 100;
		long windowScn = 100;
		int index = 0;
		int size = keys.size();
		try {

			for (int i=0 ; i < numEvents; ++i)
			{
				//assumption: serialized event fits in maxEventSize
				ByteBuffer buf = ByteBuffer.allocate(maxEventSize).order(DbusEventV1.byteOrder);
				DbusEventV1.serializeEvent(new DbusEventKey(keys.get((index++)%size)),
				        (short)0,
						RngUtils.randomPositiveShort(),
						System.currentTimeMillis(),
						srcId,
						RngUtils.schemaMd5,
						RngUtils.randomString(payloadSize).getBytes(),
						false,
						buf);
				DbusEventInternalWritable dbe = new DbusEventV1(buf,0);
				dbe.setSequence(windowScn);
				dbe.applyCrc();
				eventVector.add(dbe);
			}
		} catch (KeyTypeNotImplementedException e) {
			e.printStackTrace();
		} catch ( UnsupportedKeyException uske) {
			uske.printStackTrace();
		}
		return;
	}
	
	private void generateStringEvents(int numEvents,
			short srcId,
			List<String> keys,
			List<DbusEvent> eventVector)
	{
		int maxEventSize = 1024;
		int payloadSize = 100;
		long windowScn = 100;
		int index = 0;
		int size = keys.size();
		try {

			for (int i=0 ; i < numEvents; ++i)
			{
				//assumption: serialized event fits in maxEventSize
				ByteBuffer buf = ByteBuffer.allocate(maxEventSize).order(DbusEventV1.byteOrder);
				DbusEventV1.serializeEvent(new DbusEventKey(keys.get((index++)%size)),
				        (short)0,
						RngUtils.randomPositiveShort(),
						System.currentTimeMillis(),
						srcId,
						RngUtils.schemaMd5,
						RngUtils.randomString(payloadSize).getBytes(),
						false,
						buf);
				DbusEventInternalWritable dbe = new DbusEventV1(buf,0);
				dbe.setSequence(windowScn);
				dbe.applyCrc();
				eventVector.add(dbe);
			}
		} catch (KeyTypeNotImplementedException e) {
			e.printStackTrace();
		}
		return;
	}
	
}
