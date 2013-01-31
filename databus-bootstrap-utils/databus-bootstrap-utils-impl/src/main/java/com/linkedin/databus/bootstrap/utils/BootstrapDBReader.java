
package com.linkedin.databus.bootstrap.utils;
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



import org.apache.avro.generic.GenericRecord;
import com.linkedin.databus.core.DbusEvent;


public class BootstrapDBReader
{    
    public static void main(String[] args)
      throws Exception
    {
      BootstrapTableReader.init(args);
      BootstrapTableReader reader = new BootstrapTableReader(new DumpEventHandler());
      reader.execute();
    }
    
    
    public static class DumpEventHandler
       implements BootstrapReaderEventHandler
    {
    	private String _query = null;
    	
		@Override
		public void onRecord(DbusEvent event, GenericRecord record) 
		{
	          System.out.print("Header:" + event);
	          System.out.println(", Payload:" + record);
		}

		@Override
		public void onStart(String query) 
		{
			_query = query;
		}

		@Override
		public void onEnd(int count) 
		{
	        System.out.println("Read " + count + " records by executing query :( " + _query + ")");
		}
    	
    }
}
