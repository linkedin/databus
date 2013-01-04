
package com.linkedin.databus.bootstrap.utils;


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
