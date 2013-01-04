package com.linkedin.databus.bootstrap.utils;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.core.DbusEvent;

public interface BootstrapReaderEventHandler {

	public void onStart(String query);
	
	public void onRecord(DbusEvent event, GenericRecord record);
	
	public void onEnd(int count);
		
}
