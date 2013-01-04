package com.linkedin.databus.client;

public interface DatabusServerConnection 
{
	  /* Close the Connection */
	  void close();
	  
	  /* Protocol Version */
	  int getVersion();
}
