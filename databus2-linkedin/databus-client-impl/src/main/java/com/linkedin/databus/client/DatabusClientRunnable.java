package com.linkedin.databus.client;

public interface DatabusClientRunnable
{
    /** get a reference to the generic databus client implementation */
    public DatabusHttpClientImpl getDatabusHttpClientImpl() ;

}
