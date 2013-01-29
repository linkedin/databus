package com.linkedin.databus2.relay;

import javax.sql.DataSource;

import org.testng.Assert;

import org.testng.annotations.Test;

public class TestOracleJarUtils {

	@Test
	public void testLoadClass() throws Exception
	{
		// Invoke the method more than once in the same process
     	Class oracleDataSourceClass1 = OracleJarUtils.loadClass("oracle.jdbc.pool.OracleDataSource");
    	Class oracleDataSourceClass2 = OracleJarUtils.loadClass("oracle.jdbc.pool.OracleDataSource");
    	
    	// Both the classes returned should be the same
    	boolean isEqual = oracleDataSourceClass1.equals(oracleDataSourceClass2);
    	Assert.assertEquals(isEqual, true);

    	boolean isIdentical = (oracleDataSourceClass1 == oracleDataSourceClass2);
    	Assert.assertEquals(isIdentical, true);

	}

	@Test
	public void testCreateOracleDataSource() throws Exception
	{
		// Invoke the method more than once in the same process
     	DataSource ds1 = OracleJarUtils.createOracleDataSource("jdbc:oracle:thin:person/person@devdb:1521:db");
     	DataSource ds2 = OracleJarUtils.createOracleDataSource("jdbc:oracle:thin:person/person@devdb:1521:db");
     	
    	// Should create a new object each time, and be invocable as many times as desired
    	boolean isEqual = ds1.equals(ds2);
    	Assert.assertEquals(isEqual, false);

    	boolean isIdentical = (ds1 == ds2);
    	Assert.assertEquals(isIdentical, false);

	}

}
