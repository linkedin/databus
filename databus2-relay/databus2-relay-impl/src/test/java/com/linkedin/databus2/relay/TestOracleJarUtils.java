package com.linkedin.databus2.relay;
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
