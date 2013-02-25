package com.linkedin.databus.client.pub;
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


import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;

import org.testng.annotations.Test;

public class TestDatabusServerCoordinates {

	@Test
	public void testEqualDatabusServerCoordinates()
	{
		InetSocketAddress ia1 = new InetSocketAddress("localhost", 9001);
		DatabusServerCoordinates rc1 = new DatabusServerCoordinates("DatabusServerCoordinates1", ia1, "ONLINE");
		DatabusServerCoordinates rc2 = new DatabusServerCoordinates("DatabusServerCoordinates1", ia1, "ONLINE");
		DatabusServerCoordinates rc3 = new DatabusServerCoordinates("DatabusServerCoordinates2", ia1, "ONLINE");		

		assertEquals(rc1.hashCode(), rc2.hashCode());
		assertEquals(rc1.hashCode(), rc3.hashCode());
		assertEquals(rc2.hashCode(), rc3.hashCode());
		
		assertEquals(rc1.equals(rc2), true);
		assertEquals(rc1.equals(rc3), true);
		assertEquals(rc2.equals(rc3), true);		
	}

	@Test
	public void testUnequalDatabusServerCoordinates()
	{
		InetSocketAddress ia1 = new InetSocketAddress("localhost", 9001);
		InetSocketAddress ia2 = new InetSocketAddress("localhost", 9002);

		DatabusServerCoordinates rc1 = new DatabusServerCoordinates("DatabusServerCoordinates1", ia1, "ONLINE");
		DatabusServerCoordinates rc2 = new DatabusServerCoordinates("DatabusServerCoordinates1", ia2, "ONLINE");
		
		assertEquals(rc1.equals(rc2), false);
	}


	@Test
	public void testUnequalDatabusServerCoordinates2()
	{
		InetSocketAddress ia1 = new InetSocketAddress("localhost", 9001);

		DatabusServerCoordinates rc1 = new DatabusServerCoordinates("DatabusServerCoordinates1", ia1, "ONLINE");
		DatabusServerCoordinates rc2 = new DatabusServerCoordinates("DatabusServerCoordinates1", ia1, "OFFLINE");
		assertEquals(rc1.equals(rc2), false);
	}

}
