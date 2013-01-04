package com.linkedin.databus.client.pub;

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
