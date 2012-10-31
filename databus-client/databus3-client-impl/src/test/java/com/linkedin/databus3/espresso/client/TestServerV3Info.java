package com.linkedin.databus3.espresso.client;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.linkedin.databus.client.pub.ServerV3Info;
import com.linkedin.databus.client.pub.DatabusServerCoordinates.StateId;
import com.linkedin.databus.core.cmclient.ResourceKey;

public class TestServerV3Info {

	@Test
	public void testServerInfoCreation()
	throws Exception
	{
		String name = "testEspressoRelay";
		int port = 10001;
		InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), port);
		new ServerV3Info( name, StateId.ONLINE.name(), addr);
	}
	
	@Test
	public void testServerInfoResourceKeyAdd()
	throws Exception
	{
		String name = "testEspressoRelay";
		int port = 10001;
		InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), port);
		ServerV3Info svi = new ServerV3Info(name, StateId.ONLINE.name(), addr);
		
		ResourceKey rk = new ResourceKey("ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER");
		svi.addResourceKey(rk);
		
		AssertJUnit.assertSame(svi.getResourceKeys().get(0), rk);
	}

}
