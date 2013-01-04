package com.linkedin.databus.client.pub;


import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.DatabusServerCoordinates.StateId;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoSetBuilder;

public class TestServerInfo {

	@Test
	public void testSupportsSources()
	{
		ServerInfo serverInfo  = new ServerInfo("test", StateId.ONLINE.name(), new InetSocketAddress("localhost", 9000),
				"source1", "source2", "source3", "source4");

		assertEquals(4, serverInfo.getSources().size());
		assertEquals(true, serverInfo.supportsSources(Arrays.asList("source1")));
		assertEquals(true, serverInfo.supportsSources(Arrays.asList("source3")));
		assertEquals(true, serverInfo.supportsSources(Arrays.asList("source2", "source4")));
		assertEquals(false, serverInfo.supportsSources(Arrays.asList("source5")));
		assertEquals(false, serverInfo.supportsSources(Arrays.asList("source3", "source1")));
	}

	@Test
	public void testToJsonString()
	{
		ServerInfo serverInfo  = new ServerInfo("test", StateId.ONLINE.name(), new InetSocketAddress("localhost", 9000),
				"source1", "source2", "source3", "source4");

		String jsonString = serverInfo.toJsonString();

		assertEquals(true, jsonString.contains("\"name\":\"test\""));
		assertEquals(true, jsonString.contains("[\"source1\",\"source2\",\"source3\",\"source4\"]"));
	}

	@Test
	public void testBuilderSetAddress() throws Exception
	{
	  ServerInfoBuilder builder = new ServerInfoBuilder();
	  String address = ServerInfoBuilder.generateAddress(null, "localhost", 12345,
                                                         "com.linkedin.events.source1",
                                                         "com.linkedin.events.source2",
                                                         "com.linkedin.events.source3");
	  builder.setAddress(address);
	  ServerInfo si = builder.build();
	  assertEquals(12345, si.getAddress().getPort());
	  assertEquals(3, si.getSources().size());
	  assertEquals("com.linkedin.events.source1", si.getSources().get(0));
      assertEquals("com.linkedin.events.source2", si.getSources().get(1));
      assertEquals("com.linkedin.events.source3", si.getSources().get(2));
	}

    @Test
    public void testBuilderSetAddressWithName() throws Exception
    {
      ServerInfoBuilder builder = new ServerInfoBuilder();
      String address = ServerInfoBuilder.generateAddress("SeRvEr", "localhost", 99,
                                                         "com.linkedin.events.source1",
                                                         "com.linkedin.events.source3");
      builder.setAddress(address);
      ServerInfo si = builder.build();
      assertEquals("SeRvEr", si.getName());
      assertEquals(99, si.getAddress().getPort());
      assertEquals(2, si.getSources().size());
      assertEquals("com.linkedin.events.source1", si.getSources().get(0));
      assertEquals("com.linkedin.events.source3", si.getSources().get(1));
    }

    @Test
    public void testServerInfoSetBuilder() throws Exception
    {
      ServerInfoBuilder sib = new ServerInfoBuilder();
      String address1 = ServerInfoBuilder.generateAddress("SeRvEr1", "localhost", 98,
                                                         "com.linkedin.events.source1",
                                                         "com.linkedin.events.source3");
      sib.setAddress(address1);
      ServerInfo si1 = sib.build();

      String address2 = ServerInfoBuilder.generateAddress("SeRvEr2", "localhost", 99,
                                                          "com.linkedin.events.source1",
                                                          "com.linkedin.events.source3");
      sib.setAddress(address2);
      ServerInfo si2 = sib.build();

      String address3 = ServerInfoBuilder.generateAddress(null, "localhost", 100,
                                                         "com.linkedin.events.source1",
                                                         "com.linkedin.events.source2",
                                                         "com.linkedin.events.source3");
      sib.setAddress(address3);
      ServerInfo si3 = sib.build();

      ServerInfoSetBuilder builder = new ServerInfoSetBuilder();
      builder.setServers(address1 + ServerInfoSetBuilder.SERVER_INFO_SEPARATOR + address2 +
                         ServerInfoSetBuilder.SERVER_INFO_SEPARATOR + address3);
      List<ServerInfo> res = builder.build();

      assertEquals(3, res.size());
      assertEquals(true, res.contains(si1));
      assertEquals(true, res.contains(si2));
      assertEquals(true, res.contains(si3));
    }
}
