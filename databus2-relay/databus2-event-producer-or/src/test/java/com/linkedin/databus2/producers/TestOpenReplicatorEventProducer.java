package com.linkedin.databus2.producers;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.google.code.or.OpenReplicator;
import com.linkedin.databus.core.util.InvalidConfigException;

public class TestOpenReplicatorEventProducer {
	@Test
	public void testUriPaths() throws Exception {
		runUriTest("mysql://user%2Fpassword@localhost:3306/1/mysql-binlog", "user", "password", "localhost", 3306, 1, "mysql-binlog");
		runUriTest("mysql://user%2Fpassword@localhost:3306/1/mysql5-binlog", "user", "password", "localhost", 3306, 1, "mysql5-binlog");
	}

	private void runUriTest(String raw, String user, String password, String host, int port, int serverId, String filename) throws InvalidConfigException, URISyntaxException {
		OpenReplicator or = new OpenReplicator();
		String prefix = OpenReplicatorEventProducer.processUri(new URI(raw), or);
		assertEquals(or.getUser(), user);
		assertEquals(or.getPassword(), password);
		assertEquals(or.getHost(), host);
		assertEquals(or.getPort(), port);
		assertEquals(or.getServerId(), serverId);
		assertEquals(prefix, filename);
	}
}
