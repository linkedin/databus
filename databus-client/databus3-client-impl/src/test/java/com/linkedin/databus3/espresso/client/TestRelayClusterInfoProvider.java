package com.linkedin.databus3.espresso.client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus3.espresso.client.cmclient.RelayClusterInfoProvider;

public class TestRelayClusterInfoProvider {
	public static final Logger LOG = Logger.getLogger("TestRelayClusterInfoProvider");

	static
	{
		LOG.getRootLogger().setLevel(Level.ERROR);		
	}

	@Test
	public void testExternalViewProcessing1()
			throws Exception
			{
		int testcaseNum = 1;
		RelayClusterInfoProvider rc= new RelayClusterInfoProvider(testcaseNum);
		Map<ResourceKey, List<DatabusServerCoordinates>> m = rc.getExternalView("");
		assertEquals(m.size(), 64);
			}

	@Test
	public void testExternalViewProcessingForMasterRole()
			throws Exception
			{
		/**
		 * 8 partitions on S1 ( 4 masters + 4 slaves ), 8 partitions on S2 ( corresponding 4 slaves + 4 masters )
		 * Have two relays R1 for S1 and R2 for S2. Another Relay specifically serves all slave partitions
		 * Clients asking for master role
		 */
		int testcaseNum = 2;
		RelayClusterInfoProvider rc= new RelayClusterInfoProvider(testcaseNum);
		Map<ResourceKey, List<DatabusServerCoordinates>> m = rc.getExternalView("");
		assertEquals(m.size(), 16);

		DatabusHttpV3ClientImpl v3Client = new DatabusHttpV3ClientImpl();

		DatabusSubscription ds0 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/0/*");
		DatabusSubscription ds1 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/1/*");
		DatabusSubscription ds2 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/2/*");
		DatabusSubscription ds3 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/3/*");
		DatabusSubscription ds4 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/4/*");
		DatabusSubscription ds5 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/5/*");
		DatabusSubscription ds6 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/6/*");
		DatabusSubscription ds7 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/7/*");
		
		
		
		// Check there are two relays to server partition 0 
		List<DatabusSubscription> lds = Arrays.asList(ds0);
		List<DatabusServerCoordinates> lrc = v3Client.getCandidateServingListForSubscriptionList(lds, m, true, true);
		assertEquals(1, lrc.size());

		// Check there are two relays to server partition 0-3  ( Error Case : No Relay to serve all 4 partitions)
		List<DatabusSubscription> lds2 = Arrays.asList(ds0, ds1, ds2, ds3);
		List<DatabusServerCoordinates> lrc2 = v3Client.getCandidateServingListForSubscriptionList(lds2, m, true, true);
		assertEquals(0, lrc2.size());

		// Check there are two relays to server partition 0-7 
		List<DatabusSubscription> lds3 = Arrays.asList(ds0, ds1, ds2, ds3, ds4, ds5, ds6, ds7);
		List<DatabusServerCoordinates> lrc3 = v3Client.getCandidateServingListForSubscriptionList(lds3, m, true, true);
		assertEquals(0, lrc3.size());		

		// Check there are two relays to server partition 0-7 (Happy Path : when exactly one relay is found)
		List<DatabusSubscription> lds4 = Arrays.asList(ds1, ds4, ds7); 
		List<DatabusServerCoordinates> lrc4 = v3Client.getCandidateServingListForSubscriptionList(lds4, m, true, true);
		assertEquals(1, lrc4.size());		

		// Check there are two relays to server partition 0-7 (Happy Path : when exactly one relay is found)
		List<DatabusSubscription> lds5 = Arrays.asList(ds0, ds2, ds3, ds5, ds6);
		List<DatabusServerCoordinates> lrc5 = v3Client.getCandidateServingListForSubscriptionList(lds5, m, true, true);
		assertEquals(1, lrc4.size());		
			}

	@Test
	public void testExternalViewProcessingForSlaveRole()
			throws Exception
			{
		/**
		 * 8 partitions on S1 ( 4 masters + 4 slaves ), 8 partitions on S2 ( corresponding 4 slaves + 4 masters )
		 * Have two relays R1 for S1 and R2 for S2. Another Relay specifically serves all slave partitions
		 * Clients asking for master role
		 */
		int testcaseNum = 2;
		RelayClusterInfoProvider rc= new RelayClusterInfoProvider(testcaseNum);
		Map<ResourceKey, List<DatabusServerCoordinates>> m = rc.getExternalView("");
		assertEquals(m.size(), 16);

		DatabusHttpV3ClientImpl v3Client = new DatabusHttpV3ClientImpl();

		DatabusSubscription ds0 = createSimpleSourceSlaveSubscription("EspressoDB8.*:0");
		DatabusSubscription ds1 = createSimpleSourceSlaveSubscription("EspressoDB8.*:1");
		DatabusSubscription ds2 = createSimpleSourceSlaveSubscription("EspressoDB8.*:2");
		DatabusSubscription ds3 = createSimpleSourceSlaveSubscription("EspressoDB8.*:3");
		DatabusSubscription ds4 = createSimpleSourceSlaveSubscription("EspressoDB8.*:4");
		DatabusSubscription ds5 = createSimpleSourceSlaveSubscription("EspressoDB8.*:5");
		DatabusSubscription ds6 = createSimpleSourceSlaveSubscription("EspressoDB8.*:6");
		DatabusSubscription ds7 = createSimpleSourceSlaveSubscription("EspressoDB8.*:7");

		LOG.info("Physical SOurce :" + ds0.getPhysicalSource());

		// Check there are two relays to server partition 0 
		List<DatabusSubscription> lds = Arrays.asList(ds0);
		List<DatabusServerCoordinates> lrc = v3Client.getCandidateServingListForSubscriptionList(lds, m, true, true);
		assertEquals(2, lrc.size());

		// Check there are two relays to server partition 0-3  ( Error Case : No Relay to serve all 4 partitions)
		List<DatabusSubscription> lds2 = Arrays.asList(ds0, ds1, ds2, ds3);
		List<DatabusServerCoordinates> lrc2 = v3Client.getCandidateServingListForSubscriptionList(lds2, m, true, true);
		assertEquals(1, lrc2.size());

		// Check there are two relays to server partition 0-7 
		List<DatabusSubscription> lds3 = Arrays.asList(ds0, ds1, ds2, ds3, ds4, ds5, ds6, ds7);
		List<DatabusServerCoordinates> lrc3 = v3Client.getCandidateServingListForSubscriptionList(lds3, m, true, true);
		assertEquals(1, lrc3.size());		

		// Check there are two relays to server partition 0-7 (Happy Path : when exactly one relay is found)
		List<DatabusSubscription> lds4 = Arrays.asList(ds1, ds4, ds7); 
		List<DatabusServerCoordinates> lrc4 = v3Client.getCandidateServingListForSubscriptionList(lds4, m, true, true);
		assertEquals(2, lrc4.size());		

		// Check there are two relays to server partition 0-7 (Happy Path : when exactly one relay is found)
		List<DatabusSubscription> lds5 = Arrays.asList(ds0, ds2, ds3, ds5, ds6);
		List<DatabusServerCoordinates> lrc5 = v3Client.getCandidateServingListForSubscriptionList(lds5, m, true, true);
		assertEquals(2, lrc4.size());		
			}

	@Test
	public void testExternalViewProcessingWithOfflineState()
			throws Exception
			{
		/**
		 * 8 partitions on S1 ( 4 masters + 4 slaves ), 8 partitions on S2 ( corresponding 4 slaves + 4 masters )
		 * Have two relays R1 for S1 and R2 for S2
		 * One more relay "R3" which serves all partition but currently offline
		 */
		int testcaseNum = 3;
		RelayClusterInfoProvider rc= new RelayClusterInfoProvider(testcaseNum);
		Map<ResourceKey, List<DatabusServerCoordinates>> m = rc.getExternalView("");
		assertEquals(m.size(), 16);

		DatabusHttpV3ClientImpl v3Client = new DatabusHttpV3ClientImpl();

		DatabusSubscription testds0 = DatabusSubscription.createSimpleSourceSubscription("EspressoDB8.*:0");
		DatabusSubscription testds00 = DatabusSubscription.createFromUri("espresso:/EspressoDB8/0/*");
		Assert.assertTrue(testds0.equalsSubscription(testds00));

		DatabusSubscription ds0 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/0/*");
		DatabusSubscription ds1 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/1/*");
		DatabusSubscription ds2 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/2/*");
		DatabusSubscription ds3 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/3/*");
		DatabusSubscription ds4 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/4/*");
		DatabusSubscription ds5 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/5/*");
		DatabusSubscription ds6 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/6/*");
		DatabusSubscription ds7 = DatabusSubscription.createFromUri("espresso://MASTER/EspressoDB8/7/*");
		

		// Check there are two relays to server partition 0 
		List<DatabusSubscription> lds = Arrays.asList(ds0);
		List<DatabusServerCoordinates> lrc = v3Client.getCandidateServingListForSubscriptionList(lds, m, true, true);
		assertEquals(1, lrc.size());

		// Check there are two relays to server partition 0-3  ( Error Case : No Relay to serve all 4 partitions)
		List<DatabusSubscription> lds2 = Arrays.asList(ds0, ds1, ds2, ds3);
		List<DatabusServerCoordinates> lrc2 = v3Client.getCandidateServingListForSubscriptionList(lds2, m, true, true);
		assertEquals(0, lrc2.size());

		// Check there are two relays to server partition 0-7 
		List<DatabusSubscription> lds3 = Arrays.asList(ds0, ds1, ds2, ds3, ds4, ds5, ds6, ds7);
		List<DatabusServerCoordinates> lrc3 = v3Client.getCandidateServingListForSubscriptionList(lds3, m, true, true);
		assertEquals(0, lrc3.size());		

		// Check there are two relays to server partition 0-7 (Happy Path : when exactly one relay is found)
		List<DatabusSubscription> lds4 = Arrays.asList(ds1, ds4, ds7); 
		List<DatabusServerCoordinates> lrc4 = v3Client.getCandidateServingListForSubscriptionList(lds4, m, true, true);
		assertEquals(1, lrc4.size());		

		// Check there are two relays to server partition 0-7 (Happy Path : when exactly one relay is found)
		List<DatabusSubscription> lds5 = Arrays.asList(ds0, ds2, ds3, ds5, ds6);
		List<DatabusServerCoordinates> lrc5 = v3Client.getCandidateServingListForSubscriptionList(lds5, m, true, true);
		assertEquals(1, lrc4.size());		
			}

	public static DatabusSubscription createSimpleSourceSlaveSubscription(String source)
	{
		int idx = source.indexOf(':');
		if(idx != -1)
			return createSimpleSourceSlaveSubscriptionV3(source);
		// Setting Logical Source ID = 0
		LogicalSource ls = new LogicalSource(0, source);
		return new DatabusSubscription(PhysicalSource.createSlaveSourceWildcard(),
				PhysicalPartition.createAnyPartitionWildcard(),
				LogicalSourceId.createAllPartitionsWildcard(ls));
	}

	public static DatabusSubscription createSimpleSourceSlaveSubscriptionV3(String source)
	{
		PhysicalPartition pPart = PhysicalPartition.createAnyPartitionWildcard();
		LogicalSourceId.Builder lidB = new LogicalSourceId.Builder();
		lidB.setId((short)0);
		int idx = source.indexOf(':');
		if(idx != -1) {
			String sourceName = source.substring(0, idx);
			int dotIdx = source.indexOf('.');
			String dbName = sourceName.substring(0, dotIdx);
			//String tableName = sourceName.substring(dotIdx+1);
			String pPid = source.substring(idx+1);
			Integer pPidInt = Integer.parseInt(pPid);
			pPart = new PhysicalPartition(pPidInt, dbName);
			lidB.setId(pPidInt.shortValue());
		}
		LogicalSourceId ls = lidB.build();
		return new DatabusSubscription(PhysicalSource.createSlaveSourceWildcard(),
				pPart,
				ls);
	}
}
