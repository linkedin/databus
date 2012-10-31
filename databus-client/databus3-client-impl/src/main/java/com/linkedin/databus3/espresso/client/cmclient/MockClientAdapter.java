package com.linkedin.databus3.espresso.client.cmclient;

import java.util.List;
import java.util.Map;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.ExternalView;

public class MockClientAdapter implements IAdapter {

	private final int _testcaseNum;

	public MockClientAdapter(int testcaseNum)
	{
		_testcaseNum = testcaseNum;
	}

	@Override
	public Map<ResourceKey, List<DatabusServerCoordinates>> getExternalView(boolean cached, String dbName) {
		String ev = "";
		if ( _testcaseNum == 1)
			ev = _ev1;
		else if ( _testcaseNum == 2)
			ev = _ev2;
		else if ( _testcaseNum == 3)
			ev = _ev3;

		ZNRecordSerializer zs = new ZNRecordSerializer();
		ZNRecord zr = (ZNRecord) zs.deserialize(ev.getBytes());
		return ClientAdapter.getExternalView(new ExternalView(zr));
	}

	@Override
	public Map<DatabusServerCoordinates, List<ResourceKey>> getInverseExternalView(
			boolean cached, String dbName)
	{
		String ev = "";
		if ( _testcaseNum == 1)
			ev = _ev1;
		else if ( _testcaseNum == 2)
			ev = _ev2;

		ZNRecordSerializer zs = new ZNRecordSerializer();
		ZNRecord zr = (ZNRecord) zs.deserialize(ev.getBytes());
		return ClientAdapter.getInverseExternalView(new ExternalView(zr));
	}


	private final String _ev1 =
					"{" +
					"  \"id\" : \"BizProfile\"," +
					"  \"mapFields\" : {" +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p13_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p14_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p17_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p1_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p21_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p2_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p32_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p38_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p3_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p42_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p49_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p4_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p53_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p58_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p63_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app09.corp.linkedin.com_12918,BizProfile,p6_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p0_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p10_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p12_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p19_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p25_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p26_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p29_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p30_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p35_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p36_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p40_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p47_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p52_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p60_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p7_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app10.corp.linkedin.com_12918,BizProfile,p9_1,MASTER\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p11_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p16_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p28_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p31_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p34_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p37_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p39_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p43_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p44_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p45_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p48_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p51_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p57_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p59_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p5_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app11.corp.linkedin.com_12918,BizProfile,p8_1,MASTER\" : {" +
					"      \"eat1-app33.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p15_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p18_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p20_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p22_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p23_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p24_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p27_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p33_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p41_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p46_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p50_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p54_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p55_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p56_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p61_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app12.corp.linkedin.com_12918,BizProfile,p62_1,ERROR\" : {" +
					"      \"eat1-app25.stg.linkedin.com_11140\" : \"ONLINE\"" +
					"    }" +
					"  }," +
					"  \"deltaList\" : [ ]," +
					"  \"simpleFields\" : {" +
					"  }," +
					"  \"listFields\" : {" +
					"  }" +
					"}";



	private final String _ev2 =
			"{" +
					"  \"id\" : \"EspressoDB8\"," +
					"  \"mapFields\" : {" +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p0_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p1_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p2_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p3_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p4_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p5_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p6_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p7_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p0_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p1_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p2_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p3_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p4_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p5_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p6_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p7_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" +  "," +
					"      \"localhost_11110\" : \"ONLINE\"" +
					"    }" +
					"  }," +
					"  \"simpleFields\" : {" +
					"  }," +
					"  \"deltaList\" : [ ]," +
					"  \"listFields\" : {" +
					"  }" +
					"}" ;

	private final String _ev3 =
			"{" +
					"  \"id\" : \"EspressoDB8\"," +
					"  \"mapFields\" : {" +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p0_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p1_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p2_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p3_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p4_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p5_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p6_1,SLAVE\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app122.corp.linkedin.com_12918,EspressoDB8,p7_1,MASTER\" : {" +
					"      \"localhost_11150\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p0_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p1_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p2_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p3_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p4_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p5_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p6_1,MASTER\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }," +
					"    \"eat1-app32.corp.linkedin.com_12918,EspressoDB8,p7_1,SLAVE\" : {" +
					"      \"localhost_11140\" : \"ONLINE\"" + "," +
					"      \"localhost_11110\" : \"OFFLINE\"" +
					"    }" +
					"  }," +
					"  \"simpleFields\" : {" +
					"  }," +
					"  \"deltaList\" : [ ]," +
					"  \"listFields\" : {" +
					"  }" +
					"}" ;

	@Override
	public void addExternalViewChangeObservers(
			DatabusExternalViewChangeObserver observer)
	{

	}

	@Override
	public void removeExternalViewChangeObservers(
			DatabusExternalViewChangeObserver observer)
	{

	}

	@Override
	public int getNumPartitions(String dbName)
	{
	  switch (_testcaseNum)
	  {
	  case 1: return 64;
      case 2: return 8;
      case 3: return 8;
	  default: return -1;
	  }
	}
}
