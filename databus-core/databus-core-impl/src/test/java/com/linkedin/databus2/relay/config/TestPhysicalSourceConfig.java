package com.linkedin.databus2.relay.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.util.InvalidConfigException;

public class TestPhysicalSourceConfig {
	private final String _configSource = "{\n" +
			"    \"name\" : \"multBufferTest\",\n" +
			"    \"id\" : 100,\n" +
			"    \"uri\" : \"doesnot matter\",\n" +
			"        \"slowSourceQueryThreshold\" : 2000,\n" +
			"        \"sources\" :\n" +
			"        [\n" +
			"                {\"id\" : 2, \n" +
			"                 \"name\" : \"com.linkedin.events.member2.profile.MemberProfile\",\n" +
			"                 \"uri\": \"member2.member_profile\", \n" +
			"                 \"partitionFunction\" : \"constant:1\"\n" +
			"                },\n" +
			"                {\"id\" : 3, \n" +
			"                 \"name\" : \"com.linkedin.events.member2.account.MemberAccount\",\n" +
			"                 \"uri\" : \"member2.member_account\", \n" +
			"                 \"partitionFunction\" : \"constant:1\"\n" +
			"                },\n" +
			"                {\"id\" : 4, \n" +
			"                 \"name\" : \"com.linkedin.events.member2.businessattr.MemberBusinessAttr\",\n" +
			"                 \"uri\" : \"member2.member_business_attr\", \n" +
			"                 \"partitionFunction\" : \"constant:1\"\n" +
			"                },\n" +
			"                {\"id\" : 5, \n" +
			"                 \"name\" : \"com.linkedin.events.member2.setting.MemberSetting\",\n" +
			"                 \"uri\" : \"member2.member_setting\", \n" +
			"                 \"partitionFunction\" : \"constant:1\"\n" +
			"                }\n" +
			"        ]\n" +
			"}";

	// test partial constructor
    @Test
    public void testPhysicalSourceConfigConstructor() {
      Integer pPartitionId = 10;
      String name = "dbName";
      PhysicalPartition pPartition = new PhysicalPartition(pPartitionId, name);
      PhysicalSource pSource = new PhysicalSource("uri");

      PhysicalSourceConfig pConfig = new PhysicalSourceConfig(pPartition.getName(), pSource.getUri(), pPartition.getId());

      int lSourceId = 10;
      String lSourceName = "lName";
      for(int i=0; i< 10; i++) {
        LogicalSourceConfig lSC = new LogicalSourceConfig();
        lSourceId = lSourceId + i;
        lSC.setId((short)lSourceId);
        lSC.setName(lSourceName+lSourceId);
        lSC.setPartition((short)0);
        lSC.setUri("lUri");
        pConfig.addSource(lSC);
      }
      assertEquals(10, pConfig.getSources().size(), "number of logical source doesn't match");
    }

	// test to see that we read and convert config correctly
	@Test
	public void testPhysicalSourceConfig() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(_configSource));

		//PhysicalSourceConfig physicalSourceConfig = mapper.readValue(sourcesJson, PhysicalSourceConfig.class);
		PhysicalSourceConfig physicalSourceConfig = mapper.readValue(isr, PhysicalSourceConfig.class);
		PhysicalSourceStaticConfig pStatic=null;
		try {
		  physicalSourceConfig.checkForNulls();
		  pStatic = physicalSourceConfig.build();
		} catch (InvalidConfigException e) {
		  fail("PhysicalSourceConfig.checkForNulls failed.", e);
		}

		// also assert basic stuff
		assertNotNull("Uri is null.", pStatic.getUri());
		assertNotNull("Name is null.", pStatic.getName());
		assertEquals(100, pStatic.getId(),"physical source id mismatch:");

		List<LogicalSourceConfig>lSources = physicalSourceConfig.getSources();
		assertEquals(4, lSources.size(), "number of logical sources");
		LogicalSourceStaticConfig lSource=null;
		try
		{
		  lSource = lSources.get(0).build();
		}
		catch (InvalidConfigException e)
		{
		  fail("get LogicalSourceStaticConfig failed", e);
		}
		assertEquals(2, lSource.getId(), "logical source id");
	}

	@Test
	public void testParsePhysicalPartitionString () throws IOException {
	  String partString  = "abc_123";
	  PhysicalPartition pPart = PhysicalPartition.parsePhysicalPartitionString(partString, "_");
	  assertEquals("abc", pPart.getName());
	  assertEquals(123, pPart.getId().intValue());

	  partString  = "abc.123";
      pPart = PhysicalPartition.parsePhysicalPartitionString(partString, "\\.");
      assertEquals("abc", pPart.getName());
      assertEquals(123, pPart.getId().intValue());

      String [] partStrings = new String [] {"abc.123","abc123", "123", "abc", "" };
      for(String s : partStrings) {
        try {
          PhysicalPartition.parsePhysicalPartitionString(s, "_");
          fail("should fail on invalid partition string");
        } catch (IOException e) {
          // expected
        }
      }
	}
	
	private String mapToJsonStr(Map<String,Object> map) throws JsonGenerationException, JsonMappingException, IOException
	{

		ObjectMapper mapper = new ObjectMapper();
		StringWriter writer = new StringWriter();
		mapper.writeValue(writer, map);
		String str =  writer.toString();
		return str;
	}
	
	@Test
	public void testPhysicalSourceConfigSerialize() throws JsonParseException, JsonMappingException, IOException 
	{
		PhysicalSourceConfig pconfig = new PhysicalSourceConfig();
		LogicalSourceConfig tab1 = new LogicalSourceConfig();
		LogicalSourceConfig tab2 = new LogicalSourceConfig();
		LogicalSourceConfig tab3 = new LogicalSourceConfig();
		tab1.setId((short) 1); tab2.setId((short) 2); tab3.setId((short) 3);

		ArrayList<LogicalSourceConfig> newSources = new ArrayList<LogicalSourceConfig>();
		newSources.add(tab1); newSources.add(tab2); newSources.add(tab3);
		
		pconfig.setSources(newSources);
		String jsonStr = pconfig.toString();
		System.out.println("json str=" + jsonStr);
		PhysicalSourceConfig npConfig = PhysicalSourceConfig.fromString(jsonStr);
		short id = npConfig.getSource(2).getId();
		System.out.println("id=" + id);
		Map<String,Object> map = pconfig.toMap();
		String s = mapToJsonStr(map);
		System.out.println("map=" + s);
		ArrayList<Map<String,Object>> sconf = (ArrayList<Map<String,Object>>) map.get("sources");
		sconf.get(2).put("id",(short)45);
		
		PhysicalSourceConfig mConfig = PhysicalSourceConfig.fromMap(map);
		System.out.println("new map obj=" + mConfig.toString());
		 
	}
	
}
