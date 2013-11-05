package com.linkedin.databus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;

public class TestDevRelaySrcConfigGen
{

  @Test
  /**
   *
   * Steps:
   * 1. Generate new Schema_registry with some example schemas.
   * 2. Construct schemaDataMAnager and generate RelayDevConfigs for the example DB.
   * 3. Open the newly generated json file containing physical-sources config and de-serialize to construct a physicalSourcesConfig object
   * 4. Ensure physicalSourcesConfig object have expected source configurations.
   *
   * @throws Exception
   */
  public void testDevRelaySrcConfigGen()
    throws Exception
  {
    // Schema
    String[] personSchema = { "{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"com.linkedin.events.example\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"firstName\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=FIRST_NAME;dbFieldPosition=2;\"},{\"name\":\"lastName\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=LAST_NAME;dbFieldPosition=3;\"},{\"name\":\"birthDate\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=BIRTH_DATE;dbFieldPosition=4;\"},{\"name\":\"deleted\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DELETED;dbFieldPosition=5;\"}],\"meta\":\"dbFieldName=sy$person;\"}"};
    String[] addressSchema = { "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"com.linkedin.events.example\",\"fields\":[{\"name\":\"txn\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=TXN;dbFieldPosition=0;\"},{\"name\":\"key\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=KEY;dbFieldPosition=1;\"},{\"name\":\"addressLine1\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ADDRESS_LINE_1;dbFieldPosition=2;\"},{\"name\":\"addressLine2\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=ADDRESS_LINE_2;dbFieldPosition=3;\"},{\"name\":\"zipCode\",\"type\":[\"long\",\"null\"],\"meta\":\"dbFieldName=ZIP_CODE;dbFieldPosition=4;\"},{\"name\":\"deleted\",\"type\":[\"string\",\"null\"],\"meta\":\"dbFieldName=DELETED;dbFieldPosition=5;\"}],\"meta\":\"dbFieldName=sy$address;\"}"};

    // Index Schema registry contents
    String[] indexContents = { "com.linkedin.events.example.person.avsc",
                               "com.linkedin.events.example.address.avsc"};

    // Meta-Data contents
    String[] idNameMapContents = {
                                  "1:com.linkedin.events.example.person",
                                  "2:com.linkedin.events.example.address"};

    String[] dbToSrcMapContents = { "{",
                                   "  \"example\" : [ \"com.linkedin.events.example.Address\", \"com.linkedin.events.example.Person\" ]",
                                   "}"
                                 };

    File schemaDir = FileUtils.createTempDir("dir_testDevRelaySrcConfigGen");
    String dbToSrcFile1 = schemaDir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile1 = schemaDir.getAbsolutePath() + "/idToName.map";
    String indexRegistry = schemaDir.getAbsolutePath() + "/index.schemas_registry";
    String personSchemaFile = schemaDir.getAbsolutePath() + "/com.linkedin.events.example.person.1.avsc";
    String addressSchemaFile = schemaDir.getAbsolutePath() + "/com.linkedin.events.example.address.1.avsc";

    FileUtils.storeLinesToTempFile(dbToSrcFile1, dbToSrcMapContents);
    FileUtils.storeLinesToTempFile(idNameMapFile1, idNameMapContents);
    FileUtils.storeLinesToTempFile(indexRegistry, indexContents);
    FileUtils.storeLinesToTempFile(personSchemaFile, personSchema);
    FileUtils.storeLinesToTempFile(addressSchemaFile, addressSchema);

    SchemaMetaDataManager mgr = new SchemaMetaDataManager(schemaDir.getAbsolutePath());
    String uri = "jdbc:oracle:thin:liar/liar@devdb:DB";
    List<String> exampleSrcs = Arrays.asList("com.linkedin.events.example.person", "com.linkedin.events.example.address");
    DevRelayConfigGenerator.generateRelayConfig(schemaDir.getAbsolutePath(), "example", uri, schemaDir.getAbsolutePath(), exampleSrcs, mgr);

    File f = new File(schemaDir.getAbsolutePath() + "/sources-example.json");
    Assert.assertTrue( f.exists(),"Physical Src Config file present?");

    BufferedReader r = new BufferedReader(new FileReader(f));

    String json = r.readLine();
    PhysicalSourceConfig config = PhysicalSourceConfig.fromString(json);
    config.checkForNulls();

    Assert.assertEquals(config.getUri(), uri);
    Assert.assertEquals(config.getName(), "example");

    List<LogicalSourceConfig> srcs = config.getSources();
    Assert.assertEquals(srcs.size(),2 );
    Assert.assertEquals(srcs.get(0).getId(), 1);
    Assert.assertEquals(srcs.get(0).getName(), "com.linkedin.events.example.person");
    Assert.assertEquals(srcs.get(0).getUri(), "example.sy$person");
    Assert.assertEquals(srcs.get(1).getId(), 2);
    Assert.assertEquals(srcs.get(1).getName(), "com.linkedin.events.example.address");
    Assert.assertEquals(srcs.get(1).getUri(), "example.sy$address");
  }

}
