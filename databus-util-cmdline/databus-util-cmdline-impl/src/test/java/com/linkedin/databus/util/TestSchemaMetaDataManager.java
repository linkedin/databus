package com.linkedin.databus.util;

import com.linkedin.databus2.test.TestUtil;
import java.io.File;
import java.util.List;

import org.apache.log4j.Level;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus.core.util.InvalidConfigException;

public class TestSchemaMetaDataManager
{

  @Test
  /**
   * Steps:
   * 1. Construct a Schema-registry directory with expected meta-files for some example sources and have a backup.
   * 2. Instantiate a SchemaMetaDataManager
   * 3. Call public getter APIs to fetch srcIds and sourceNames and validate they are expected.
   * 4. Call store() and ensure the persisted matches with the backup.
   * 5. Add a new source. Validate the new sourceId is expected.
   * 6. Call public getter APIs to fetch srcIds and sourceNames and validate they are expected.
   *
   * @throws Exception
   */
  public void testSchemaMetaDataManager() throws Exception
  {
    File schema_dir = FileUtils.createTempDir("dir_testSchemaMetaDataManager");

    // Input contents
    String[] idNameMapContents = {
                                  "1:com.linkedin.events.example.person",
                                  "2:com.linkedin.events.example.address",
                                  "3:com.linkedin.events.example.date",
                                  "10:com.linkedin.events.another_example.zipcode",
                                 };

    String[] dbToSrcMapContents = { "{",
                                   "  \"another_example\" : [ \"com.linkedin.events.another_example.zipcode\" ],",
                                   "  \"example\" : [ \"com.linkedin.events.example.address\", \"com.linkedin.events.example.date\", \"com.linkedin.events.example.person\" ]",
                                   "}"
                                 };

    String dbToSrcFile1 = schema_dir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile1 = schema_dir.getAbsolutePath() + "/idToName.map";
    FileUtils.storeLinesToTempFile(dbToSrcFile1,dbToSrcMapContents);
    FileUtils.storeLinesToTempFile(idNameMapFile1, idNameMapContents);

    File newSchemaDir = FileUtils.createTempDir("dir_testSchemaMetaDataManager_backup");
    String dbToSrcFile2 = newSchemaDir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile2 = newSchemaDir.getAbsolutePath() + "/idToName.map";
    Runtime.getRuntime().exec("cp " + dbToSrcFile1 + " " + dbToSrcFile2 );
    Runtime.getRuntime().exec("cp " + idNameMapFile1 + " " + idNameMapFile2 );
    SchemaMetaDataManager mgr = new SchemaMetaDataManager(schema_dir.getAbsolutePath());

    //Validate Managed Sources
    List<String> sources = mgr.getManagedSourcesForDB("another_example");
    Assert.assertEquals( sources.size(), 1, "Number of Sources");
    Assert.assertEquals(sources.get(0),"com.linkedin.events.another_example.zipcode","Number of Sources");
    sources = mgr.getManagedSourcesForDB("example");
    Assert.assertEquals(sources.size(),3, "Number of Sources");
    Assert.assertEquals( sources.get(0), "com.linkedin.events.example.person", "Source 1");
    Assert.assertEquals( sources.get(1), "com.linkedin.events.example.address", "Source 2");
    Assert.assertEquals( sources.get(2), "com.linkedin.events.example.date","Source 3");
    Assert.assertNull(mgr.getManagedSourcesForDB("unknown_source"),"Unknown Source");

    //Validate getSrcId
    Assert.assertEquals( mgr.getSrcId("com.linkedin.events.another_example.zipcode"),10,"ZipCode SrcId");
    Assert.assertEquals( mgr.getSrcId("com.linkedin.events.example.person"),1, "Person SrcId");
    Assert.assertEquals( mgr.getSrcId("com.linkedin.events.example.address"), 2, "Address SrcId");
    Assert.assertEquals( mgr.getSrcId("com.linkedin.events.example.date"),3, "Date SrcId");

    //unknown SrcName
    boolean exception = false;
    try
    {
      mgr.getSrcId("Unknown SrcId");
    } catch (RuntimeException re) {
      exception = true;
    }
    Assert.assertTrue(exception, "Got exception ?");

    //Store to meta-files and compare to expected files.
    mgr.store();
    FileUtils.compareTwoTextFiles(dbToSrcFile1, dbToSrcFile2);
    FileUtils.compareTwoTextFiles(idNameMapFile1, idNameMapFile2);

    // Adding new source
    short srcId = mgr.updateAndGetNewSrcId("another_example", "com.linkedin.events.another_example.city");
    Assert.assertEquals(srcId,11,"New SrcId ");
    sources = mgr.getManagedSourcesForDB("another_example");
    Assert.assertEquals( sources.size(), 2, "Number of Sources");
    Assert.assertEquals(sources.get(0),"com.linkedin.events.another_example.zipcode","Zipcode Sources");
    Assert.assertEquals(sources.get(1),"com.linkedin.events.another_example.city","City Source ");
    Assert.assertEquals( mgr.getSrcId("com.linkedin.events.another_example.city"),11,"City SrcId");
    sources = mgr.getManagedSourcesForDB("example");
    Assert.assertEquals(sources.size(),3, "Number of Sources");
    Assert.assertEquals( sources.get(0), "com.linkedin.events.example.person", "Source 1");
    Assert.assertEquals( sources.get(1), "com.linkedin.events.example.address", "Source 2");
    Assert.assertEquals( sources.get(2), "com.linkedin.events.example.date","Source 3");
    Assert.assertNull(mgr.getManagedSourcesForDB("unknown_source"),"Unknown Source");

    System.out.println(mgr.getDbToSrcMap());
  }

  @Test
  public void testDuplicateSourceId() throws Exception
  {
    File schema_dir = FileUtils.createTempDir("dir_testSchemaMetaDataManager");

    // Input contents
    String[] idNameMapContents = {
                                  "1:com.linkedin.events.example.person",
                                  "2:com.linkedin.events.example.address",
                                  "3:com.linkedin.events.example.date",
                                  "2:com.linkedin.events.another_example.zipcode",
                                 };

    String[] dbToSrcMapContents = { "{",
                                   "  \"another_example\" : [ \"com.linkedin.events.another_example.zipcode\" ],",
                                   "  \"example\" : [ \"com.linkedin.events.example.address\", \"com.linkedin.events.example.date\", \"com.linkedin.events.example.person\" ]",
                                   "}"
                                 };

    String dbToSrcFile1 = schema_dir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile1 = schema_dir.getAbsolutePath() + "/idToName.map";
    FileUtils.storeLinesToTempFile(dbToSrcFile1,dbToSrcMapContents);
    FileUtils.storeLinesToTempFile(idNameMapFile1, idNameMapContents);

    File newSchemaDir = FileUtils.createTempDir("dir_testSchemaMetaDataManager_backup");
    String dbToSrcFile2 = newSchemaDir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile2 = newSchemaDir.getAbsolutePath() + "/idToName.map";
    Runtime.getRuntime().exec("cp " + dbToSrcFile1 + " " + dbToSrcFile2 );
    Runtime.getRuntime().exec("cp " + idNameMapFile1 + " " + idNameMapFile2 );

    SchemaMetaDataManager mgr = null;

    //unknown SrcName
    boolean exception = false;
    try
    {
      mgr = new SchemaMetaDataManager(schema_dir.getAbsolutePath());
    } catch (RuntimeException re) {
      exception = true;
    }
    Assert.assertTrue(exception, "Got exception ?");
  }

  @Test
  public void testDuplicateSourceName() throws Exception
  {
    File schema_dir = FileUtils.createTempDir("dir_testSchemaMetaDataManager");

    // Input contents
    String[] idNameMapContents = {
                                  "1:com.linkedin.events.example.person",
                                  "2:com.linkedin.events.example.address",
                                  "3:com.linkedin.events.example.address",
                                  "4:com.linkedin.events.another_example.zipcode",
                                 };

    String[] dbToSrcMapContents = { "{",
                                   "  \"another_example\" : [ \"com.linkedin.events.another_example.zipcode\" ],",
                                   "  \"example\" : [ \"com.linkedin.events.example.address\", \"com.linkedin.events.example.date\", \"com.linkedin.events.example.person\" ]",
                                   "}"
                                 };

    String dbToSrcFile1 = schema_dir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile1 = schema_dir.getAbsolutePath() + "/idToName.map";
    FileUtils.storeLinesToTempFile(dbToSrcFile1,dbToSrcMapContents);
    FileUtils.storeLinesToTempFile(idNameMapFile1, idNameMapContents);

    File newSchemaDir = FileUtils.createTempDir("dir_testSchemaMetaDataManager_backup");
    String dbToSrcFile2 = newSchemaDir.getAbsolutePath() + "/physical_logical_src_map.json";
    String idNameMapFile2 = newSchemaDir.getAbsolutePath() + "/idToName.map";
    Runtime.getRuntime().exec("cp " + dbToSrcFile1 + " " + dbToSrcFile2 );
    Runtime.getRuntime().exec("cp " + idNameMapFile1 + " " + idNameMapFile2 );

    SchemaMetaDataManager mgr = null;
    //unknown SrcName
    boolean exception = false;
    try
    {
      mgr = new SchemaMetaDataManager(schema_dir.getAbsolutePath());
    } catch (RuntimeException re) {
      exception = true;
    }
    Assert.assertTrue(exception, "Got exception ?");
  }

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    //setup logging
    TestUtil.setupLogging(true, null, Level.INFO);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
  }
}
