package com.linkedin.databus2.schemas;

import java.io.File;
import java.io.FileWriter;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestFileSystemVersionedSchemaSetProvider
{
  private static final String TEST_SCHEMA1_TEXT =
    "{\"name\":\"schema1\",\"namespace\":\"com.linkedin.databus2.test\",\"type\":\"record\"," +
    " \"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}";
  private static final String TEST_SCHEMA2_TEXT =
    "{\"name\":\"schema2\",\"namespace\":\"com.linkedin.databus2.test\",\"type\":\"record\"," +
    " \"fields\":[{\"name\":\"field1\",\"type\":\"double\"},{\"name\":\"field2\",\"type\":\"string\"}]}";
  private static final String TEST_SCHEMA3_TEXT =
    "{\"name\":\"AnotherSchema1\",\"namespace\":\"com.linkedin.databus2.test\",\"type\":\"record\"," +
    " \"fields\":[{\"name\":\"field1\",\"type\":\"double\"},{\"name\":\"field2\",\"type\":\"string\"}]}";

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    //Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Test
  public void testSimpleDir() throws Exception
  {
    final File tmpDir = File.createTempFile("TestFileSystemVersionedSchemaSetProvider", "tmp");
    tmpDir.deleteOnExit();

    Assert.assertTrue(tmpDir.delete(), "temp file deleted");
    Assert.assertTrue(tmpDir.mkdir());

    File schema1File = new File(tmpDir, "TestSchema.1.avsc");
    schema1File.deleteOnExit();

    File schema2File = new File(tmpDir, "TestSchema.3.avsc");
    schema2File.deleteOnExit();

    File schema3File = new File(tmpDir, "AnotherSchema.2.avsc");
    schema3File.deleteOnExit();

    FileWriter schema1Writer = null;
    FileWriter schema2Writer = null;
    FileWriter schema3Writer = null;

    try
    {

      schema3Writer = new FileWriter(schema3File);
      schema3Writer.write(TEST_SCHEMA3_TEXT);
      schema3Writer.flush();

      schema2Writer = new FileWriter(schema2File);
      schema2Writer.write(TEST_SCHEMA2_TEXT);
      schema2Writer.flush();

      schema1Writer = new FileWriter(schema1File);
      schema1Writer.write(TEST_SCHEMA1_TEXT);
      schema1Writer.flush();

      FileSystemVersionedSchemaSetProvider schemaSetProvider =
          new FileSystemVersionedSchemaSetProvider(tmpDir);
      VersionedSchemaSet schemaSet = schemaSetProvider.loadSchemas();

      VersionedSchema vs1 = schemaSet.getLatestVersionByName("TestSchema");
      Assert.assertNotNull(vs1);
      Assert.assertEquals(vs1.getId(), new VersionedSchemaId("TestSchema", (short)3),
                          "latest TestSchema id correct");


      SchemaId vs1Id = SchemaId.forSchema(TEST_SCHEMA1_TEXT);
      VersionedSchema vs11 = schemaSet.getById(vs1Id);
      Assert.assertNotNull(vs11);
      Assert.assertEquals(vs11.getId(), new VersionedSchemaId("TestSchema", (short)1),
                          "TestSchema v1 present");

      VersionedSchema vs12 = schemaSet.getSchemaByNameVersion("TestSchema", (short)1);
      Assert.assertNotNull(vs12);
      Assert.assertEquals(vs12.getId(), new VersionedSchemaId("TestSchema", (short)1),
                          "TestSchema v1 readable");

      VersionedSchema vs2 = schemaSet.getSchemaByNameVersion("AnotherSchema", (short)2);
      Assert.assertNotNull(vs2);
      Assert.assertEquals(vs2.getId(), new VersionedSchemaId("AnotherSchema", (short)2),
                          "AnotherSchema v2 readable");

      VersionedSchema vs21 = schemaSet.getSchemaByNameVersion("AnotherSchema", (short)1);
      Assert.assertNull(vs21, "AnotherSchema v1 not present");

      VersionedSchema vs3 = schemaSet.getSchemaByNameVersion("FakeSchema", (short)1);
      Assert.assertNull(vs3, "FakeSchema not present");

    }
    finally
    {
      if (null != schema1Writer) schema1Writer.close();
      if (null != schema2Writer) schema2Writer.close();
      if (null != schema3Writer) schema3Writer.close();
    }

  }

}
