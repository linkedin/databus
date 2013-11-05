package com.linkedin.databus2.schemas;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.Map;
import java.util.SortedMap;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestVersionedSchemaSet
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
  private static final String TEST_SCHEMA4_TEXT_TEMPLATE =
      "{\"name\":\"%s\",\"namespace\":\"com.linkedin.databus2.test\",\"type\":\"record\"," +
      " \"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}";

  @Test
  public void testOneAdd() throws Exception
  {
    VersionedSchemaSet schemaSet = new VersionedSchemaSet();
    Schema schema1 = Schema.parse(TEST_SCHEMA1_TEXT);
    VersionedSchema vs1 = new VersionedSchema("testSchema", (short)1, schema1, null);

    Assert.assertTrue(schemaSet.add(vs1));

    VersionedSchema test1 = schemaSet.getLatestVersionByName("testSchema");
    Assert.assertNotNull(test1, "schema present");
    Assert.assertEquals(test1.getVersion(), (short)1, "version correct");
    Assert.assertEquals(test1.getSchemaBaseName(), "testSchema");

    SchemaId vs1Id = SchemaId.createWithMd5(schema1);
    VersionedSchema test2 = schemaSet.getById(vs1Id);
    Assert.assertNotNull(test2, "schema present");
    Assert.assertEquals(test2, vs1, "schema id correct");

    VersionedSchema test3 = schemaSet.getSchemaByNameVersion("testSchema", (short)1);
    Assert.assertNotNull(test3, "schema present");
    Assert.assertEquals(test3, vs1, "schema name-version correct");

    test3 = schemaSet.getSchemaByNameVersion("testSchema", (short)2);
    Assert.assertNull(test3, "schema name-version not present");

    test1 = schemaSet.getLatestVersionByName("TestSchema");
    Assert.assertNull(test1, "schema base name not present");

    //make sure we don't readd the same schema
    Assert.assertTrue(!schemaSet.add(vs1));
    Assert.assertTrue(!schemaSet.add("testSchema", (short)1, TEST_SCHEMA1_TEXT));
    //make sure we don't override existing schema
    Assert.assertTrue(!schemaSet.add("testSchema", (short)1, TEST_SCHEMA2_TEXT));
    Assert.assertEquals(schemaSet.getSchema(new VersionedSchemaId("testSchema", (short)1)),
                        vs1);

  }

  @Test
  public void testExternalSchemaIdAdd() throws Exception
  {
      VersionedSchemaSet schemaSet = new VersionedSchemaSet();
      byte[] crc32 = {0x01,0x02,0x03,0x04};
      String sourceName = "test_schema";
      boolean added = schemaSet.add(sourceName,(short) 1,new SchemaId(crc32),TEST_SCHEMA1_TEXT);
      Assert.assertTrue(added);
      SchemaId fetchId = new SchemaId(crc32);
      VersionedSchema vs = schemaSet.getById(fetchId);
      Assert.assertTrue(vs != null);
      Assert.assertEquals(vs.getVersion() , 1);
      Assert.assertEquals(sourceName,vs.getSchemaBaseName());
  }

  @Test
  public void testMultiAdd() throws Exception
  {
    VersionedSchemaSet schemaSet = new VersionedSchemaSet();

    Schema schema1 = Schema.parse(TEST_SCHEMA1_TEXT);
    SchemaId vs1Id = SchemaId.createWithMd5(schema1);
    VersionedSchema vs1 = new VersionedSchema("testSchema", (short)1, schema1, null);
    schemaSet.add(vs1);

    Schema schema2 = Schema.parse(TEST_SCHEMA2_TEXT);
    SchemaId vs2Id = SchemaId.createWithMd5(schema2);
    VersionedSchema vs2 = new VersionedSchema("testSchema", (short)2, schema2, null);
    schemaSet.add(vs2);

    Schema schema3 = Schema.parse(TEST_SCHEMA3_TEXT);
    SchemaId vs3Id = SchemaId.createWithMd5(schema3);
    VersionedSchema vs3 = new VersionedSchema("anotherSchema", (short)3, schema3, null);
    schemaSet.add(vs3);

    VersionedSchema test1 = schemaSet.getLatestVersionByName("testSchema");
    Assert.assertNotNull(test1, "schema present");
    Assert.assertEquals(test1.getVersion(), (short)2, "version correct");
    Assert.assertEquals(test1.getSchemaBaseName(), "testSchema");

    VersionedSchema test2 = schemaSet.getById(vs1Id);
    Assert.assertNotNull(test2, "schema present");
    Assert.assertEquals(test2, vs1, "schema id 1 correct");

    test2 = schemaSet.getById(vs2Id);
    Assert.assertNotNull(test2, "schema present");
    Assert.assertEquals(test2, vs2, "schema id 2 correct");

    test2 = schemaSet.getById(vs3Id);
    Assert.assertNotNull(test2, "schema present");
    Assert.assertEquals(test2, vs3, "schema id 3 correct");

    VersionedSchema test3 = schemaSet.getSchemaByNameVersion("anotherSchema", (short)3);
    Assert.assertNotNull(test3, "schema present");
    Assert.assertEquals(test3, vs3, "schema name-version correct");

    test3 = schemaSet.getSchemaByNameVersion("testSchema", (short)2);
    Assert.assertNotNull(test3, "schema present");
    Assert.assertEquals(test3, vs2, "schema name-version correct");

    SortedMap<VersionedSchemaId, VersionedSchema> testSchemas = schemaSet.getAllVersionsByName("testSchema");
    Assert.assertNotNull(testSchemas, "schemas list present");
    Assert.assertEquals(testSchemas.size(), 2, "correct number of schemas list");
    Assert.assertEquals(testSchemas.get(testSchemas.firstKey()), vs1, "v1 present");
    Assert.assertEquals(testSchemas.get(testSchemas.lastKey()), vs2, "v2 present");

    schemaSet.add("testSchema",  (short)3, TEST_SCHEMA3_TEXT);
    Assert.assertEquals(schemaSet.getLatestVersionByName("testSchema").getSchema().getName(),
                        "AnotherSchema1");
  }


  @Test
  public void testConcurrentAdd() throws InterruptedException
  {
    final VersionedSchemaSet schemaSet = new VersionedSchemaSet();

    final int schemaNum = 10;
    final short versionNum = 10;
    Thread[][] threads = new Thread[schemaNum][];
    for (int sIdx = 0; sIdx < schemaNum; ++sIdx)
    {
      threads[sIdx] = new Thread[versionNum + 1];
      for (short vIdx = 1; vIdx <= versionNum;  ++vIdx)
      {
        final int finalSIdx = sIdx;
        final short finalVIdx = vIdx;
        threads[sIdx][vIdx] = new Thread(new Runnable()
        {
          @Override
          public void run()
          {
            String schemaName = "schema" + finalSIdx;
            String schemaStr = String.format(TEST_SCHEMA4_TEXT_TEMPLATE, schemaName);
            schemaSet.add(schemaName, finalVIdx, schemaStr);
          }
        }, "schema adder " + sIdx + "-" + vIdx);
        threads[sIdx][vIdx].setDaemon(true);
      }
    }
    for (int sIdx = 0; sIdx < schemaNum; ++sIdx)
    {
      for (short vIdx = 1; vIdx <= versionNum;  ++vIdx)
      {
        threads[sIdx][vIdx].start();
      }
    }

    for (int sIdx = 0; sIdx < schemaNum; ++sIdx)
    {
      for (short vIdx = 1; vIdx <= versionNum;  ++vIdx)
      {
        threads[sIdx][vIdx].join(1000);
        Assert.assertTrue(!threads[sIdx][vIdx].isAlive(), threads[sIdx][vIdx].getName() + " is dead");
        VersionedSchema vschema = schemaSet.getSchemaByNameVersion("schema" + sIdx, vIdx);
        Assert.assertNotNull(vschema, "schema" + sIdx + "-" + vIdx + " added");
        Assert.assertEquals(vschema.getSchemaBaseName(), "schema" + sIdx);
        Assert.assertEquals(vschema.getSchema().getName(), "schema" + sIdx);
        Assert.assertEquals(vschema.getVersion(), vIdx);
      }
    }
  }

  @Test
  /** Test that schemas which don't have stable reparsing, we add all MD5's */
  public void testUpdateMd5Index()
  {
    final String schemaStr = "{\"type\":\"record\",\"name\":\"A\",\"fields\":[" +
        "{\"name\":\"J\",\"type\":\"string\",\"J1\":\"V1\",\"indexType\":\"V2\",\"d\":" +
        "\"V3\",\"J3\":\"10000\"}]}";

    VersionedSchemaSet vschemaSet = new VersionedSchemaSet(true);
    vschemaSet.add("testSchema", (short)1, schemaStr);
    Assert.assertEquals(vschemaSet.getIdToSchema().size(), 2);

    vschemaSet.add("TestSchema", (short)3, TEST_SCHEMA3_TEXT);
    Assert.assertEquals(vschemaSet.getIdToSchema().size(), 3);
    for (Map.Entry<SchemaId, VersionedSchema> e: vschemaSet.getIdToSchema().entrySet())
    {
      System.out.println(String.format("ids[%s] -> %s.%d", e.getKey(), e.getValue().getSchemaBaseName(),
                                       e.getValue().getVersion()));
    }
  }
}
