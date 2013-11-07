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
package com.linkedin.databus.util;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests {@link InteractiveSchemaGeneratorCli}
 */
public class TestInteractiveSchemaGeneratorCli
{

  @Test
  public void testDefaultValues()
  {
    InteractiveSchemaGeneratorCli cli = new InteractiveSchemaGeneratorCli();
    Assert.assertTrue(cli.processCommandLineArgs(new String[]{}));

    Assert.assertFalse(cli.isAutomatic());
    Assert.assertEquals(cli.getDbName(), null);
    Assert.assertEquals(cli.getDburl(), InteractiveSchemaGenerator.DEFAULT_DATABASE);
    Assert.assertEquals(cli.getPassword(), null);
    Assert.assertEquals(cli.getSchemaRegistryPath(),
                        InteractiveSchemaGenerator.DEFAULT_SCHEMA_REGISTRY_LOCATION);
    Assert.assertEquals(cli.getTableName(), null);
    Assert.assertEquals(cli.getUser(), null);
    Assert.assertEquals(cli.getFields(), null);
    Assert.assertEquals(cli.getPrimaryKeys(), null);
  }

  @Test
  public void testAllValues()
  {
    InteractiveSchemaGeneratorCli cli = new InteractiveSchemaGeneratorCli();
    Assert.assertTrue(cli.processCommandLineArgs(new String[]{
        "-A",
        "-b", " testDb",
        "--dburl", "jdbc:some:url",
        "-f", "field1,field2, Field3, field4 ",
        "--pk", " Pk1 , pK2 ",
        "-p", " PaSs WoRd ",
        "-S", "/some/path",
        "--table", "Table1",
        "-u", "admin"
    }));

    Assert.assertTrue(cli.isAutomatic());
    Assert.assertEquals(cli.getDbName(), "testDb");
    Assert.assertEquals(cli.getDburl(), "jdbc:some:url");
    Assert.assertEquals(cli.getPassword(), " PaSs WoRd ");
    Assert.assertEquals(cli.getSchemaRegistryPath(), "/some/path");
    Assert.assertEquals(cli.getTableName(), "Table1");
    Assert.assertEquals(cli.getUser(), "admin");
    Assert.assertEquals(cli.getFields(), Arrays.asList("field1", "field2", "Field3", "field4"));
    Assert.assertEquals(cli.getPrimaryKeys(), Arrays.asList("Pk1", "pK2"));
  }

}
