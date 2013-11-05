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


import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * NOTE: if this test is failing when run in Eclipse, run it once from the command to populate
 * the resources/ directory.
 * */
public class TestFileSystemSchemaRegistryService
{

  static
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Test
  public void testFallbackToResource() throws Exception
  {
    FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
    configBuilder.setFallbackToResources(true);
    configBuilder.setSchemaDir("/no/such/dir");

    FileSystemSchemaRegistryService.StaticConfig config = configBuilder.build();
    FileSystemSchemaRegistryService service = FileSystemSchemaRegistryService.build(config);

    Map<Short,String > fakeSchemas =
        service.fetchAllSchemaVersionsBySourceName("com.linkedin.events.example.fake.FakeSchema");
    Assert.assertNotNull(fakeSchemas);
    Assert.assertTrue(2 <= fakeSchemas.size());

    String personSchema = service.fetchLatestSchemaBySourceName("com.linkedin.events.example.person.Person");
    Assert.assertNotNull(personSchema);
  }

  @Test(expectedExceptions=InvalidConfigException.class)
  /** Instantiation should fail with fallbackToResourceDisabled and a missing directory */
  public void testErrorOnMissingSchemasDir() throws InvalidConfigException
  {
    final String dirName = "/lets/hope/no/one/creates/this/dir";
    Assert.assertFalse((new File(dirName)).exists());

    FileSystemSchemaRegistryService.Config cfgBuilder = new FileSystemSchemaRegistryService.Config();
    cfgBuilder.setFallbackToResources(false);
    cfgBuilder.setSchemaDir(dirName);

    FileSystemSchemaRegistryService.build(cfgBuilder);
  }

  @Test(expectedExceptions=DatabusRuntimeException.class)
  /** Instantiation should fail with fallbackToResourceDisabled and a missing directory */
  public void testErrorOnEmptySchemasDir() throws IOException, InvalidConfigException
  {
    File tempDir = FileUtils.createTempDir("testErrorOnEmptySchemasDir");
    Assert.assertTrue(tempDir.exists());

    FileSystemSchemaRegistryService.Config cfgBuilder = new FileSystemSchemaRegistryService.Config();
    cfgBuilder.setFallbackToResources(false);
    cfgBuilder.setSchemaDir(tempDir.getAbsolutePath());

    FileSystemSchemaRegistryService.build(cfgBuilder);
  }
}
