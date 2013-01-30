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


import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceVersionedSchemaSetProvider
{

  @Test
  /**
   * NOTE: if this test is failing when run in Eclipse, run it once from the command to populate
   * the resources/ directory.
   * */
  public void testStandardSchema() throws Exception
  {
    ResourceVersionedSchemaSetProvider resourceProvider = new ResourceVersionedSchemaSetProvider(this.getClass().getClassLoader());
    VersionedSchemaSet schemaSet = resourceProvider.loadSchemas();

    VersionedSchema bizfollowSchema = schemaSet.getLatestVersionByName("com.linkedin.events.bizfollow.bizfollow.BizFollow");
    Assert.assertNotNull(bizfollowSchema);
    Assert.assertTrue(bizfollowSchema.getVersion() >= 2, "version >= 2: " + bizfollowSchema.getVersion());

    VersionedSchema liarJobSchema = schemaSet.getLatestVersionByName("com.linkedin.events.liar.jobrelay.LiarJobRelay");
    Assert.assertNotNull(liarJobSchema);

    VersionedSchema fakeSchema = schemaSet.getLatestVersionByName("fake.schema");
    Assert.assertTrue(null == fakeSchema);
  }

}
