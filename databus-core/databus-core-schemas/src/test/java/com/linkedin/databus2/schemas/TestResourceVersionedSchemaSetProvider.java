package com.linkedin.databus2.schemas;

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
