package com.linkedin.databus2.schemas;

import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    Map<Short,String > bizfollowSchemas =
        service.fetchAllSchemaVersionsByType("com.linkedin.events.bizfollow.bizfollow.BizFollow");
    Assert.assertNotNull(bizfollowSchemas);
    Assert.assertTrue(2 <= bizfollowSchemas.size());

    String liarMemberSchema = service.fetchLatestSchemaByType("com.linkedin.events.liar.memberrelay.LiarMemberRelay");
    Assert.assertNotNull(liarMemberSchema);
  }
}
