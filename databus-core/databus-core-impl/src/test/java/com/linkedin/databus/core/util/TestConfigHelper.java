package com.linkedin.databus.core.util;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConfigHelper
{

  @Test
  public void testParseDuration() throws Exception
  {
    Assert.assertEquals(ConfigHelper.parseDuration("123", TimeUnit.MILLISECONDS), 123);
    Assert.assertEquals(ConfigHelper.parseDuration(" 123s", TimeUnit.MILLISECONDS), 123000);
    Assert.assertEquals(ConfigHelper.parseDuration("10 MIN ", TimeUnit.MICROSECONDS), 10 * 60 * 1000000);
    Assert.assertEquals(ConfigHelper.parseDuration("70 Hr", TimeUnit.DAYS), 2);
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.DAYS), 2 );
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.HOURS), 2L * 24 );
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.MINUTES), 2L * 24 * 60);
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.SECONDS), 2L * 24 * 60 * 60);
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.MILLISECONDS),
                        2L * 24 * 60 * 60 * 1000);
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.MICROSECONDS),
                        2L * 24 * 60 * 60 * 1000 * 1000);
    Assert.assertEquals(ConfigHelper.parseDuration("2 days", TimeUnit.NANOSECONDS),
                        2L * 24 * 60 * 60 * 1000 * 1000 * 1000);
    Assert.assertEquals(ConfigHelper.parseDuration("70ms", TimeUnit.NANOSECONDS), 70000000);
    Assert.assertEquals(ConfigHelper.parseDuration("1hr", TimeUnit.MILLISECONDS), 1L * 60 * 60 * 1000);
  }

  @Test
  public void testParseDurationInvalid()
  {
    try
    {
      ConfigHelper.parseDuration("", TimeUnit.MILLISECONDS);
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseDuration("1month", TimeUnit.MILLISECONDS);
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseDuration("1minutae", TimeUnit.MILLISECONDS);
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseDuration("1.3hr", TimeUnit.MILLISECONDS);
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseDuration("-2", TimeUnit.MILLISECONDS);
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
  }

  @Test
  public void testParseByteSize() throws Exception
  {
    Assert.assertEquals(ConfigHelper.parseByteSize("999"), 999L);
    Assert.assertEquals(ConfigHelper.parseByteSize("123k"), 123000L);
    Assert.assertEquals(ConfigHelper.parseByteSize("345K"), 345L * 1024);
    Assert.assertEquals(ConfigHelper.parseByteSize("666m"), 666000000L);
    Assert.assertEquals(ConfigHelper.parseByteSize(" 1000 M "), 1024L * 1024000L);
    Assert.assertEquals(ConfigHelper.parseByteSize("4g"), 4000000000L);
    Assert.assertEquals(ConfigHelper.parseByteSize("8G"), 8L * 1024L * 1024L * 1024L);
    Assert.assertEquals(ConfigHelper.parseByteSize("1t"), 1000000000000L);
    Assert.assertEquals(ConfigHelper.parseByteSize("10 T"), 10L * 1024L * 1024L * 1024L * 1024L);
    Assert.assertEquals(ConfigHelper.parseByteSize("10p"), 10000000000000000L);
    Assert.assertEquals(ConfigHelper.parseByteSize("9P"), 9L * 1024L * 1024L * 1024L * 1024L * 1024);
    Assert.assertEquals(ConfigHelper.parseByteSize(" 1e "), 1000000000000000000L);
    Assert.assertEquals(ConfigHelper.parseByteSize(" 1E"), 1L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L);
  }

  @Test
  public void testParseByteSizeInvalid()
  {
    try
    {
      ConfigHelper.parseByteSize("");
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseByteSize("1mega");
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseByteSize("100MB");
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseByteSize("1.5g");
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
    try
    {
      ConfigHelper.parseByteSize("-12k");
      Assert.fail();
    }
    catch (InvalidConfigException e) {/*OK*/}
  }

}
