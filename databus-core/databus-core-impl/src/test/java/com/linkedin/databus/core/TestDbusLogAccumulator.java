package com.linkedin.databus.core;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusLogAccumulator.DebugMessage;
import com.linkedin.databus2.test.TestUtil;

public class TestDbusLogAccumulator
{
  @BeforeClass
  public void setUpClass()
  {
    TestUtil.setupLogging(true, null, Level.INFO);
  }


  @Test
  public void testDbusLogAccumulator()
  {
    DbusLogAccumulator acc = new DbusLogAccumulator(3);
    Logger log = Logger.getLogger(TestDbusLogAccumulator.class);
    MyLogger mlog = new MyLogger(TestDbusLogAccumulator.class.getName());
    mlog.setLogger(log);

    acc.addMessage(new DebugMessage("MSG %d", 1));
    acc.addMessage(new DebugMessage("MSG %s", "TWO"));
    acc.addMessage(new DebugMessage("MSG 3"));
    acc.addMessage(new DebugMessage("MSG %d", 4));
    acc.addMessage(new DebugMessage("MSG %s", "FIVE"));
    acc.addMessage(new DebugMessage("MSG 6"));
    acc.addMessage("MSG 7");

    Assert.assertEquals(7, acc.getTotalNumberOfMessages());
    Assert.assertEquals(3, acc.getNumberOfMessages());

    acc.prettyLog(mlog, Level.ERROR);
    Assert.assertFalse(mlog.verifyError("MSG 1"));
    Assert.assertFalse(mlog.verifyError("MSG TWO"));
    Assert.assertFalse(mlog.verifyError("MSG 3"));
    Assert.assertFalse(mlog.verifyError("MSG 4"));
    Assert.assertTrue(mlog.verifyError("MSG FIVE"));
    Assert.assertTrue(mlog.verifyError("MSG 6"));
    Assert.assertTrue(mlog.verifyError("MSG 7"));

    acc.reset();

    mlog.setLevel(Level.DEBUG);
    acc.addMessage(new DebugMessage("MSG1 %d", 1));
    acc.addMessage(new DebugMessage("MSG1 %s", "TWO"));
    acc.addMessage(new DebugMessage("MSG1 3"));
    acc.addMessage(new DebugMessage("MSG1 %d", 4));
    acc.addMessage(new DebugMessage("MSG1 %s", "FIVE"));
    acc.addMessage(new DebugMessage("MSG1 6"));
    acc.addMessage("MSG1 7");

    Exception e = new Exception("Exception 1");
    acc.addMessage(new DebugMessage("MSG1 %d", 8).setException(e));

    Assert.assertEquals(8, acc.getTotalNumberOfMessages());
    Assert.assertEquals(3, acc.getNumberOfMessages());



    acc.prettyLog(mlog, Level.INFO);

    Assert.assertFalse(mlog.verifyInfo("MSG1 1"));
    Assert.assertFalse(mlog.verifyInfo("MSG1 TWO"));
    Assert.assertFalse(mlog.verifyInfo("MSG1 3"));
    Assert.assertFalse(mlog.verifyInfo("MSG1 4"));
    Assert.assertFalse(mlog.verifyInfo("MSG1 FIVE"));
    Assert.assertTrue(mlog.verifyInfo("MSG1 6"));
    Assert.assertTrue(mlog.verifyInfo("MSG1 7"));
    Assert.assertTrue(mlog.verifyInfo("MSG1 8. Exception message = java.lang.Exception: Exception 1. Exception cause = null"));
  }

  public static class MyLogger extends Logger {

    private final List<String> allErrorMsg;
    private final List<String> allWarnMsg;
    private final List<String> allInfoMsg;
    private Logger _log;


    public MyLogger(String name) {
      super(name);
      allErrorMsg = new ArrayList<String>(32);
      allWarnMsg = new ArrayList<String>(32);
      allInfoMsg = new ArrayList<String>(32);
    }
    public void setLogger(Logger log) {
      _log = log;
    }

    @Override
    public void error(Object msg) {
      allErrorMsg.add((String)msg);
      _log.error(msg);
    }
    @Override
    public void info(Object msg) {
      allInfoMsg.add((String)msg);
      _log.info(msg);
    }
    @Override
    public void warn(Object msg) {
      allWarnMsg.add((String)msg);
      _log.warn(msg);
    }

    @Override
    public boolean isDebugEnabled() {
      return _log.isDebugEnabled();
    }

    public boolean verifyError(String msg) {
      return allErrorMsg.contains(msg);
    }
    public boolean verifyInfo(String msg) {
      return allInfoMsg.contains(msg);
    }
    public boolean verifyWarn(String msg) {
      return allWarnMsg.contains(msg);
    }
  }
}
