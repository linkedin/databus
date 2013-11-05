package com.linkedin.databus2.core.container;

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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteOrder;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.annotations.Test;
import org.testng.Assert;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.test.TestUtil;


public class TestServerContainer
{
  static Logger LOG = Logger.getLogger(TestServerContainer.class);
  static {
    
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger log = Logger.getRootLogger();
    log.removeAllAppenders();
    log.addAppender(defaultAppender);
    log.setLevel(Level.WARN);
    LOG.setLevel(Level.INFO);
  }
  @Test
  public void testConfig() throws Exception
  {
    ServerContainer.Config config;
    ServerContainer.StaticConfig sConfig;
    {
      // Make sure that readTimeout defaults to the relay readTimeout settings as long as
      // the bootstrap's readTimeout is not set.
      config = new ServerContainer.Config();
      sConfig = config.build();
      long readTimeout = sConfig.getReadTimeoutMs();
      Assert.assertEquals(sConfig.getReadTimeoutMs(), sConfig.getBstReadTimeoutMs());
      config.setReadTimeoutMs(readTimeout+1);
      sConfig = config.build();
      Assert.assertEquals(readTimeout+1, sConfig.getReadTimeoutMs());
      Assert.assertEquals(readTimeout+1, sConfig.getBstReadTimeoutMs());
      config.setBstReadTimeoutMs(readTimeout+2);
      sConfig = config.build();
      Assert.assertEquals(readTimeout+1, sConfig.getReadTimeoutMs());
      Assert.assertEquals(readTimeout+2, sConfig.getBstReadTimeoutMs());
    }
  }
  
  @Test
  public void testContainerPort() throws Exception {
    ServerContainer.Config config = new ServerContainer.Config();
    config.getJmx().setRmiEnabled(false);
    ServerContainer.StaticConfig sConfig = config.build();
    int containerId = -1;
    int httpPort = -1;
    
    Assert.assertEquals(sConfig.getHttpPort(), 9000);
    
   
    MyServerContainer sc = new MyServerContainer(sConfig);
    Assert.assertEquals(sc.getHttpPort(), -1);
    Assert.assertEquals(sc.getBaseDir(), ".");
    try {
      sc.start();
      Assert.assertEquals(sc.getHttpPort(), 9000);
      TestUtil.checkServerRunning("localhost", sc.getHttpPort() , LOG);
      httpPort = readPort(sc); // read the persisted port
      Assert.assertNotEquals(httpPort, -1);
    } finally {
      sc.shutdown();
      sc = null;
    }
    LOG.info("container id = " + sConfig.getId());
    Assert.assertNotEquals(containerId, sConfig.getId());
    containerId = sConfig.getId();
    
    // now try with port 0
    config = new ServerContainer.Config();
    config.setContainerBaseDir("/tmp");
    config.getJmx().setRmiEnabled(false);
    config.setHttpPort(0);
    sConfig = config.build();
    
    Assert.assertEquals(sConfig.getHttpPort(), 0);
   
    sc = new MyServerContainer(sConfig);
    Assert.assertEquals(sc.getHttpPort(), -1);
    Assert.assertEquals(sc.getBaseDir(), "/tmp");
    try {
      sc.start();
      int newPort = sc.getHttpPort();
      TestUtil.checkServerRunning("localhost", newPort , LOG);
      Assert.assertNotEquals(newPort, 9000); // very theoretically it is possible, but not probable
      Assert.assertNotEquals(newPort, 0);
      Assert.assertEquals(newPort, readPort(sc));
      Assert.assertNotEquals(newPort, httpPort);    // should be different port
    } finally {
      sc.shutdown();
    }
    LOG.info("container id = " + sConfig.getId() + ";portFile=" + sc.getHttpPortFileName());
    Assert.assertNotEquals(containerId, sConfig.getId());
    Assert.assertEquals( sc.getHttpPortFileName(), "/tmp/containerPortNum_"+sConfig.getId());
  }
  
  /* read port number from a file */
  private int readPort(ServerContainer sc) {
    File file = new File(sc.getHttpPortFileName());
    FileReader fr;
    try {
      fr = new FileReader(file);
    } catch (FileNotFoundException e1) {
      return -1;
    }
    char [] cbuf = new char[10];
    int size = 0;
    try {
      size = fr.read(cbuf);
    } catch (IOException e) {
      return -1;
    }
    return Integer.parseInt(new String(cbuf, 0, size));
  }
  
  
  class MyServerContainer extends ServerContainer
  {
    MyServerContainer(ServerContainer.StaticConfig sConfig) throws InvalidConfigException, IOException, DatabusException
    {
      super(sConfig, ByteOrder.BIG_ENDIAN);
    }

    @Override
    protected DatabusComponentAdmin createComponentAdmin()
    {  
      return new DatabusComponentAdmin(this, null, "fake");  
    }

    public void start() {
      super.doStart();
    }
    @Override
    public void pause()  {}
    @Override
    public void resume() {}
    @Override
    public void suspendOnError(Throwable cause){}
  }
}
