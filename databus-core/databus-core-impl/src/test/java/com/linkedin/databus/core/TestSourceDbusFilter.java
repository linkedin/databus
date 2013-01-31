package com.linkedin.databus.core;
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


import static org.testng.AssertJUnit.*;

import java.util.HashSet;

import org.apache.log4j.Logger;

import org.testng.annotations.Test;

import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.filter.SourceDbusFilter;

public class TestSourceDbusFilter
{
  public static final String MODULE = TestSourceDbusFilter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  @Test
  public void testAllow()
  {

    HashSet<Integer> sources = new HashSet<Integer>();
    sources.add(23);
    sources.add(45);
    DbusEvent e = RngUtils.randomEvent((short) 23);
    LOG.debug(e);
    SourceDbusFilter sdFilter = new SourceDbusFilter(sources);
    assertTrue(sdFilter.allow(e));
  }

}
