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
package com.linkedin.databus2.ggParser.XmlStateMachine;

import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.avro.Schema.Field;
import org.testng.annotations.Test;

import com.linkedin.databus2.core.DatabusException;

/**
 * Tests the XmlFormatTrailValidator class.
 */
public class TestPrimaryKey
{
  /** Tests the validator with missing TK-SCN column */
  @Test
  public void testSimpleKeyParsing() throws DatabusException
  {
    String pkStr = "anetId";
    ArrayList<String> pkStrList =  new ArrayList<String>();
    pkStrList.add("anetId");

    Field sf = new Field("anetId", null, null, null);
    Assert.assertEquals(sf.name(), "anetId");

    Field sf2 = new Field("settings", null, null, null);
    Assert.assertEquals(sf2.name(), "settings");

    PrimaryKey pk = new PrimaryKey(pkStr);
    Assert.assertFalse(pk.isCompositeKey());
    Assert.assertEquals(pk.getNumKeys(), 1);
    Assert.assertEquals(pk.getPKeyList(), pkStrList);
    Assert.assertEquals(pk.isPartOfPrimaryKey(sf), true);
    Assert.assertEquals(pk.isPartOfPrimaryKey(sf2), false);
  }

  /** Tests the validator with missing TK-SCN column */
  @Test
  public void testCompositeKeyParsing() throws DatabusException
  {
    String pkStr = "anetId,settingId";
    ArrayList<String> pkStrList =  new ArrayList<String>();
    pkStrList.add("anetId");
    pkStrList.add("settingId");

    Field sf = new Field("anetId", null, null, null);
    Assert.assertEquals(sf.name(), "anetId");

    Field sf2 = new Field("settingId", null, null, null);
    Assert.assertEquals(sf2.name(), "settingId");

    Field sf3 = new Field("junkId", null, null, null);
    Assert.assertEquals(sf3.name(), "junkId");

    PrimaryKey pk = new PrimaryKey(pkStr);
    Assert.assertTrue(pk.isCompositeKey());
    Assert.assertEquals(pk.getNumKeys(), 2);
    Assert.assertEquals(pk.getPKeyList(), pkStrList);

    Assert.assertEquals(pk.isPartOfPrimaryKey(sf), true);
    Assert.assertEquals(pk.isPartOfPrimaryKey(sf2), true);
    Assert.assertEquals(pk.isPartOfPrimaryKey(sf3), false);
  }

  @Test
  public void testTrailingSpacesInPK() throws DatabusException
  {
    String pkStr = " anetId, settingId   ";
    ArrayList<String> pkStrList =  new ArrayList<String>();
    pkStrList.add("anetId");
    pkStrList.add("settingId");

    Field sf = new Field("anetId  ", null, null, null);
    Assert.assertEquals(sf.name(), "anetId  ");

    Field sf2 = new Field(" settingId ", null, null, null);
    Assert.assertEquals(sf2.name(), " settingId ");

    PrimaryKey pk = new PrimaryKey(pkStr);
    Assert.assertTrue(pk.isCompositeKey());
    Assert.assertEquals(pk.getNumKeys(), 2);
    Assert.assertEquals(pk.getPKeyList(), pkStrList);

    Assert.assertEquals(pk.isPartOfPrimaryKey(sf), true);
    Assert.assertEquals(pk.isPartOfPrimaryKey(sf2), true);

    Assert.assertTrue(PrimaryKey.isPrimaryKey(pkStr, sf));
    Assert.assertTrue(PrimaryKey.isPrimaryKey(pkStr, sf2));
  }
}
