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
package com.linkedin.databus2.ggParser.staxparser.validator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus2.test.TestUtil;

/**
 * Tests the XmlFormatTrailValidator class.
 */
public class TestXmlFormatTrailValidator
{
  private static String XML_ENCODING = "ISO-8859-1";

  public static String VALID_XML_FRAGMENT1 =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "      <column name=\"GG_STATUS\">o</column> \n" +
 "   </columns>\n" +
 "   <tokens>\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "     <token name=\"TK-CSN\">5992126505944</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "    </columns>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

  //bogus tag <I am bogus> in the first transactions TK-CSN
  public static String INVALID_XML_TAG =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "      <column name=\"GG_STATUS\">o</column> \n" +
 "   </columns>\n" +
 "   <tokens>\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "     <token name=\"TK-CSN\">5992126505944<I am bogus></token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

 //<columns> not closed in second transaction
  public static String INVALID_XML_FRAGMENT1 =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "      <column name=\"GG_STATUS\">o</column> \n" +
 "   </columns>\n" +
 "   <tokens>\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "     <token name=\"TK-CSN\">5992126505944</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

  /** <tokens> has a bogus attribute */
  public static String INVALID_DTD_FRAGMENT1 =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"no\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "      <column name=\"GG_STATUS\">o</column> \n" +
 "   </columns>\n" +
 "   <tokens a=\"1\">\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "      <token name=\"TK-CSN\">5992126505944</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "    </columns>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

  /** unrecognized element: <comments> */
  public static String INVALID_DTD_FRAGMENT2 =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"no\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "      <column name=\"GG_STATUS\">o</column> \n" +
 "   </columns>\n" +
 "   <tokens>\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "      <token name=\"TK-CSN\">5992126505944</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "    </columns>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 "   <comments />\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

  public static String MISSING_GG_STATUS_FRAGMENT =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "   </columns>\n" +
 "   <tokens>\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "     <token name=\"TK-CSN\">5992126505944</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "    </columns>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

  public static String MISSING_DELETE_GG_STATUS_FRAGMENT =
 "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
 "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"delete\">\n" +
 "    <columns>\n" +
 "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
 "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
 "   </columns>\n" +
 "   <tokens>\n" +
 "      <token name=\"TK-XID\">58.27.365982</token>\n" +
 "     <token name=\"TK-CSN\">5992126505944</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n" +
 "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
 "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
 "   <columns>\n" +
 "     <column name=\"ID\" key=\"true\">190894</column>\n" +
 "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
 "     <column name=\"GG_STATUS\">o</column>\n" +
 "    </columns>\n" +
 "   <tokens>\n" +
 "     <token name=\"TK-XID\">75.6.143099</token>\n" +
 "     <token name=\"TK-CSN\">5992126506801</token>\n" +
 "   </tokens>\n" +
 " </dbupdate>\n" +
 "</transaction>\n";

  public static String MISSING_XID_FRAGMENT =
      "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
          "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
          "    <columns>\n" +
          "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
          "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
          "      <column name=\"GG_STATUS\">o</column> \n" +
          "   </columns>\n" +
          "   <tokens>\n" +
          "      <token name=\"TK-XID\">58.27.365982</token>\n" +
          "     <token name=\"TK-CSN\">5992126505944</token>\n" +
          "   </tokens>\n" +
          " </dbupdate>\n" +
          "</transaction>\n" +
          "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
          "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
          "   <columns>\n" +
          "     <column name=\"ID\" key=\"true\">190894</column>\n" +
          "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
          "     <column name=\"GG_STATUS\">o</column>\n" +
          "    </columns>\n" +
          "   <tokens>\n" +
          "     <token name=\"TK-CSN\">5992126506801</token>\n" +
          "   </tokens>\n" +
          " </dbupdate>\n" +
          "</transaction>\n";

  public static String MISSING_SCN_FRAGMENT =
      "<transaction timestamp=\"2013-05-15:14:49:11.000000\">\n" +
          "  <dbupdate table=\"TESTDB1.TABLE_A\" type=\"insert\">\n" +
          "    <columns>\n" +
          "      <column name=\"KEY\" key=\"true\">1234</column> \n" +
          "      <column name=\"GG_MODI_TS\">2013-05-15:14:49:11.042776000</column> \n" +
          "      <column name=\"GG_STATUS\">o</column> \n" +
          "   </columns>\n" +
          "   <tokens>\n" +
          "      <token name=\"TK-XID\">58.27.365982</token>\n" +
          "     <token name=\"TK-CSN\">5992126505944</token>\n" +
          "   </tokens>\n" +
          " </dbupdate>\n" +
          "</transaction>\n" +
          "<transaction timestamp=\"2013-05-15:14:49:12.000000\">\n" +
          "  <dbupdate table=\"TESTDB2.TABLE_X\" type=\"insert\">\n" +
          "   <columns>\n" +
          "     <column name=\"ID\" key=\"true\">190894</column>\n" +
          "     <column name=\"GG_MODI_TS\">2013-05-15:14:49:12.462923000</column>\n" +
          "     <column name=\"GG_STATUS\">o</column>\n" +
          "    </columns>\n" +
          "   <tokens>\n" +
          "     <token name=\"TK-XID\">75.6.143099</token>\n" +
          "   </tokens>\n" +
          " </dbupdate>\n" +
          "</transaction>\n";

  @BeforeClass
  public void setUp() throws Exception
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestXmlFormatTrailValidator_", ".log",
                                             Level.ERROR);
  }

  @Test //(expectedExceptions = DatabusRuntimeException.class) - no longer true with DDSDBUS-2796 retry-fixes
  /** Tests the validator with invalid XML */
  public void testInvalidXmlTag() throws Exception
  {
    File tmpDir = createTestDir(INVALID_XML_TAG);
    XmlFormatTrailValidator validator = createValidator(tmpDir, false);
    Assert.assertFalse(validator.run());
  }

  @Test
  /** Tests the validator with invalid XML */
  public void testInvalidXml() throws Exception
  {
    File tmpDir = createTestDir(INVALID_XML_FRAGMENT1);
    XmlFormatTrailValidator validator = createValidator(tmpDir, false);
    Assert.assertFalse(validator.run());
  }

  @Test
  /** Tests the validator with valid XML */
  public void testValidXml() throws Exception
  {
    File tmpDir = createTestDir(VALID_XML_FRAGMENT1);
    XmlFormatTrailValidator validator = createValidator(tmpDir, true);
    Assert.assertTrue(validator.run());
  }

  //@Test
  //TODO Need to add the woodstox library to the repo for this to work
  /** Tests the validator with valid XML not conforming to the DTD and DTD validation on */
  public void testInvalidDtd1ValidationOn() throws Exception
  {
    File tmpDir = createTestDir(INVALID_DTD_FRAGMENT1);
    XmlFormatTrailValidator validator = createValidator(tmpDir, true);
    Assert.assertFalse(validator.run());
  }

  //@Test
  //TODO Need to add the woodstox library to the repo for this to work
  /** Tests the validator with valid XML not conforming to the DTD and DTD validation on */
  public void testInvalidDtd2ValidationOn() throws Exception
  {
    File tmpDir = createTestDir(INVALID_DTD_FRAGMENT2);
    XmlFormatTrailValidator validator = createValidator(tmpDir, true);
    Assert.assertFalse(validator.run());
  }

  @Test
  /** Tests the validator with valid XML not conforming to the DTD and DTD validation off */
  public void testInvalidDtd1ValidationOff() throws Exception
  {
    File tmpDir = createTestDir(INVALID_DTD_FRAGMENT1);
    XmlFormatTrailValidator validator = createValidator(tmpDir, true);
    Assert.assertTrue(validator.run());
  }

  @Test
  /** Tests the validator with valid XML not conforming to the DTD and DTD validation off */
  public void testInvalidDtd2ValidationOff() throws Exception
  {
    File tmpDir = createTestDir(VALID_XML_FRAGMENT1, INVALID_DTD_FRAGMENT2);
    XmlFormatTrailValidator validator = createValidator(tmpDir, true);
    Assert.assertTrue(validator.run());
  }

  @Test
  /** Tests the validator with valid XML having invalid characters*/
  public void testInvalidCharacters() throws Exception
  {
    byte[] file1 = VALID_XML_FRAGMENT1.getBytes(XML_ENCODING);
    byte[] file2 = VALID_XML_FRAGMENT1.getBytes(XML_ENCODING);
    file2[30] = (byte)1;

    File tmpDir = createTestDir(file1, file2);
    XmlFormatTrailValidator validator = createValidator(tmpDir, true);
    Assert.assertFalse(validator.run());
  }

  @Test
  /** Tests the validator with missing GG_STATUS column */
  public void testMissingGGStatus() throws Exception
  {
    File tmpDir = createTestDir(MISSING_GG_STATUS_FRAGMENT);
    XmlFormatTrailValidator validator = createValidator(tmpDir, false);
    Assert.assertFalse(validator.run());
  }

  @Test
  /** Tests the validator with missing GG_STATUS column in a DELETE; this should not fail*/
  public void testMissingGGStatusInDelete() throws Exception
  {
    File tmpDir = createTestDir(MISSING_DELETE_GG_STATUS_FRAGMENT);
    XmlFormatTrailValidator validator = createValidator(tmpDir, false);
    Assert.assertTrue(validator.run());
  }

  @Test
  /** Tests the validator with missing TK-XID column */
  public void testMissingXid() throws Exception
  {
    File tmpDir = createTestDir(MISSING_XID_FRAGMENT);
    XmlFormatTrailValidator validator = createValidator(tmpDir, false);
    Assert.assertFalse(validator.run());
  }

  @Test
  /** Tests the validator with missing TK-SCN column */
  public void testMissingScn() throws Exception
  {
    File tmpDir = createTestDir(MISSING_SCN_FRAGMENT);
    XmlFormatTrailValidator validator = createValidator(tmpDir, false);
    Assert.assertFalse(validator.run());
  }

  private static XmlFormatTrailValidator createValidator(File trailDir,
                                                         boolean dtdValidationEnabled)
          throws Exception
  {
    return new XmlFormatTrailValidator(trailDir.getAbsolutePath(), "x", false, dtdValidationEnabled, null);
  }

  public static File createTestDir(String ...filesContents) throws IOException
  {
    byte[][] contentBytes = new byte[filesContents.length][];
    for (int i = 0; i < filesContents.length; ++i)
    {
      contentBytes[i] = filesContents[i].getBytes(XML_ENCODING);
    }
    return createTestDir(contentBytes);
  }

  public static File createTestDir(byte[] ...filesContents) throws IOException
  {
    File tmpDir = FileUtils.createTempDir("TestXmlFormatTrailValidator");
    for (int i = 0; i < filesContents.length; ++i)
    {
      String fileName = String.format("x%06d", i);
      File trailFile = new File(tmpDir, fileName);
      trailFile.deleteOnExit();
      FileOutputStream trailOS = new FileOutputStream(trailFile);
      trailOS.write(filesContents[i]);
      trailOS.close();
    }

    return tmpDir;
  }
}
