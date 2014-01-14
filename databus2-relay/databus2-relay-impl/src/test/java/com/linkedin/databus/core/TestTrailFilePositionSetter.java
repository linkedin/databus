/*
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
 */

package com.linkedin.databus.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.TrailFilePositionSetter.FileFilter;
import com.linkedin.databus.core.TrailFilePositionSetter.FilePositionResult;
import com.linkedin.databus.core.TrailFilePositionSetter.FilePositionResult.Status;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.producers.db.GGXMLTrailTransactionFinder;
import com.linkedin.databus2.test.TestUtil;

public class TestTrailFilePositionSetter
{
  public static final String MODULE = TestTrailFilePositionSetter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final String TRAIL_FILENAME_PREFIX = "x3";
  private static final String[] _txnPattern =
    {
      "<transaction timestamp=\"2013-03-09:02:54:34.000000\">",
      " <dbupdate table=\"TASKMGR.TASKCTL_JOBS_1\" type=\"update\">",
      "   <columns>",
      "     <column name=\"JOB_ID\" key=\"true\">1621745</column>",
      "     <column name=\"GG_MODI_TS\">2013-03-09:02:54:33.996072000</column>",
      "     <column name=\"GG_STATUS\">o</column>",
      "   </columns>",
      "   <tokens>",
      "     <token name=\"TK-XID\">42.8.2681282</token>",
      "     <token name=\"TK-CSN\">${SCN}</token>",
      "   </tokens>",
      " </dbupdate>",
      " <dbupdate table=\"TASKMGR.TASKCTL_JOBS_2\" type=\"update\">",
      "   <columns>",
      "     <column name=\"JOB_ID\" key=\"true\">1621745</column>",
      "     <column name=\"GG_MODI_TS\">2013-03-09:02:54:33.996072000</column>",
      "     <column name=\"GG_STATUS\">o</column>",
      "   </columns>",
      "   <tokens>",
      "     <token name=\"TK-XID\">42.8.2681283</token>",
      "     <token name=\"TK-CSN\">${SCN}</token>",
      "   </tokens>",
      " </dbupdate>",
      "</transaction>"
   }; // currently 24 lines; if startLine > 0, probably want it less than this
  private static final String DONE_STRING =
      "done\n----------------------------------------------------------------------------\n";

  private ArrayList<File> _dirsToDelete = new ArrayList<File>(100);


  private BufferedWriter createWriter(String dir, String file) throws IOException
  {
    BufferedWriter w = new BufferedWriter(new FileWriter(new File(dir + "/" + file)));
    return w;
  }

  private void createTrailFiles(String dir, String prefix, long numTxns, long numLinesPerFile,
                                long numLinesPerNewline, String newlineChar, int startLine,
                                long corruptedScn, String corruption,
                                boolean addAlternateCorruption, String altCorruption)
    throws IOException
  {
    HashSet<Long> corruptedScns = new HashSet<Long>(1);
    corruptedScns.add(new Long(corruptedScn));
    createTrailFiles(dir, prefix, numTxns, numLinesPerFile, numLinesPerNewline, newlineChar,
                     startLine, corruptedScns, corruption, addAlternateCorruption, altCorruption);
  }

  private void createTrailFiles(String dir,
                                String prefix,
                                long numTxns,
                                long numLinesPerFile,
                                long numLinesPerNewline,
                                String newlineChar,
                                int startLine,
                                Set<Long> corruptedScns,
                                String corruption,
                                boolean addAlternateCorruption,
                                String altCorruption) throws IOException
  {
    createTrailFiles(dir,
                     prefix,
                     numTxns,
                     numLinesPerFile,
                     numLinesPerNewline,
                     newlineChar,
                     startLine,
                     corruptedScns,
                     corruption,
                     addAlternateCorruption,
                     altCorruption,
                     _txnPattern,
                     1,
                     100);
  }

  private void createTrailFiles(String dir,
                                String prefix,
                                long numTxns,
                                long numLinesPerFile,
                                long numLinesPerNewline,
                                String newlineChar,
                                int startLine,
                                String[] txnPattern,
                                int numDbUpdatesWithSameScn,
                                long startScn) throws IOException
  {
    createTrailFiles(dir,
                     prefix,
                     numTxns,
                     numLinesPerFile,
                     numLinesPerNewline,
                     newlineChar,
                     startLine,
                     new HashSet<Long>(),
                     "",
                     false,
                     "",
                     txnPattern,
                     numDbUpdatesWithSameScn,
                     startScn);
  }

  private void createTrailFiles(String dir, String prefix, long numTxns, long numLinesPerFile,
          long numLinesPerNewline, String newlineChar, int startLine,
          Set<Long> corruptedScns, String corruption,
          boolean addAlternateCorruption, String altCorruption, String[] txnPattern, int numDbUpdatesWithSameScn, long currScn)
      throws IOException
 {
    long numFiles = ((numTxns * (txnPattern.length))/numLinesPerFile) + 1;
    long numDigits = new Double(Math.log10(numFiles)).longValue() + 1;
    long currFileNum = 0;
    String currFile = prefix + toFixedLengthString(currFileNum, numDigits);
    long lineCount = 0;
    BufferedWriter w = createWriter(dir, currFile);
    int start = startLine;
    int dbUpdates = 0;
    for (long txn = 0; txn < numTxns; ++txn)
    {
      boolean corruptNextTokensEndTag = false;
      if (txn > 0)
        start = 0;
      for (int j = 0 ; j < txnPattern.length; ++j)
      {
        lineCount++;
        String txnLine = txnPattern[j];
        if (txnLine.contains("${SCN}"))
        {
          dbUpdates++;
          txnLine = txnLine.replace("${SCN}", new Long(currScn).toString() + (corruptedScns.contains(currScn)? corruption : ""));
          if (addAlternateCorruption && corruptedScns.contains(currScn))
            corruptNextTokensEndTag = true;
          if (dbUpdates >= numDbUpdatesWithSameScn)
          {
            currScn++;
            dbUpdates = 0;
          }
        }
        if (corruptNextTokensEndTag && txnLine.contains("</tokens>"))
        {
          //txnLine = txnLine.append(newlineChar).append("   ").append(altCorruption);
          txnLine = txnLine + newlineChar + "   " + altCorruption;
          corruptNextTokensEndTag = false;
        }

        if (j >= start)
        {
          w.append(txnLine);

          if (lineCount%numLinesPerNewline == 0)
            w.append(newlineChar);
        }

        if ( (lineCount%numLinesPerFile) == 0)
        {
          w.close();
          currFileNum++;
          currFile = prefix + toFixedLengthString(currFileNum, numDigits);
          w = createWriter(dir, currFile);
        }
      }
    }

    if ( w != null)
      w.close();
  }

  private static String toFixedLengthString(Long num, long numDigits)
  {
    String n = num.toString();
    long numDigitsToBeZeroed = numDigits - n.length();
    StringBuilder bld = new StringBuilder();
    for ( int i = 0; i < numDigitsToBeZeroed; i++)
      bld.append('0');
    bld.append(n);
    return bld.toString();
  }

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    //set up logging
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestTrailFilePositionSetter_", ".log", Level.INFO);  // FATAL
  }

  @AfterClass  // optionally could do this after every test instead
  public void tearDownClass()
  {
    for (File dir : _dirsToDelete)
    {
      FileUtils.deleteQuietly(dir);
    }
    _dirsToDelete.clear();
  }

  private File createTempDir()
      throws IOException
  {
    File dir = File.createTempFile("dir_", null);

    if (!(dir.delete()))
      throw new IOException("Unable to delete temp file " + dir.getAbsolutePath());

    if (!dir.mkdir())
      throw new IOException("Unable to create tempDir: " + dir.getAbsolutePath());

    _dirsToDelete.add(dir);  // deleteOnExit() deletes only if dir empty => use Apache FileUtils helper instead

    return dir;
  }

  /**
   * DDSDBUS-2603 NUllPointerException in GG Relay in Experimental CLuster
   * @throws Exception
   */
  @Test
  public void testDeletedDirectory()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testDeletedDirectory");
    log.info("starting");

    File dir = createTempDir();

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();

    // Now delete the directory to make the locateFilePosition() see null for listFiles() call.
    boolean deleted = dir.delete();
    Assert.assertTrue(deleted, "Deleted the trail directory successfully");

    FilePositionResult res = posSetter.locateFilePosition(100,finder);
    Assert.assertEquals(res.getStatus(), Status.NO_TXNS_FOUND, "File Position Result Status");

    log.info(DONE_STRING);
  }

  /**
   * Verify that corruption that occurs early in the file causes USE_EARLIEST_SCN to return the
   * first SCN of the first uncorrupted transaction.
   */
  @Test
  public void testFailureMode_MalformedFirstTransactionInFirstFile()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testFailureMode_MalformedFirstTransactionInFirstFile");
    log.info("starting");

    File dir = createTempDir();

    // first SCN is hardcoded always to be 100
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX, 150 /* numTxns, 24 lines each */,
                     1250 /* numLinesPerFile */, 1 /* numLinesPerNewline */, "\n", 0,
                     100 /* corrupt first SCN */, "xyzzy", false, "");

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    FilePositionResult res;

    // SCN 100 is corrupted, so 101 is the effective oldest SCN => 100 treated as error:
    res = posSetter.locateFilePosition(100, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                        "expected error for exact-match SCN that's corrupted and oldest in all trail files.");

    // SCN 101 is OK (regexQuery() doesn't fully validate XML):
    finder.reset();
    res = posSetter.locateFilePosition(TrailFilePositionSetter.USE_EARLIEST_SCN, finder);
    assertFilePositionResult(res, dir, 101, FilePositionResult.Status.FOUND);

    log.info(DONE_STRING);
  }

  /**
   * Verify that corruption that occurs after the desired SCN/transaction doesn't cause errors
   * when requesting an exact SCN.
   */
  @Test
  public void testFailureMode_MalformedLastTransactionInMiddleFile()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testFailureMode_MalformedLastTransactionInMiddleFile");
    log.info("starting");

    File dir = createTempDir();

    // first SCN is hardcoded always to be 100
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX, 150 /* numTxns, 24 lines each */,
                     1250 /* numLinesPerFile */, 1 /* numLinesPerNewline */, "\n", 0,
                     307 /* corrupt last SCN in 2nd file */, "plugh", false, "");

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    FilePositionResult res;

    // corruption at SCN 307 occurs after SCN 299, so latter should be found OK:
    res = posSetter.locateFilePosition(299, finder);
    assertFilePositionResult(res, dir, 299, FilePositionResult.Status.FOUND);

    // SCN 306 is in same transaction as 307, but regexQuery() doesn't fully validate XML => OK
    finder.reset();
    res = posSetter.locateFilePosition(306, finder);
    assertFilePositionResult(res, dir, 306, FilePositionResult.Status.FOUND);

    log.info(DONE_STRING);
  }

  /**
   * Verify that corruption in the very last transaction causes USE_LATEST_SCN to return
   * the last SCN from the previous transaction.
   */
  @Test
  public void testFailureMode_MalformedLastTransactionInLastFile()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testFailureMode_MalformedLastTransactionInLastFile");
    log.info("starting");

    File dir = createTempDir();

    // first SCN is hardcoded always to be 100
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX, 150 /* numTxns, 24 lines and 2 SCNs each */,
                     1250 /* numLinesPerFile */, 1 /* numLinesPerNewline */, "\n", 0,
                     399 /* corrupt last SCN in 3rd file */, "quux", false, "");

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    FilePositionResult res;

    // corruption at SCN 399 => should get 398 back (same transaction, but again, regexQuery() doesn't fully validate)
    res = posSetter.locateFilePosition(TrailFilePositionSetter.USE_LATEST_SCN, finder);
    assertFilePositionResult(res, dir, 398, FilePositionResult.Status.FOUND);

    log.info(DONE_STRING);
  }

  /**
   * Verify that a non-corrupted SCN value within a corrupted transaction causes an exception to be thrown
   * when the exact SCN is requested.
   */
  @Test
  public void testFailureMode_MalformedTransactionForExactScn()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testFailureMode_MalformedTransactionForExactScn");
    log.info("starting");

    File dir = createTempDir();

    // first SCN is hardcoded always to be 100
    // this adds a non-XML string near the end of the dbupdate for SCN 377, but the SCN value itself is clean:
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX, 150 /* numTxns, 24 lines each */,
                     1250 /* numLinesPerFile */, 1 /* numLinesPerNewline */, "\n", 0,
                     377 /* corrupt a txn in 3rd file */, "", true, "kilroy");

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    FilePositionResult res;

    // with a full XML parser, this should throw an exception; with regexQuery(), it's fine:
    finder.reset();
    res = posSetter.locateFilePosition(377, finder);
    assertFilePositionResult(res, dir, 377, FilePositionResult.Status.FOUND);

    log.info(DONE_STRING);
  }

  /**
   * Verify that a transaction with corruption in all of its SCNs doesn't cause problems.
   * Case 1:  bad transaction at the beginning of the first trail file.
   * Case 2:  bad transaction at the beginning of the middle trail file (partial transaction).
   * Case 3:  bad transaction at the beginning of the middle trail file (first full transaction; preceding partial bad).
   * Case 4:  bad transaction in the middle of the middle trail file.
   * Case 5:  bad transaction at the beginning of the last trail file (first full transaction; preceding partial is OK).
   * Case 6:  bad transaction at the end of the last trail file.
   */
  @Test
  public void testFailureMode_MalformedTransactionNoValidScnsInMiddleOfMiddleFile()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testFailureMode_MalformedTransactionNoValidScns");
    log.info("starting");

    File dir = createTempDir();

    // corrupt both SCNs in each of six transactions:
    HashSet<Long> corruptedScns = new HashSet<Long>(10);
    corruptedScns.add(new Long(100));  // case 1
    corruptedScns.add(new Long(101));  // case 1
    corruptedScns.add(new Long(204));  // case 2
    corruptedScns.add(new Long(205));  // case 2
    corruptedScns.add(new Long(206));  // case 3
    corruptedScns.add(new Long(207));  // case 3
    corruptedScns.add(new Long(250));  // case 4
    corruptedScns.add(new Long(251));  // case 4
    corruptedScns.add(new Long(310));  // case 5
    corruptedScns.add(new Long(311));  // case 5
    corruptedScns.add(new Long(398));  // case 6
    corruptedScns.add(new Long(399));  // case 6
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX, 150 /* numTxns, 24 lines each */,
                     1250 /* numLinesPerFile */, 1 /* numLinesPerNewline */, "\n", 0,
                     corruptedScns, "blargh", false, "");

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    FilePositionResult res;

    // SCN 101 is before the earliest (valid) SCN present, so expect ERROR:
    res = posSetter.locateFilePosition(101, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                        "expected error for exact-match SCN that's 'too old'.");

    // For SCN <= the earliest transactions maxSCN, we throw error
    finder.reset();
    res = posSetter.locateFilePosition(102, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                      "expected error for exact-match SCN that's 'too old'.");

    // expect first non-corrupted SCN here, not first "transaction SCN":
    finder.reset();
    res = posSetter.locateFilePosition(TrailFilePositionSetter.USE_EARLIEST_SCN, finder);
    assertFilePositionResult(res, dir, 102, FilePositionResult.Status.FOUND);

    // 107 = max SCN of its transaction = "transaction SCN" => should get FOUND
    finder.reset();
    res = posSetter.locateFilePosition(107, finder);
    assertFilePositionResult(res, dir, 107, FilePositionResult.Status.FOUND);

    // 203 = last valid SCN in first file = max SCN of its transaction = "transaction SCN"
    // => should be FOUND
    finder.reset();
    res = posSetter.locateFilePosition(203, finder);
    assertFilePositionResult(res, dir, 203, FilePositionResult.Status.FOUND);

    // SCN 204 is invalid and is part of a transaction split across first/second files;
    // 209 = next "transaction SCN" and is near the top of the middle file
    finder.reset();
    res = posSetter.locateFilePosition(204, finder);
    assertFilePositionResult(res, dir, 209, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 250 is invalid (as is 251); expect 253 since max SCN of following transaction
    finder.reset();
    res = posSetter.locateFilePosition(250, finder);
    assertFilePositionResult(res, dir, 253, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 251 is invalid (as is 250); expect 253 since max SCN of following transaction
    finder.reset();
    res = posSetter.locateFilePosition(251, finder);
    assertFilePositionResult(res, dir, 253, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 252 is valid and present, but weird corner case => still EXACT_SCN_NOT_FOUND
    finder.reset();
    res = posSetter.locateFilePosition(252, finder);
    assertFilePositionResult(res, dir, 252, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 253 is valid and present and max SCN of its transaction => FOUND
    finder.reset();
    res = posSetter.locateFilePosition(253, finder);
    assertFilePositionResult(res, dir, 253, FilePositionResult.Status.FOUND);

    // SCN 309 is valid and present and max SCN of its transaction => FOUND (even though
    // split across second/third files, and following transaction is corrupted)
    finder.reset();
    res = posSetter.locateFilePosition(309, finder);
    assertFilePositionResult(res, dir, 309, FilePositionResult.Status.FOUND);

    // SCN 310 is invalid (as is 311); expect 313
    finder.reset();
    res = posSetter.locateFilePosition(310, finder);
    assertFilePositionResult(res, dir, 313, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 311 is invalid (as is 310); expect 313
    finder.reset();
    res = posSetter.locateFilePosition(311, finder);
    assertFilePositionResult(res, dir, 313, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 398 is invalid (as is 399) and is in last transaction of last file, but since
    // trail file is expected to continue growing (i.e., eventually to have a valid SCN
    // that's larger than the request), expect EXACT_SCN_NOT_FOUND rather than ERROR.  SCN
    // returned will be that of last valid transaction, i.e., 397.
    // [checks beginning of last valid transaction == 396/397 one at byte offset 35650]
    finder.reset();
    res = posSetter.locateFilePosition(398, finder);
    assertFilePositionResult(res, dir, 397, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // SCN 405 is completely missing (would be after last transaction of last file); expect
    // same behavior as previous case
    finder.reset();
    res = posSetter.locateFilePosition(405, finder);
    assertFilePositionResult(res, dir, 397, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // last valid transaction-SCN is 397
    finder.reset();
    res = posSetter.locateFilePosition(TrailFilePositionSetter.USE_LATEST_SCN, finder);
    assertFilePositionResult(res, dir, 397, FilePositionResult.Status.FOUND);

    log.info(DONE_STRING);
  }

  /**
   * Verify that requesting an SCN that's older than the oldest trail file causes ERROR
   * to be returned.  Also check various valid states:  SCN that isn't the max for its
   * transaction; SCN that is the max for its transaction; earliest SCN; etc.
   */
  @Test
  public void testRequestedScnOlderThanOldestTransaction()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testRequestedScnOlderThanOldestTransaction");
    //log.setLevel(Level.DEBUG);
    log.info("starting");

    File dir = createTempDir();

    // first SCN is hardcoded always to be 100:
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX, 150 /* numTxns, 24 lines each */,
                     1250 /* numLinesPerFile */, 1 /* numLinesPerNewline */, "\n", 0,
                     -1 /* no corruption */, "", false, "");

    TrailFilePositionSetter posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    FilePositionResult res;

    // SCN 99 is before the earliest SCN present in the trail files (100), so expect ERROR:
    res = posSetter.locateFilePosition(99, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                        "expected error for exact-match SCN that's too old.");

    // "Transaction-SCN" semantics:  max SCN is 121 in this transaction and 119 in the
    // previous one, so those are the values used for most of the "is it found?" logic.
    // Since 120 falls between the two but doesn't equal either one, it's not considered
    // to be found exactly (even though it's actually present), but the state is still
    // valid, so processing can continue (i.e., it doesn't matter).
    finder.reset();
    res = posSetter.locateFilePosition(120, finder);
    assertFilePositionResult(res, dir, 120, FilePositionResult.Status.EXACT_SCN_NOT_FOUND);

    // For SCN <= the earliest transactions maxSCN, we throw error
    finder.reset();
    res = posSetter.locateFilePosition(100, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                          "expected error for exact-match SCN that's too old.");

    // Related, weird corner case:  USE_EARLIEST_SCN uses the min SCN in the oldest transaction,
    // so even though an exact match for 100 doesn't return FOUND, this does:
    finder.reset();
    res = posSetter.locateFilePosition(TrailFilePositionSetter.USE_EARLIEST_SCN, finder);
    assertFilePositionResult(res, dir, 100, FilePositionResult.Status.FOUND);

    // For SCN <= the earliest transactions maxSCN, we throw error
    finder.reset();
    res = posSetter.locateFilePosition(101, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                                  "expected error for exact-match SCN that's too old.");

    log.info(DONE_STRING);
  }

  @Test
  public void testScnPositionLocator()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testScnPositionLocator");
    log.info("starting");

    testScnPositionLocator(0,100,200,102, log); // case where all txn are complete

    log.info(DONE_STRING);
  }

  private void testScnPositionLocator(int startLine, int startScn, int endScn, int beginFoundScn, Logger log)
      throws Exception
  {
    File dir = createTempDir();
    // TODO/FIXME:  why are startScn and endScn being used for numTxns and numLinesPerFile, respectively?
    createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX,startScn, endScn,1,"\n",startLine, -1, "", false, "");
    log.info("Directory is: " + dir);

    TrailFilePositionSetter posSetter = null;
    //GoldenGateTransactionSCNFinder finder;
    GGXMLTrailTransactionFinder finder;

    //less than minScn
    for (long i = 0 ; i < beginFoundScn ; i ++)
    {
      posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
      //finder = new GoldenGateTransactionSCNFinder();
      finder = new GGXMLTrailTransactionFinder();

      FilePositionResult res = posSetter.locateFilePosition(i,finder);
      Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                          "Result Status for SCN: " + i + ", Result: " + res);
    }

    //Found Case
    for (long i = beginFoundScn ; i < (startScn + endScn) ; i ++)
    {
      posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
      //finder = new GoldenGateTransactionSCNFinder();
      finder = new GGXMLTrailTransactionFinder();

      FilePositionResult res = posSetter.locateFilePosition(i,finder);
      log.info("For scn (" + i + "):  the result is:  "  + res);
      if (i%2 == 0)
        assertFilePositionResult(res,dir,i+1,FilePositionResult.Status.EXACT_SCN_NOT_FOUND);
      else
        assertFilePositionResult(res,dir,i,FilePositionResult.Status.FOUND);
    }

    //Found Case
    for (long i = (startScn + endScn) ; i < (startScn + endScn) + 20 ; i ++)
    {
      posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
      //finder = new GoldenGateTransactionSCNFinder();
      finder = new GGXMLTrailTransactionFinder();

      FilePositionResult res = posSetter.locateFilePosition(i,finder);
      log.info("For scn (" + i + "):  the result is:  "  + res);
      assertFilePositionResult(res,dir,299,FilePositionResult.Status.EXACT_SCN_NOT_FOUND);
    }
  }

  @Test
  public void testRepeatSCN() throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testRepeatSCN");
    File dir = createTempDir();
    long startScn = 100;
    int numTxnsPerFile = 10;
    /**
     * Create trail files with the following SCN range
     *
     * Trail-File       Number of Txns          SCN Range
     *   x300               10                  100-100
     *   x301               10                  100-100
     *   x302               10                  101-101
     *   x303               10                  101-101
     *   x304               10                  102-102
     *   x305               10                  102-102
     *   x306               10                  103-103
     *   x307               10                  103-103
     *   x308               10                  104-104
     *   x309               10                  104-104
     *
     */
    createTrailFiles(dir.getAbsolutePath(),
                     TRAIL_FILENAME_PREFIX,
                     100L,
                     _txnPattern.length * numTxnsPerFile,
                     1L,
                     "\n",
                     0,
                     _txnPattern,
                     2 * numTxnsPerFile * 2, // Transactions in 2 files have the same SCN
                     startScn);
    log.info("Directory is: " + dir);

    TrailFilePositionSetter posSetter = null;
    // GoldenGateTransactionSCNFinder finder = new GoldenGateTransactionSCNFinder();
    GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();
    posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);

    // SCN 100 is not found because this is the first SCN in the trail file.
    FilePositionResult res = posSetter.locateFilePosition(100, finder);
    Assert.assertEquals(res.getStatus(),
                        FilePositionResult.Status.ERROR,
                        "Result Status for SCN: " + 100 + ", Result: " + res);

    // SCN 101 is found
    res = posSetter.locateFilePosition(101, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.FOUND, "Result Status");
    Assert.assertEquals(res.getTxnPos().getFile(), "x302", "File found");
    Assert.assertEquals(res.getTxnPos().getFileOffset(), 0, "File offset found");
    Assert.assertEquals(res.getTxnPos().getLineNumber(), 1, "File line number found");
    Assert.assertEquals(res.getTxnPos().getMinScn(), 101, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getMaxScn(), 101, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getTxnRank(), 10, "Txn Rank");

    // SCN 102 is found
    res = posSetter.locateFilePosition(102, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.FOUND, "Result Status");
    Assert.assertEquals(res.getTxnPos().getFile(), "x304", "File found");
    Assert.assertEquals(res.getTxnPos().getFileOffset(), 0, "File offset found");
    Assert.assertEquals(res.getTxnPos().getLineNumber(), 1, "File line number found");
    Assert.assertEquals(res.getTxnPos().getMinScn(), 102, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getMaxScn(), 102, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getTxnRank(), 10, "Txn Rank");

    // SCN 103 is found
    res = posSetter.locateFilePosition(103, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.FOUND, "Result Status");
    Assert.assertEquals(res.getTxnPos().getFile(), "x306", "File found");
    Assert.assertEquals(res.getTxnPos().getFileOffset(), 0, "File offset found");
    Assert.assertEquals(res.getTxnPos().getLineNumber(), 1, "File line number found");
    Assert.assertEquals(res.getTxnPos().getMinScn(), 103, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getMaxScn(), 103, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getTxnRank(), 10, "Txn Rank");

    // SCN 104 is found
    res = posSetter.locateFilePosition(104, finder);
    Assert.assertEquals(res.getStatus(), FilePositionResult.Status.FOUND, "Result Status");
    Assert.assertEquals(res.getTxnPos().getFile(), "x308", "File found");
    Assert.assertEquals(res.getTxnPos().getFileOffset(), 0, "File offset found");
    Assert.assertEquals(res.getTxnPos().getLineNumber(), 1, "File line number found");
    Assert.assertEquals(res.getTxnPos().getMinScn(), 104, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getMaxScn(), 104, "MinScn check");
    Assert.assertEquals(res.getTxnPos().getTxnRank(), 10, "Txn Rank");
  }

  @Test
  public void testScnPositionSetter1()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testScnPositionSetter1");
    log.info("starting");

    testScnPositionSetter(0,100,200,100, log); // case where all txn are complete
    testScnPositionSetter(10,100,200,102, log); // case where initial txn is incomplete

    log.info(DONE_STRING);
  }

  private void testScnPositionSetter(int startLine, int startScn, int endScn, int beginFoundScn, Logger log)
    throws Exception
  {
    String[] newLines = { "\n", "\r", "\r\n" };

    for (String newLine : newLines)
    {
      File dir = createTempDir();
      createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX,startScn, endScn,1,newLine,startLine, -1, "", false, "");
      log.info("Directory is: " + dir);

      TrailFilePositionSetter posSetter = null;
      //GoldenGateTransactionSCNFinder finder = new GoldenGateTransactionSCNFinder();
      GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();

      //less than minScn
      for (long i = 0 ; i < beginFoundScn ; i ++)
      {
        posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
        Logger log2 = posSetter._log;
        log2.setLevel(Level.INFO);
        log2.info("Created a TrailFilePositionSetter with suffix x3");
        //finder = new GoldenGateTransactionSCNFinder();
        finder = new GGXMLTrailTransactionFinder();

        FilePositionResult res = posSetter.getFilePosition(i,finder);
        Assert.assertEquals(res.getStatus(), FilePositionResult.Status.ERROR,
                            "Result Status for SCN: " + i + ", Result: " + res);
      }

      //Found Case
      for (long i = beginFoundScn ; i < (startScn + endScn) ; i ++)
      {
        posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
        //finder = new GoldenGateTransactionSCNFinder();
        finder = new GGXMLTrailTransactionFinder();

        FilePositionResult res = posSetter.getFilePosition(i,finder);
        log.info("For scn (" + i + "):  the result is:  "  + res);
        if (i%2 == 0)
          assertFilePositionResult(res,dir,i+1,FilePositionResult.Status.EXACT_SCN_NOT_FOUND);
        else
          assertFilePositionResult(res,dir,i,FilePositionResult.Status.FOUND);
      }

      //Found Case
      for (long i = (startScn + endScn) ; i < (startScn + endScn) + 20 ; i ++)
      {
        posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
        //finder = new GoldenGateTransactionSCNFinder();
        finder = new GGXMLTrailTransactionFinder();

        FilePositionResult res = posSetter.getFilePosition(i,finder);
        log.info("For scn (" + i + "):  the result is:  "  + res);
        assertFilePositionResult(res,dir,299,FilePositionResult.Status.EXACT_SCN_NOT_FOUND);
      }
    }
  }

  /**
   * Case where XML snippets have newlines at different positions.
   * NOTE:  This test is EXCEEDINGLY slow...
   *
   * @throws Exception
   */
  @Test
  public void testScnPositionSetter2()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testScnPositionSetter2");
    log.info("starting");

    String[] newLines = { "\r", "\n", "\r\n" };

    for (String newLine : newLines)
    {
      // Iterate by Number of Tags per line
      for (int j = 10; j < 32; j++ )
      {
        log.info("NumPerLine: " + j);
        File dir = createTempDir();
        createTrailFiles(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX,100, 200,j,newLine,0, -1, "", false, "");
        log.info("Directory is: " + dir);

        TrailFilePositionSetter posSetter = null;
        //GoldenGateTransactionSCNFinder finder = new GoldenGateTransactionSCNFinder();
        GGXMLTrailTransactionFinder finder = new GGXMLTrailTransactionFinder();

        //less than minScn
        log.info("less than MinScn case started !!");
        for (long i = 0 ; i < 100 ; i ++)
        {
          if ( true )break;
          posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
          //finder = new GoldenGateTransactionSCNFinder();
          finder = new GGXMLTrailTransactionFinder();

          FilePositionResult res = posSetter.getFilePosition(i,finder);
          Assert.assertEquals(res.getStatus(),FilePositionResult.Status.ERROR,"Result Status");
        }

        log.info("less than MinScn case passed !!");


        //Found Case
        for (long i = 100 ; i < 300 ; i ++)
        {
          posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
          finder = new GGXMLTrailTransactionFinder();

          log.info("SCN:  " + i);
          FilePositionResult res = posSetter.getFilePosition(i,finder);
          log.info("For scn (" + i + "):  the result is:  "  + res);
          if (i%2 == 0)
            assertFilePositionResult(res,dir,i+1,FilePositionResult.Status.EXACT_SCN_NOT_FOUND);
          else
            assertFilePositionResult(res,dir,i,FilePositionResult.Status.FOUND);

        }

        //Found Case
        FilePositionResult res = null;
        for (long i = 300 ; i < 320 ; i ++)
        {
          posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
          //finder = new GoldenGateTransactionSCNFinder();
          finder = new GGXMLTrailTransactionFinder();

          res = posSetter.getFilePosition(i,finder);
          //log.info("For scn (" + i + "):  the result is:  "  + res);
          assertFilePositionResult(res,dir,299,FilePositionResult.Status.EXACT_SCN_NOT_FOUND);
        }

        // USE Latest SCN (-1)
        posSetter = new TrailFilePositionSetter(dir.getAbsolutePath(), TRAIL_FILENAME_PREFIX);
        //finder = new GoldenGateTransactionSCNFinder();
        finder = new GGXMLTrailTransactionFinder();

        res = posSetter.getFilePosition(-1,finder);
        //log.info("For scn (" + i + "):  the result is:  "  + res);
        assertFilePositionResult(res,dir,299,FilePositionResult.Status.FOUND);
      }
    }

    log.info(DONE_STRING);
  }

  private void assertFilePositionResult(FilePositionResult res, File dir, long expScn,
                                        FilePositionResult.Status expStatus)
    throws Exception
  {
    Assert.assertEquals(res.getStatus(),expStatus,"Result Status for scn: " + expScn);

    ConcurrentAppendableCompositeFileInputStream c =
        new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(),
                                                         res.getTxnPos().getFile(),
                                                         res.getTxnPos().getFileOffset(),
                                                         new TrailFilePositionSetter.FileFilter(dir, TRAIL_FILENAME_PREFIX),
                                                         true);

    ReadFirstSCN scnFetcher = new ReadFirstSCN(new BufferedReader(new InputStreamReader(c)),expScn);
    scnFetcher.start();

    scnFetcher.awaitShutdown();
    if(c != null)
      c.close();
    LOG.info("Result is: " + res);
    Assert.assertEquals(scnFetcher.isPatternMatched(), true, "expected SCN " + expScn + " not found at byte offset " +
                        res.getTxnPos().getFileOffset() + " of file " + res.getTxnPos().getFile() + ".");
    LOG.info("Finding result txn (" + res + ") passed !!");
  }

  public static class ReadFirstSCN
     extends DatabusThreadBase
  {
    private final BufferedReader _reader;
    private boolean _patternMatched = false;
    private String _errorMessage = null;
    private long _expScn = -1;
    public ReadFirstSCN(BufferedReader reader, long expScn)
    {
      super("ReadFirSCN");
      _reader = reader;
      _expScn = expScn;
    }

    @Override
    public void run()
    {
      boolean beginSeen = false;
      String line = null;
      try
      {
        String pattern = "<token name=\"tk-csn\">" + _expScn + "</token>";

        while ((line = _reader.readLine()) != null)
        {
         //LOG.info("Line:  (" + line + ")");

          line = line.toLowerCase();
          if ( !beginSeen)
          {
            beginSeen = true;

            if ( ! line.startsWith(GGXMLTrailTransactionFinder.TRANSACTION_BEGIN_PREFIX))
            {
              _errorMessage = "Stream not starting with transaction begin string !!  Line is: " + line;
              return;
            }
          }

          if ( line.contains(pattern) && line.contains(GGXMLTrailTransactionFinder.TRANSACTION_END_PREFIX))
          {
            int index1 = line.indexOf(pattern);
            int index2 = line.indexOf(GGXMLTrailTransactionFinder.TRANSACTION_END_PREFIX);

            if ( index1 < index2)
            {
              _patternMatched = true;
            }
            break;
          }

          if (line.contains(pattern))
            _patternMatched = true;

          if (line.contains(GGXMLTrailTransactionFinder.TRANSACTION_END_PREFIX))
            break;
        }
      }
      catch (IOException e)
      {
        e.printStackTrace();
      } finally {
        if ( null != _reader)
          try
          {
            _reader.close();
          }
          catch (IOException e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          doShutdownNotify();
      }
    }

    public boolean isPatternMatched()
    {
      return _patternMatched;
    }
  }

  @Test
  public void testSplitBytesByNewLines()
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testSplitBytesByNewLines");
    log.info("starting");

    String s = "abc\r\ndef\r\n";
    List<String> l = new ArrayList<String>();
    List<Integer> endPos = new ArrayList<Integer>();
    String lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 0, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(3,l.size(),"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"","First in " + l);
    Assert.assertEquals(l.get(1),"abc","Second in " + l);
    Assert.assertEquals(l.get(2),"def","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),-1,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 1, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(3,l.size(),"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"a","First in " + l);
    Assert.assertEquals(l.get(1),"bc","Second in " + l);
    Assert.assertEquals(l.get(2),"def","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),-1,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 2, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(3,l.size(),"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"ab","First in " + l);
    Assert.assertEquals(l.get(1),"c","Second in " + l);
    Assert.assertEquals(l.get(2),"def","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),-1,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 3, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 5, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(l.size(),3,"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"","Second in " + l);
    Assert.assertEquals(l.get(2),"def","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),-1,"First in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 6, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(l.size(),3,"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"d","Second in " + l);
    Assert.assertEquals(l.get(2),"ef","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),-1,"First in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    //log.info("Starting !!");
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 7, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(l.size(),3,"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"de","Second in " + l);
    Assert.assertEquals(l.get(2),"f","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),-1,"Second in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),10,"Third in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 4, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),10,"Second in " + endPos);

    s = "abc\r\ndef\r\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 8, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),10,"Second in " + endPos);

    s = "abc\ndef\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 3, null, l, endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),8,"Second in " + endPos);

    s = "abc\ndef\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),8,"Second in " + endPos);

    s = "abc\r\ndef\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertNull(lastLineStr,"Returned String should be null");
    Assert.assertEquals(2,l.size(),"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),9,"Second in " + endPos);

    s = "abc\r\ndef";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l, endPos);

    Assert.assertEquals( lastLineStr,"def","Returned String");
    Assert.assertEquals(l.size(),1,"Length");
    Assert.assertEquals(endPos.size(),1,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);

    s = "abc\r\ndef\r";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);
    //log.info("Length: " + lastLineStr.length());
    Assert.assertEquals( lastLineStr,"def\r","Returned String");
    Assert.assertEquals(l.size(),1,"Length");
    Assert.assertEquals(endPos.size(),1,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);

    lastLineStr = "abc";
    s = "def\r\nghi";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, lastLineStr, l, endPos);

    Assert.assertEquals( lastLineStr,"ghi","Returned String");
    Assert.assertEquals(l.size(),1,"Length");
    Assert.assertEquals(endPos.size(),1,"Length");
    Assert.assertEquals(l.get(0),"abcdef","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),8,"First in " + endPos);

    lastLineStr = "abc\r";
    s = "def\r\nghi";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, lastLineStr, l,endPos);

    Assert.assertEquals( lastLineStr,"ghi","Returned String");
    Assert.assertEquals(l.size(),2,"Length: " + l);
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),9,"Second in " + endPos);

    lastLineStr = "abc\r";
    s = "\ndef\r\nghi";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1,lastLineStr, l,endPos);
    Assert.assertEquals( lastLineStr,"ghi","Returned String");
    Assert.assertEquals(l.size(),2,"Length: " + l);
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),5,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),10,"Second in " + endPos);

    lastLineStr = "abc";
    s = "\ndef\r\nghi";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, lastLineStr, l, endPos);

    Assert.assertEquals( lastLineStr,"ghi","Returned String");
    Assert.assertEquals(l.size(),2,"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),9,"Second in " + endPos);

    lastLineStr = "abc";
    s = "\rdef\r\nghi";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, lastLineStr, l,endPos);

    Assert.assertEquals( lastLineStr,"ghi","Returned String");
    Assert.assertEquals(l.size(),2,"Length");
    Assert.assertEquals(endPos.size(),2,"Length");

    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"def","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),9,"Second in " + endPos);

    lastLineStr = "xyz";
    s = "abcdef";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, lastLineStr, l,endPos);

    Assert.assertEquals( lastLineStr,"xyzabcdef","Returned String");
    Assert.assertEquals(l.size(),0,"Length");
    Assert.assertEquals(endPos.size(),0,"Length");

    s = "abcdef";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertEquals( lastLineStr,"abcdef","Returned String");
    Assert.assertEquals(l.size(),0,"Length");
    Assert.assertEquals(endPos.size(),0,"Length");

    s = "abc\n\n  def";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertEquals( lastLineStr,"  def","Returned String");
    Assert.assertEquals(l.size(),2,"Length");
    Assert.assertEquals(endPos.size(),2,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"","Second in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),5,"Second in " + endPos);

    s = "abc\n\n  def\n   ghi";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), true, 4, null, l,endPos);

    Assert.assertEquals( lastLineStr,"   ghi","Returned String");
    Assert.assertEquals(l.size(),3,"Length");
    Assert.assertEquals(endPos.size(),3,"Length");
    Assert.assertEquals(l.get(0),"abc","First in " + l);
    Assert.assertEquals(l.get(1),"","Second in " + l);
    Assert.assertEquals(l.get(2),"  def","Third in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),4,"First in " + endPos);
    Assert.assertEquals(endPos.get(1).intValue(),5,"Second in " + endPos);
    Assert.assertEquals(endPos.get(2).intValue(),11,"Third in " + endPos);

    s = "\n";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertEquals( lastLineStr,null,"Returned String");
    Assert.assertEquals(l.size(),1,"Length");
    Assert.assertEquals(endPos.size(),1,"Length");
    Assert.assertEquals(l.get(0),"","First in " + l);
    Assert.assertEquals(endPos.get(0).intValue(),1,"First in " + endPos);

    s = " ";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertEquals( lastLineStr," ","Returned String");
    Assert.assertEquals(l.size(),0,"Length");
    Assert.assertEquals(endPos.size(),0,"Length");

    s = "";
    l = new ArrayList<String>();
    endPos = new ArrayList<Integer>();
    lastLineStr = TrailFilePositionSetter.splitBytesByNewLines(s.getBytes(Charset.defaultCharset()), s.length(), false, -1, null, l,endPos);

    Assert.assertEquals( lastLineStr,null,"Returned String");
    Assert.assertEquals(l.size(),0,"Length");
    Assert.assertEquals(endPos.size(),0,"Length");

    log.info(DONE_STRING);
  }

  @Test
  public void testTrailFileComparator()
  {
    final Logger log = Logger.getLogger("TestTrailFilePositionSetter.testTrailFileComparator");
    log.info("starting");

    String prefix = "x4";
    File dir = new File("/tmp");
    FileFilter f = new TrailFilePositionSetter.FileFilter(dir,prefix);

    //isTrailFile Check
    Assert.assertFalse(f.isTrailFile(null),"Null trail file");
    Assert.assertFalse(f.isTrailFile(dir),"Dir as  trail file");
    Assert.assertFalse(f.isTrailFile(new File("")),"Empty trail file name");
    Assert.assertFalse(f.isTrailFile(new File("/tmp/x12222")),"trail file with different prefix");
    Assert.assertFalse(f.isTrailFile(new File("/tmp/42222")),"trail file with different prefix");
    Assert.assertFalse(f.isTrailFile(new File("/tmp/x412222.xml")),"trail file with bad suffix");
    Assert.assertFalse(f.isTrailFile(new File("/tmp/x412222x")),"trail file with bad suffix");
    Assert.assertTrue(f.isTrailFile(new File("/tmp/x4122222")),"correct trail file");
    Assert.assertTrue(f.isTrailFile(new File("x4122222")),"correct trail file");

    //compareTo Check
    Assert.assertEquals(-1, f.compareFileName(new File("/tmp/x400001"), new File("/tmp/x400002")));
    Assert.assertEquals(-1, f.compareFileName(new File("/tmp/x400001"), new File("/tmp/x400002")));  // DUPLICATE?
    Assert.assertEquals(1, f.compareFileName(new File("/tmp/x410001"), new File("/tmp/x400002")));
    Assert.assertEquals(1, f.compareFileName(new File("/tmp/x410009"), new File("/tmp/x410003")));
    Assert.assertEquals(0, f.compareFileName(new File("/tmp/x410009"), new File("/tmp/x410009")));

    //isNextFileInSeq
    Assert.assertFalse(f.isNextFileInSequence(new File("/tmp/x400000"), new File("/tmp/x400000")));
    Assert.assertFalse(f.isNextFileInSequence(new File("/tmp/x410000"), new File("/tmp/x400000")));
    Assert.assertFalse(f.isNextFileInSequence(new File("/tmp/x400001"), new File("/tmp/x400000")));
    Assert.assertTrue(f.isNextFileInSequence(new File("/tmp/x400000"), new File("/tmp/x400001")));

    log.info(DONE_STRING);
  }
}
